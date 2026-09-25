// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Request-level, non-executing compilation. No logical Explain wrapper or cache.
use crate::prepared::typed_parameters::TypedParameterEnv;
use crate::{ProtocolResultSink, Session, StatementCompletion};
use paro_common::{
    chunk::Chunk,
    error::{self as error, Result},
    types::LogicalType,
    vector::Vector,
};
use paro_context::ExecutionTerminal;
use paro_context::{
    compile_diagnostics::{CaptureLevel, CompileCapture},
    StatementOptions,
};
use paro_execution::query_executor::compiled::ExecutionRequest;
use paro_execution::query_executor::executor::Executor;
use paro_parser::ast::{ExplainOption, Statement};

/// Own only the implicit transaction started by this request. Dropping a
/// backpressured request must not leave it attached to the next statement.
struct CompileTransaction<'a> {
    session: &'a mut Session,
    auto: bool,
}

impl Drop for CompileTransaction<'_> {
    fn drop(&mut self) {
        if self.auto {
            let _ = self.session.rollback_auto_transaction(None);
        }
    }
}

impl Session {
    pub(crate) async fn execute_compile_explain<S: ProtocolResultSink>(
        &mut self,
        target: Statement,
        options: &[ExplainOption],
        trailing_format: Option<String>,
        sink: &mut S,
    ) -> Result<()> {
        self.execute_compile_explain_with_parameters(
            target,
            options,
            trailing_format,
            &[],
            None,
            sink,
        )
        .await
    }

    /// Execute the extended-protocol form of EXPLAIN (COMPILE).
    ///
    /// Parse and Bind only retain the target and its concrete parameter
    /// environment.  This method is called from Execute, so the target is
    /// compiled exactly once at the actual execution boundary.  ANALYZE uses
    /// the same immutable image and bindings; it never invokes the compiler a
    /// second time.
    pub(crate) async fn execute_compile_explain_with_parameters<S: ProtocolResultSink>(
        &mut self,
        target: Statement,
        options: &[ExplainOption],
        trailing_format: Option<String>,
        parameter_types: &[LogicalType],
        parameter_env: Option<&TypedParameterEnv>,
        sink: &mut S,
    ) -> Result<()> {
        if trailing_format.is_some() {
            return Err(error::not_supported(
                "EXPLAIN (COMPILE) does not accept trailing FORMAT",
            ));
        }
        if !matches!(target, Statement::Query(_)) {
            return Err(error::not_supported(
                "EXPLAIN (COMPILE) supports query/CTE targets only",
            ));
        }
        let json = options.contains(&ExplainOption::FormatJson);
        let auto = self.transaction.is_auto_commit() && !self.transaction.has_active_transaction();
        if auto {
            self.begin_transaction_internal()?;
        }
        let mut transaction = CompileTransaction {
            session: self,
            auto,
        };
        let session = &mut *transaction.session;
        let capture =
            CompileCapture::try_start_with_level(if options.contains(&ExplainOption::Detail) {
                CaptureLevel::Detail
            } else {
                CaptureLevel::Summary
            });
        let cancellation = session
            .current_statement_cancellation()
            .expect("compile request scope");
        let ctx = session.freeze_statement_context(
            StatementOptions {
                compile_capture: capture.clone(),
                ..StatementOptions::default()
            },
            cancellation.clone(),
        );
        let execution_ctx = ctx.clone();
        // Calling the production compiler exactly once preserves binding, settings,
        // verifier, budgets and cancellation. ANALYZE admits and executes this
        // same immutable artifact below; it never recompiles the target.
        let result =
            paro_compiler::compile_statement_with_parameter_types(ctx, target, parameter_types);
        let compiled = match result {
            Ok(compiled) => compiled,
            Err(e) => {
                if auto {
                    let _ = session.rollback_auto_transaction(Some(&e));
                }
                return Err(e);
            }
        };
        let Some(capture) = capture else {
            // No unaccounted fallback result buffer. The target has compiled;
            // the diagnostic request cannot retain another document. A fixed
            // terminal envelope is carried by the ordinary error protocol.
            use paro_context::compile_diagnostics::{CompileDocument, UnavailableReason};
            let detail = serde_json::to_string(&CompileDocument::unavailable(
                UnavailableReason::ProcessCapacity,
            ))
            .expect("fixed-size unavailable document");
            let e = error::configuration_limit_exceeded("compile diagnostic capacity unavailable")
                .detail(detail);
            if auto {
                session.commit_auto_transaction()?;
            }
            transaction.auto = false;
            return Err(e);
        };
        let capture = capture.seal();
        let execution_receipt = if options.contains(&ExplainOption::Analyze) {
            let execution = match parameter_env {
                Some(parameter_env) => {
                    ExecutionRequest::from_typed_env(compiled.clone(), parameter_env)?
                }
                None => ExecutionRequest::unparameterized(compiled.clone())?,
            };
            let executor = Executor::new(execution_ctx);
            let mut handler = match executor.execute(execution) {
                Ok(handler) => handler,
                Err(error) => {
                    if auto {
                        let _ = session.rollback_auto_transaction(Some(&error));
                    }
                    return Err(error);
                }
            };
            let execution_id = handler.execution_id();
            while let Some(_chunk) = handler.fetch()? {}
            execution_id.and_then(|id| {
                session
                    .diagnostics
                    .execution_receipt(id)
                    .filter(|receipt| receipt.terminal != ExecutionTerminal::NotExecuted)
            })
        } else {
            None
        };
        drop(compiled);
        let document = paro_execution::explain::compile_render::render_with_execution_level(
            &capture,
            json,
            execution_receipt,
            options.contains(&ExplainOption::Detail),
        );
        let send = async {
            sink.start_result(&["QUERY PLAN".into()], &[LogicalType::Varchar])
                .await?;
            let allocator = session.buffer_allocator();
            let mut vector = Vector::try_from_strings(&[document.as_str()], allocator.clone())?;
            vector = vector.reference_with_lifetime_owner(capture.clone());
            let chunk = Chunk::from_vectors(vec![vector], allocator);
            sink.push_diagnostic_chunk(&chunk, capture).await?;
            sink.finish_result(&StatementCompletion::Explain).await
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                cancellation.check().and_then(|()| Err(error::query_canceled()))
            }
            result = send => result,
        };
        match result {
            Ok(()) => {
                if auto {
                    session.commit_auto_transaction()?;
                }
                transaction.auto = false;
                Ok(())
            }
            Err(e) => {
                if auto {
                    let _ = session.rollback_auto_transaction(Some(&e));
                }
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_context::StatementCancellation;
    use paro_execution::pipeline::StatementProgram;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn observer_preserves_physical_artifacts_and_forced_compile_does_not_publish_cache() {
        std::thread::Builder::new()
            .stack_size(32 << 20)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        let mut session =
                            Session::new(777, paro_instance::Instance::new_in_memory());
                        session.begin_transaction_internal().unwrap();
                        let ctx = session.freeze_statement_context(
                            StatementOptions::default(),
                            StatementCancellation::new(CancellationToken::new(), None),
                        );
                        let stmt = paro_parser::parse_one("SELECT 41 AS result").unwrap().stmt;
                        let ordinary =
                            paro_compiler::compile_statement(ctx.clone(), stmt.clone()).unwrap();
                        let capture = CompileCapture::try_start().unwrap();
                        let mut observed_ctx = ctx.as_ref().clone();
                        observed_ctx.options.compile_capture = Some(capture.clone());
                        let observed =
                            paro_compiler::compile_statement(Arc::new(observed_ctx), stmt.clone())
                                .unwrap();
                        assert_eq!(ordinary.result_schema(), observed.result_schema());
                        let (StatementProgram::Physical(a), StatementProgram::Physical(b)) =
                            (ordinary.program(), observed.program())
                        else {
                            panic!("expected deferred physical artifacts")
                        };
                        assert_eq!(a.physical_fingerprint, b.physical_fingerprint);
                        assert_eq!(a.grant, b.grant);
                        assert_eq!(
                            a.plan.properties.get(a.plan.root).unwrap().cumulative_cost,
                            b.plan.properties.get(b.plan.root).unwrap().cumulative_cost
                        );
                        assert!(session
                            .reusable_instance_query_plan(&stmt, &[], &ctx)
                            .is_none());
                        let mut sink = crate::CollectingSink::new();
                        session
                            .execute_simple_query(
                                "EXPLAIN (COMPILE, FORMAT JSON) SELECT 41 AS result",
                                &mut sink,
                            )
                            .await
                            .unwrap();
                        assert!(session
                            .reusable_instance_query_plan(&stmt, &[], &ctx)
                            .is_none());
                        let token = CancellationToken::new();
                        token.cancel();
                        let mut cancelled = ctx.as_ref().clone();
                        cancelled.cancellation = StatementCancellation::new(token, None);
                        cancelled.options.compile_capture = Some(capture);
                        let error = paro_compiler::compile_statement(Arc::new(cancelled), stmt)
                            .unwrap_err();
                        assert!(error.is_query_canceled());
                        session.rollback_auto_transaction(None).unwrap();
                    });
            })
            .unwrap()
            .join()
            .unwrap();
    }
}

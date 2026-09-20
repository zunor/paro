// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Request-level, non-executing compilation. No logical Explain wrapper or cache.
use crate::{ProtocolResultSink, Session, StatementCompletion};
use paro_common::{
    chunk::Chunk,
    error::{self as error, Result},
    types::LogicalType,
    vector::Vector,
};
use paro_context::{compile_diagnostics::CompileCapture, StatementOptions};
use paro_parser::ast::{ExplainOption, Statement};

impl Session {
    pub(crate) async fn execute_compile_explain<S: ProtocolResultSink>(
        &mut self,
        target: Statement,
        options: &[ExplainOption],
        trailing_format: Option<String>,
        sink: &mut S,
    ) -> Result<()> {
        if trailing_format.is_some() {
            return Err(error::not_supported(
                "EXPLAIN (COMPILE) does not accept trailing FORMAT",
            ));
        }
        if options.contains(&ExplainOption::Analyze) || options.contains(&ExplainOption::Detail) {
            return Err(error::not_supported(
                "EXPLAIN (COMPILE) currently supports non-executing Summary only",
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
        let capture = CompileCapture::try_start();
        let ctx = self.freeze_statement_context(
            StatementOptions {
                compile_capture: capture.clone(),
                ..StatementOptions::default()
            },
            self.current_statement_cancellation()
                .expect("compile request scope"),
        );
        // Calling the production compiler exactly once preserves binding, settings,
        // verifier, budgets and cancellation. Never admit/lower/execute this artifact.
        let result = paro_compiler::compile_statement(ctx, target);
        let compiled = match result {
            Ok(compiled) => compiled,
            Err(e) => {
                if auto {
                    let _ = self.rollback_auto_transaction(Some(&e));
                }
                return Err(e);
            }
        };
        drop(compiled);
        let Some(capture) = capture else {
            // No unaccounted fallback result buffer. The target has compiled;
            // the diagnostic request cannot retain another document. A fixed
            // terminal envelope is carried by the ordinary error protocol.
            let e = error::configuration_limit_exceeded("compile diagnostic capacity unavailable")
                .detail(r#"{"schema_version":1,"diagnostic":"Unavailable","target_compile":"Success","target_execution":"NotExecuted","reason":"ProcessCapacity"}"#);
            if auto {
                self.commit_auto_transaction()?;
            }
            return Err(e);
        };
        capture.seal();
        let document = paro_execution::explain::compile_render::render(&capture, json);
        let result = async {
            sink.start_result(&["QUERY PLAN".into()], &[LogicalType::Varchar])
                .await?;
            let allocator = self.buffer_allocator();
            let mut vector = Vector::try_from_strings(&[document.as_str()], allocator.clone())?;
            vector = vector.reference_with_lifetime_owner(capture.clone());
            let chunk = Chunk::from_vectors(vec![vector], allocator);
            sink.push_diagnostic_chunk(&chunk, capture).await?;
            sink.finish_result(&StatementCompletion::Explain).await
        }
        .await;
        match result {
            Ok(()) => {
                if auto {
                    self.commit_auto_transaction()?;
                }
                Ok(())
            }
            Err(e) => {
                if auto {
                    let _ = self.rollback_auto_transaction(Some(&e));
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
                        let (StatementProgram::Portfolio(a), StatementProgram::Portfolio(b)) =
                            (ordinary.program(), observed.program())
                        else {
                            panic!("expected deferred portfolios")
                        };
                        assert_eq!(a.variants.len(), b.variants.len());
                        for (a, b) in a.variants.iter().zip(b.variants.iter()) {
                            assert_eq!(a.physical_fingerprint, b.physical_fingerprint);
                            assert_eq!(a.cost, b.cost);
                            assert_eq!(a.admissible_classes, b.admissible_classes);
                        }
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

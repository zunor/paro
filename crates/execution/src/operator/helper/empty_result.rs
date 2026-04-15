//! Physical empty result operator.

use std::any::Any;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;

/// Source operator that immediately finishes without producing any rows.
#[derive(Debug)]
pub struct EmptyResult {
    types: Vec<LogicalType>,
}

impl EmptyResult {
    pub fn new(types: Vec<LogicalType>) -> Self {
        Self { types }
    }
}

impl PhysicalOperator for EmptyResult {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::EmptyResult
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        _input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        if chunk.column_count() != self.types.len() {
            *chunk = Chunk::initialize(&self.types, 0);
        } else {
            chunk.reset();
        }
        chunk.set_cardinality(0);
        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::EmptyResult;
    use crate::execution_context::ExecutionContext;
    use crate::operator::state::OperatorSourceInput;
    use crate::operator::PhysicalOperator;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_scheduler::task::InterruptState;
    use std::sync::Arc;

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    #[test]
    fn empty_result_source_finishes_immediately() {
        let op = EmptyResult::new(vec![LogicalType::Integer]);
        let session = test_session();
        let thread = crate::thread_context::ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let gstate = op
            .get_global_source_state(&ctx, None)
            .expect("global source");
        let mut lstate = op
            .get_local_source_state(&ctx, gstate.as_ref())
            .expect("local source");
        let interrupt = InterruptState::default();
        let mut input = OperatorSourceInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        let mut chunk = Chunk::initialize(&[LogicalType::Integer], 1);

        let result = op.get_data(&ctx, &mut chunk, &mut input).expect("get data");
        assert_eq!(result, crate::result_type::SourceResultType::Finished);
        assert_eq!(chunk.size(), 0);
    }
}

//! Plan Limit - Convert Limit to StreamingLimit
//!
//!
//! ## Design Notes
//! - MVP implements StreamingLimit only (simplest, most common case)
//! - StreamingLimit is used when:
//!   - Insertion order doesn't need to be preserved, OR
//!   - Source doesn't support batch index
//! - PhysicalLimit (batch) would be used for parallel execution with batch index
//!
//! ## Known Limitations
//! - Only constant LIMIT/OFFSET values supported (no expression evaluation)
//! - LIMIT PERCENT not supported
//! - Batch limit (PhysicalLimit) not implemented

use super::generator::PhysicalPlanGenerator;
use crate::operator::helper::streaming_limit::StreamingLimit;
use crate::operator::PhysicalOperator;
use paro_common::error::{self as paro_error, Result};
use paro_planner::expression::Expression;
use paro_planner::operator::limit::Limit;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for Limit.
    ///
    /// Converts Limit to StreamingLimit.
    /// Currently only supports constant LIMIT/OFFSET values.
    pub fn create_plan_limit(
        &self,
        limit: &Limit,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // Extract constant limit value
        let limit_value = match &limit.limit {
            Some(expr) => Some(extract_constant_usize(expr)?),
            None => None,
        };

        // Extract constant offset value
        let offset_value = match &limit.offset {
            Some(expr) => Some(extract_constant_usize(expr)?),
            None => None,
        };

        // Get output types from child
        let types = child.types().to_vec();

        // Create streaming limit operator
        // For MVP, we use non-parallel mode (parallel = false)
        let streaming_limit = StreamingLimit::new(
            types,
            limit_value,
            offset_value,
            false, // parallel = false for MVP
            child,
        );

        Ok(Arc::new(streaming_limit))
    }
}

/// Extract a constant usize value from a Expression.
///
/// Returns an error if the expression is not a constant integer.
fn extract_constant_usize(expr: &Expression) -> Result<usize> {
    match expr {
        Expression::Constant(constant) => {
            // Try to convert the value to usize
            match &constant.value {
                paro_common::runtime_value::Value::Integer(i) => {
                    if *i < 0 {
                        Err(paro_error::syntax(format!(
                            "LIMIT/OFFSET value cannot be negative: {}",
                            i
                        )))
                    } else {
                        Ok(*i as usize)
                    }
                }
                paro_common::runtime_value::Value::BigInt(i) => {
                    if *i < 0 {
                        Err(paro_error::syntax(format!(
                            "LIMIT/OFFSET value cannot be negative: {}",
                            i
                        )))
                    } else {
                        Ok(*i as usize)
                    }
                }
                paro_common::runtime_value::Value::SmallInt(i) => {
                    if *i < 0 {
                        Err(paro_error::syntax(format!(
                            "LIMIT/OFFSET value cannot be negative: {}",
                            i
                        )))
                    } else {
                        Ok(*i as usize)
                    }
                }
                paro_common::runtime_value::Value::TinyInt(i) => {
                    if *i < 0 {
                        Err(paro_error::syntax(format!(
                            "LIMIT/OFFSET value cannot be negative: {}",
                            i
                        )))
                    } else {
                        Ok(*i as usize)
                    }
                }
                paro_common::runtime_value::Value::UBigInt(i) => Ok(*i as usize),
                paro_common::runtime_value::Value::UInteger(i) => Ok(*i as usize),
                paro_common::runtime_value::Value::USmallInt(i) => Ok(*i as usize),
                paro_common::runtime_value::Value::UTinyInt(i) => Ok(*i as usize),
                other => Err(paro_error::syntax(format!(
                    "LIMIT/OFFSET must be an integer, got: {:?}",
                    other
                ))),
            }
        }
        _ => Err(paro_error::not_implemented(
            "Non-constant LIMIT/OFFSET expressions not yet supported".to_string(),
        )),
    }
}

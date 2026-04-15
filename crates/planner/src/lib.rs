//! SQL AST → logical plan: binding, name resolution, and logical operator trees.
//!
//! Entry points: [`crate::planner::Planner`], [`crate::binder::Binder`], [`crate::operator::LogicalOperator`].
//! Types live in submodules (for example [`crate::visitor::LogicalOperatorVisitor`]), not at the crate root.

pub mod binder;
pub mod expression;
pub mod operator;
pub mod plan;
pub mod planner;
pub mod verify;
pub mod visitor;

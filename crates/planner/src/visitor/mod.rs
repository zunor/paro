//! Traverses and rewrites logical operator trees (`LogicalOperatorVisitor`).

mod logical_operator_visitor;

pub use logical_operator_visitor::{enumerate_expressions, LogicalOperatorVisitor};

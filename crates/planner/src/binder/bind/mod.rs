//! AST → bound IR (`bind_*`): expressions, clauses, `FROM`, graph patterns, and per-statement binders.

pub mod clause;
pub mod expr;
pub mod from;
pub mod graph;
pub mod query;
pub mod statement;
pub mod type_name;

//! # Type Hierarchy
//! - Primitive: Boolean, Integer, Float, etc.
//! - Compound: Array (fixed-size), List (variable-size), Struct

mod inline_string;
mod logical_type;
mod nested_types;
pub mod pg_oid;
pub mod pg_type_descriptor;
mod physical_type;

pub use inline_string::{InlineString, INLINE_LENGTH, PREFIX_LENGTH};
pub use logical_type::LogicalType;
pub use nested_types::{ArrayType, ListType, StructType};
pub use pg_type_descriptor::{logical_type_from_pg_oid, PgTypeDescriptor};
pub use physical_type::PhysicalType;

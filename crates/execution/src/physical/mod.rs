//! Crate-local vocabulary for the optimizer-produced immutable physical plan.
//!
//! The public owner is `paro_optimizer::physical`; execution keeps this alias
//! only so runtime implementation modules can use a concise path.

pub use paro_optimizer::physical::*;

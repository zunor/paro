// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

mod call;
mod case;
mod cast;
mod collate;
mod column_ref;
mod dispatcher;
mod helpers;
mod index;
mod literal;
mod operators;
mod subquery;
mod window;

pub use call::bind_function;
pub use case::bind_case;
pub use cast::{bind_cast, bind_try_cast};
pub use collate::{bind_collate, validate_collation};
pub use column_ref::bind_column_ref_from_column_ref;
pub use dispatcher::{bind_expression, BindResult, BoundColumnReferenceInfo, ExpressionBinder};
pub use index::IndexBinder;
pub use literal::bind_literal;
pub use operators::{
    bind_array, bind_between, bind_coalesce, bind_comparison, bind_conjunction, bind_in_list,
    bind_is_null, bind_like, bind_map_access, bind_not, bind_tuple, try_bind_comparison,
};
pub use subquery::bind_subquery_expression;
pub use window::bind_window_expression;

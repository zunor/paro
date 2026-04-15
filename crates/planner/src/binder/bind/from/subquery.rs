//! Subquery TableRef Binder
//!
//!
//!
//! ## Supported
//! - Basic subquery as table reference: `SELECT * FROM (SELECT...) AS alias`
//! - Column alias: `SELECT * FROM (SELECT 1, 2) AS t(a, b)`
//!
//! ## Not Supported Yet
//! - Correlated non-LATERAL subqueries in FROM
//! - LATERAL table functions
//! - LATERAL nested join trees beyond direct subquery RHS
use crate::binder::ir::{BoundFromItem, BoundFromSubquery, BoundQuery};
use crate::binder::plan::subquery::{
    split_child_correlated_columns, CorrelationBoundaryMode, CorrelationProjectionMode,
};
use crate::binder::Binder;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{Query, TableAlias};
use std::mem;

/// Bind a subquery as a table reference.
///
/// This handles the `(SELECT...) AS alias` pattern in FROM clauses.
/// The subquery is bound in a child BindContext, and its result columns
/// are registered in the parent context under the given alias.
pub fn bind_subquery_ref(
    binder: &mut Binder,
    subquery: Query,
    alias: Option<TableAlias>,
    lateral: bool,
) -> Result<BoundFromItem> {
    // 1. Create a child binder for the subquery
    // This ensures the subquery has its own BindContext
    let mut child_binder = binder.create_child();

    // 2. Bind the subquery
    let bound_subquery = child_binder.bind_query(subquery)?;
    let correlated_columns_from_child = mem::take(&mut child_binder.correlated_columns);

    let split = split_child_correlated_columns(
        correlated_columns_from_child,
        CorrelationBoundaryMode::ScopeBoundary,
    );
    let correlated_columns = if lateral {
        split.projected_correlations(CorrelationProjectionMode::IncludeDepthOnePropagated)
    } else {
        split.local_to_child_parent.clone()
    };
    binder.correlated_columns.extend(split.propagate_to_parent);

    if !lateral && !correlated_columns.is_empty() {
        return Err(paro_error::syntax(
            "Subquery in FROM cannot reference outer columns without LATERAL",
        ));
    }

    // 3. Determine alias and column names
    let (subquery_alias, column_names, column_types) = determine_alias_and_columns(
        &bound_subquery,
        alias,
        binder.bind_context.unnamed_subquery_count(),
    )?;

    // Update unnamed subquery counter
    if subquery_alias.starts_with("unnamed_subquery") {
        let _ = binder.bind_context.next_unnamed_subquery_alias();
    }

    // 4. Register the subquery in the parent BindContext
    let subquery_index = binder.bind_context.generate_table_index();
    binder.bind_context.add_binding(
        subquery_alias.clone(),
        subquery_index,
        column_names.clone(),
        column_types.clone(),
    );

    // 5. Create the bound subquery ref
    Ok(BoundFromItem::Subquery(BoundFromSubquery {
        subquery: Box::new(bound_subquery),
        alias: subquery_alias,
        column_names,
        column_types,
        subquery_index,
        lateral,
        correlated_columns,
    }))
}

/// Determine the alias and column names for a subquery.
fn determine_alias_and_columns(
    subquery: &BoundQuery,
    alias: Option<TableAlias>,
    unnamed_count: usize,
) -> Result<(String, Vec<String>, Vec<paro_common::types::LogicalType>)> {
    let column_types = subquery.types();
    let mut subquery_names = subquery.names();
    normalize_derived_column_names(&mut subquery_names);

    if let Some(table_alias) = alias {
        let alias_name = table_alias.name.name;

        // If explicit column aliases are provided
        let column_names = if table_alias.columns.is_empty() {
            // Use the original column names from the subquery
            subquery_names
        } else {
            // Verify column count matches
            if table_alias.columns.len() != subquery_names.len() {
                return Err(paro_error::syntax(format!(
                    "Subquery alias '{}' specifies {} columns, but subquery returns {}",
                    alias_name,
                    table_alias.columns.len(),
                    subquery_names.len()
                )));
            }
            // Use the provided column aliases
            table_alias.columns.iter().map(|c| c.name.clone()).collect()
        };

        Ok((alias_name, column_names, column_types))
    } else {
        // Generate anonymous alias
        let alias_name = if unnamed_count == 0 {
            "unnamed_subquery".to_string()
        } else {
            format!("unnamed_subquery{}", unnamed_count + 1)
        };

        Ok((alias_name, subquery_names, column_types))
    }
}

fn normalize_derived_column_names(names: &mut [String]) {
    for name in names.iter_mut() {
        if let Some(unqualified) = unqualified_identifier_name(name) {
            *name = unqualified.to_string();
        }
    }
}

fn unqualified_identifier_name(name: &str) -> Option<&str> {
    let mut segments = name.split('.');
    let first = segments.next()?;
    let mut last = first;
    let mut count = 1usize;
    for segment in segments {
        if !is_identifier_segment(segment) {
            return None;
        }
        last = segment;
        count += 1;
    }

    if count >= 2 && is_identifier_segment(first) {
        Some(last)
    } else {
        None
    }
}

fn is_identifier_segment(segment: &str) -> bool {
    let mut chars = segment.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::test_binder;
    use paro_common::types::LogicalType;
    use paro_parser::{ast::Statement, parse_one};

    fn parse_query(sql: &str) -> Query {
        match parse_one(sql).expect("parse").stmt {
            Statement::Query(query) => *query,
            other => panic!("expected query statement, got {other:?}"),
        }
    }

    #[test]
    fn lateral_subquery_keeps_depth_one_correlations_locally() {
        let mut binder = test_binder();
        binder.bind_context.add_binding(
            "t".to_string(),
            7,
            vec!["x".to_string()],
            vec![LogicalType::Integer],
        );

        let bound = bind_subquery_ref(&mut binder, parse_query("SELECT t.x"), None, true)
            .expect("bind lateral subquery");

        match bound {
            BoundFromItem::Subquery(subquery) => {
                assert!(subquery.lateral);
                assert_eq!(subquery.correlated_columns.len(), 1);
                assert_eq!(subquery.correlated_columns[0].table_index, 7);
                assert_eq!(subquery.correlated_columns[0].depth, 1);
            }
            other => panic!("expected bound subquery, got {other:?}"),
        }

        assert!(binder.correlated_columns.is_empty());
    }

    #[test]
    fn non_lateral_from_subquery_rejects_outer_references() {
        let mut binder = test_binder();
        binder.bind_context.add_binding(
            "t".to_string(),
            9,
            vec!["x".to_string()],
            vec![LogicalType::Integer],
        );

        let error = bind_subquery_ref(&mut binder, parse_query("SELECT t.x"), None, false)
            .expect_err("non-lateral subquery should reject outer refs");

        assert!(error
            .to_string()
            .contains("cannot reference outer columns without LATERAL"));
    }

    #[test]
    fn lateral_subquery_keeps_nested_outer_correlations_locally() {
        let mut binder = test_binder();
        binder.bind_context.add_binding(
            "t".to_string(),
            11,
            vec!["x".to_string()],
            vec![LogicalType::Integer],
        );

        let bound = bind_subquery_ref(
            &mut binder,
            parse_query("SELECT EXISTS(SELECT 1 WHERE t.x = 1) AS has_match"),
            None,
            true,
        )
        .expect("bind lateral subquery with nested correlation");

        match bound {
            BoundFromItem::Subquery(subquery) => {
                assert!(subquery.lateral);
                assert_eq!(subquery.correlated_columns.len(), 1);
                assert_eq!(subquery.correlated_columns[0].table_index, 11);
                assert_eq!(subquery.correlated_columns[0].depth, 1);
            }
            other => panic!("expected bound subquery, got {other:?}"),
        }

        assert!(binder.correlated_columns.is_empty());
    }
}

//! Binds `WITH` (CTE) definitions. Recursive CTEs need a top-level `UNION`/`UNION ALL`. `MATERIALIZED` is parsed only.

use std::sync::Arc;

use crate::binder::plan::subquery::{split_child_correlated_columns, CorrelationBoundaryMode};
use crate::binder::Binder;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{CTEHint, SetExpr, SetOperator, TableAlias, CTE as Cte};

use crate::binder::ir::{
    BoundQuery, CTEBindInfo, CTEBindState, CTEBindStatus, CTEMaterialize,
    RecursiveCTE as BoundRecursiveCTE, CTE,
};

impl Binder {
    /// Bind a CTE definition and register it in the context.
    /// The CTE is not fully bound here - it's lazily bound when referenced.
    pub fn register_cte(&mut self, cte: Cte, recursive: bool) -> Result<Arc<CTEBindState>> {
        let name = cte.alias.name.name.clone();
        let aliases: Vec<String> = cte.alias.columns.iter().map(|c| c.name.clone()).collect();

        let materialized = match cte.materialization {
            CTEHint::Default => CTEMaterialize::Default,
            CTEHint::Materialized => CTEMaterialize::Materialized,
            CTEHint::NotMaterialized => CTEMaterialize::NotMaterialized,
        };

        let cte_index = self.bind_context.generate_table_index();

        let info = CTEBindInfo {
            name: name.clone(),
            aliases,
            query: cte.query,
            materialized,
            cte_index,
            recursive,
        };
        let state = Arc::new(CTEBindState::new(info));

        // Register the CTE in the context so it can be referenced
        self.bind_context.register_cte(name, Arc::clone(&state));

        Ok(state)
    }

    /// Bind a CTE query when it's referenced.
    /// This is called lazily when the CTE is first used.
    pub fn bind_cte(&mut self, info: &CTEBindInfo) -> Result<CTE> {
        if info.recursive && is_recursive_union_query(info.query.as_ref()) {
            return self.bind_recursive_cte(info);
        }

        self.bind_non_recursive_cte(info)
    }

    pub fn bind_non_recursive_cte(&mut self, info: &CTEBindInfo) -> Result<CTE> {
        // Create a child binder for the CTE query
        let mut child_binder = self.create_child();

        // Bind the CTE query
        let bound_query = child_binder.bind_query(*info.query.clone())?;

        // Get the result types and names
        let mut names = bound_query.names();
        normalize_derived_column_names(&mut names);
        let types = bound_query.types();

        // Apply column aliases if specified
        for (i, alias) in info.aliases.iter().enumerate() {
            if i < names.len() {
                names[i] = alias.clone();
            }
        }

        // Validate alias count if specified
        if !info.aliases.is_empty() && info.aliases.len() != names.len() {
            return Err(paro_error::syntax(format!(
                "CTE '{}' has {} columns but {} aliases were specified",
                info.name,
                names.len(),
                info.aliases.len()
            )));
        }

        // Move correlated columns from child to parent
        let split = split_child_correlated_columns(
            child_binder.correlated_columns,
            CorrelationBoundaryMode::TransparentBoundary,
        );
        self.correlated_columns.extend(split.propagate_to_parent);

        self.finish_bound_cte(info, bound_query, names, types, None)
    }

    pub fn get_or_bind_shared_cte(&mut self, state: Arc<CTEBindState>) -> Result<Arc<CTE>> {
        if let Some(bound) = state.bound.get() {
            let mut runtime = state.runtime.lock().map_err(|e| {
                paro_error::internal(format!("Failed to lock CTE runtime state: {e}"))
            })?;
            runtime.ref_count += 1;
            return Ok(Arc::clone(bound));
        }

        {
            let mut runtime = state.runtime.lock().map_err(|e| {
                paro_error::internal(format!("Failed to lock CTE runtime state: {e}"))
            })?;
            runtime.ref_count += 1;
            match runtime.status {
                CTEBindStatus::Bound => {
                    let bound = state.bound.get().ok_or_else(|| {
                        paro_error::internal("Bound CTE missing OnceLock payload".to_string())
                    })?;
                    return Ok(Arc::clone(bound));
                }
                CTEBindStatus::Failed => {
                    let err = runtime.last_error.clone().ok_or_else(|| {
                        paro_error::internal("Failed CTE missing cached error".to_string())
                    })?;
                    return Err((*err).clone());
                }
                CTEBindStatus::Binding => {
                    return Err(paro_error::syntax(format!(
                        "Non-recursive CTE '{}' cannot reference itself",
                        state.info.name
                    )));
                }
                CTEBindStatus::Unbound => {
                    runtime.status = CTEBindStatus::Binding;
                    runtime.last_error = None;
                }
            }
        }

        let bind_result = self.bind_cte(&state.info).map(Arc::new);
        match bind_result {
            Ok(bound_cte) => {
                let _ = state.bound.set(Arc::clone(&bound_cte));
                let mut runtime = state.runtime.lock().map_err(|e| {
                    paro_error::internal(format!("Failed to lock CTE runtime state: {e}"))
                })?;
                runtime.status = CTEBindStatus::Bound;
                runtime.last_error = None;
                Ok(bound_cte)
            }
            Err(err) => {
                let mut runtime = state.runtime.lock().map_err(|e| {
                    paro_error::internal(format!("Failed to lock CTE runtime state: {e}"))
                })?;
                runtime.status = CTEBindStatus::Failed;
                runtime.last_error = Some(Arc::new(err.clone()));
                Err(err)
            }
        }
    }

    pub(crate) fn finish_bound_cte(
        &self,
        info: &CTEBindInfo,
        query: BoundQuery,
        mut names: Vec<String>,
        types: Vec<paro_common::types::LogicalType>,
        recursive: Option<BoundRecursiveCTE>,
    ) -> Result<CTE> {
        normalize_derived_column_names(&mut names);

        if !info.aliases.is_empty() && info.aliases.len() != names.len() {
            return Err(paro_error::syntax(format!(
                "CTE '{}' has {} columns but {} aliases were specified",
                info.name,
                names.len(),
                info.aliases.len()
            )));
        }

        for (i, alias) in info.aliases.iter().enumerate() {
            if i < names.len() {
                names[i] = alias.clone();
            }
        }

        Ok(CTE {
            name: info.name.clone(),
            query,
            names,
            types,
            materialized: info.materialized,
            cte_index: info.cte_index,
            recursive,
        })
    }
}

fn is_recursive_union_query(query: &paro_parser::ast::Query) -> bool {
    matches!(
        query.body,
        SetExpr::SetOperation(ref set_op)
            if matches!(set_op.op, SetOperator::Union)
    )
}

pub(crate) fn normalize_derived_column_names(names: &mut [String]) {
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

/// Extract column aliases from a TableAlias.
pub fn extract_cte_aliases(alias: &TableAlias) -> Vec<String> {
    alias.columns.iter().map(|c| c.name.clone()).collect()
}

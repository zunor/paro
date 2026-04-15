use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{SetExpr, SetOperator};

use crate::binder::ir::{
    BoundQuery, CTEBindInfo, CTEBindState, CTEBindStatus, RecursiveCTE as BoundRecursiveCTE,
};
use crate::binder::plan::subquery::{split_child_correlated_columns, CorrelationBoundaryMode};
use crate::binder::Binder;

impl Binder {
    pub(crate) fn bind_recursive_cte(
        &mut self,
        info: &CTEBindInfo,
    ) -> Result<crate::binder::ir::CTE> {
        let query = info.query.as_ref();
        if !query.order_by.is_empty() || !query.limit.is_empty() || query.offset.is_some() {
            return Err(paro_error::not_implemented(format!(
                "Recursive CTE '{}' does not support ORDER BY/LIMIT/OFFSET in the recursive definition yet",
                info.name
            )));
        }

        let SetExpr::SetOperation(set_op) = &query.body else {
            return self.bind_non_recursive_cte(info);
        };
        if !matches!(set_op.op, SetOperator::Union) {
            return self.bind_non_recursive_cte(info);
        }

        let mut anchor_binder = self.create_child();
        let anchor = anchor_binder.bind_set_expr((*set_op.left).clone(), &[], &[], &None)?;
        let anchor_types = anchor.types();
        let anchor_names = self.resolve_recursive_cte_names(info, anchor.names())?;

        let placeholder_state = self.build_recursive_placeholder_state(
            info,
            anchor.clone(),
            anchor_names.clone(),
            anchor_types.clone(),
        )?;

        let mut recursive_binder = self.create_child();
        recursive_binder
            .bind_context
            .register_cte(info.name.clone(), Arc::clone(&placeholder_state));

        let mut recursive =
            recursive_binder.bind_set_expr((*set_op.right).clone(), &[], &[], &None)?;
        self.validate_recursive_cte_shape(&anchor, &recursive, info)?;
        recursive.cast_to_types(&anchor_types, &self.cast_functions)?;

        let anchor_split = split_child_correlated_columns(
            anchor_binder.correlated_columns,
            CorrelationBoundaryMode::TransparentBoundary,
        );
        self.correlated_columns
            .extend(anchor_split.propagate_to_parent);
        let recursive_split = split_child_correlated_columns(
            recursive_binder.correlated_columns,
            CorrelationBoundaryMode::TransparentBoundary,
        );
        self.correlated_columns
            .extend(recursive_split.propagate_to_parent);

        let bound_query = self.bind_set_operation(
            SetOperator::Union,
            set_op.all,
            anchor.clone(),
            recursive.clone(),
        )?;

        let is_recursive = placeholder_state.ref_count()? > 0;
        let recursive = is_recursive.then_some(BoundRecursiveCTE {
            union_all: set_op.all,
            anchor,
            recursive,
        });

        self.finish_bound_cte(info, bound_query, anchor_names, anchor_types, recursive)
    }

    fn resolve_recursive_cte_names(
        &self,
        info: &CTEBindInfo,
        mut names: Vec<String>,
    ) -> Result<Vec<String>> {
        super::cte::normalize_derived_column_names(&mut names);
        if !info.aliases.is_empty() && info.aliases.len() != names.len() {
            return Err(paro_error::syntax(format!(
                "CTE '{}' has {} columns but {} aliases were specified",
                info.name,
                names.len(),
                info.aliases.len()
            )));
        }
        for (idx, alias) in info.aliases.iter().enumerate() {
            if idx < names.len() {
                names[idx] = alias.clone();
            }
        }
        Ok(names)
    }

    fn validate_recursive_cte_shape(
        &self,
        anchor: &BoundQuery,
        recursive: &BoundQuery,
        info: &CTEBindInfo,
    ) -> Result<()> {
        let anchor_types = anchor.types();
        let recursive_types = recursive.types();
        if anchor_types.len() != recursive_types.len() {
            return Err(paro_error::syntax(format!(
                "Recursive CTE '{}' has mismatched column counts: {} vs {}",
                info.name,
                anchor_types.len(),
                recursive_types.len()
            )));
        }
        Ok(())
    }

    fn build_recursive_placeholder_state(
        &self,
        info: &CTEBindInfo,
        query: BoundQuery,
        names: Vec<String>,
        types: Vec<LogicalType>,
    ) -> Result<Arc<CTEBindState>> {
        let state = Arc::new(CTEBindState::new(info.clone()));
        let placeholder = Arc::new(crate::binder::ir::CTE {
            name: info.name.clone(),
            query,
            names,
            types,
            materialized: info.materialized,
            cte_index: info.cte_index,
            recursive: None,
        });
        let _ = state.bound.set(placeholder);
        let mut runtime = state.runtime.lock().map_err(|e| {
            paro_error::internal(format!(
                "Failed to lock recursive CTE placeholder state: {e}"
            ))
        })?;
        runtime.status = CTEBindStatus::Bound;
        runtime.last_error = None;
        drop(runtime);
        Ok(state)
    }
}

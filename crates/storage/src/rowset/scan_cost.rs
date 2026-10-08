// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared access-cost policy for rowset scan planning and runtime adaptation.

use paro_common::error::{self as paro_error, Result};
use paro_common::types::{LogicalType, PhysicalType};
use paro_common::vector::VECTOR_SIZE;

/// One predicate a scan evaluates: the fraction of rows it keeps and the
/// bytes of the stored columns it reads. Runtime stages are published by a
/// join build after planning; static stages are pushed from the query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScanPredicateStage {
    pub selectivity: f64,
    pub width: usize,
    pub runtime: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScanAccessCostModel {
    unknown_selectivity: f64,
    gather_access_penalty: f64,
    gather_startup_cost: usize,
    default_variable_width: usize,
    default_nested_width: usize,
}

impl Default for ScanAccessCostModel {
    fn default() -> Self {
        Self::try_new(0.25, 2.0, VECTOR_SIZE, 32, 64)
            .expect("built-in scan access costs must satisfy public validation")
    }
}

impl ScanAccessCostModel {
    pub fn try_new(
        unknown_selectivity: f64,
        gather_access_penalty: f64,
        gather_startup_cost: usize,
        default_variable_width: usize,
        default_nested_width: usize,
    ) -> Result<Self> {
        if !unknown_selectivity.is_finite() || !(0.0..=1.0).contains(&unknown_selectivity) {
            return Err(paro_error::invalid_input(
                "scan unknown selectivity must be finite and in [0, 1]",
            ));
        }
        if !gather_access_penalty.is_finite() || gather_access_penalty <= 0.0 {
            return Err(paro_error::invalid_input(
                "scan gather access penalty must be finite and positive",
            ));
        }
        if gather_startup_cost == 0 {
            return Err(paro_error::invalid_input(
                "scan gather startup cost must be positive",
            ));
        }
        if default_variable_width == 0 || default_nested_width == 0 {
            return Err(paro_error::invalid_input(
                "scan fallback widths must be positive",
            ));
        }
        Ok(Self {
            unknown_selectivity,
            gather_access_penalty,
            gather_startup_cost,
            default_variable_width,
            default_nested_width,
        })
    }

    pub fn estimated_width(self, ty: &LogicalType) -> usize {
        match ty.physical_type() {
            PhysicalType::Varchar => self.default_variable_width,
            PhysicalType::List | PhysicalType::Struct | PhysicalType::Array => {
                self.default_nested_width
            }
            _ => ty.type_size().max(1),
        }
    }

    pub fn unknown_selectivity(self) -> f64 {
        self.unknown_selectivity
    }

    pub fn gather_access_penalty(self) -> f64 {
        self.gather_access_penalty
    }

    /// Fixed preparation cost of opening a sparse gather frontier, expressed
    /// in the same byte-work units as width-based scan costing. The default is
    /// one unit per vector slot, representing the fixed executor, snapshot,
    /// and batch-frontier work without charging the full byte width of a
    /// reusable row-id scratch vector.
    pub fn gather_startup_cost(self) -> usize {
        self.gather_startup_cost
    }

    pub fn late_materialization_is_cheaper(
        self,
        predicate_width: usize,
        deferred_width: usize,
        eager_width: usize,
        selectivity: Option<f64>,
    ) -> bool {
        let selectivity = selectivity
            .unwrap_or(self.unknown_selectivity)
            .clamp(0.0, 1.0);
        let late_cost = predicate_width as f64
            + selectivity * deferred_width as f64 * self.gather_access_penalty;
        late_cost < eager_width as f64
    }

    /// Decode bytes per scanned row for `width` stored bytes evaluated through
    /// `stages`, as the executor runs them. Materialization follows the same
    /// decision as scan preparation: a runtime stage's selectivity is unknown
    /// when the scan is prepared. A late scan evaluates stages in ascending
    /// selectivity; the first decodes every row, every later stage and the
    /// deferred columns decode the vector blocks holding a survivor.
    pub fn staged_decode_bytes(self, width: usize, stages: &[ScanPredicateStage]) -> f64 {
        let mut ordered = stages.to_vec();
        ordered.sort_by(|left, right| left.selectivity.total_cmp(&right.selectivity));
        let read = ordered.first().map_or(0, |stage| stage.width.min(width));
        let static_selectivity = stages
            .iter()
            .filter(|stage| !stage.runtime)
            .map(|stage| stage.selectivity.clamp(0.0, 1.0))
            .reduce(|left, right| left * right);
        let decision_selectivity = if stages.iter().any(|stage| stage.runtime) {
            static_selectivity.map(|selectivity| selectivity.min(self.unknown_selectivity))
        } else {
            static_selectivity
        };
        if stages.is_empty()
            || !self.late_materialization_is_cheaper(
                read,
                width - read,
                width,
                decision_selectivity,
            )
        {
            return width as f64;
        }
        let (mut bytes, mut surviving, mut remaining) = (0.0, 1.0, width);
        for stage in ordered {
            let columns = stage.width.min(remaining);
            remaining -= columns;
            bytes += columns as f64 * self.deferred_decode_fraction(surviving);
            surviving *= stage.selectivity.clamp(0.0, 1.0);
        }
        bytes + remaining as f64 * self.deferred_decode_fraction(surviving)
    }

    /// Fraction of a late scan's deferred decode work still performed when
    /// `selectivity` of its rows survive the predicates. Deferred columns
    /// decode whole vector blocks, so uniformly spread survivors touch almost
    /// every block long before they are a large fraction of the rows: the
    /// work falls only as blocks with no survivor appear. Dense selections
    /// switch to sequential materialization, so it never exceeds one.
    pub fn deferred_decode_fraction(self, selectivity: f64) -> f64 {
        let selectivity = selectivity.clamp(0.0, 1.0);
        1.0 - (1.0 - selectivity).powf(VECTOR_SIZE as f64)
    }

    pub fn sequential_materialization_is_cheaper(
        self,
        selected_rows: usize,
        physical_rows: usize,
    ) -> bool {
        physical_rows != 0
            && selected_rows as f64 * self.gather_access_penalty >= physical_rows as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(selectivity: f64, width: usize, runtime: bool) -> ScanPredicateStage {
        ScanPredicateStage {
            selectivity,
            width,
            runtime,
        }
    }

    #[test]
    fn staged_decode_follows_the_scan_materialization_decision() {
        let model = ScanAccessCostModel::default();
        assert_eq!(model.staged_decode_bytes(64, &[]), 64.0);
        // A nearly unselective static predicate stays eager.
        assert_eq!(
            model.staged_decode_bytes(64, &[stage(0.99, 8, false)]),
            64.0
        );
        // A late scan reads its predicate columns for every row and still
        // touches every block of the deferred columns at 1% survivors.
        assert_eq!(
            model.staged_decode_bytes(64, &[stage(0.01, 8, false)]),
            64.0
        );
        // Very sparse survivors leave most deferred blocks untouched.
        let sparse = model.staged_decode_bytes(64, &[stage(1.0e-5, 8, false)]);
        assert!(sparse > 8.0 && sparse < 8.0 + 56.0 * 0.05, "{sparse}");
    }

    #[test]
    fn staged_decode_orders_stages_by_selectivity() {
        let model = ScanAccessCostModel::default();
        let wide_static = stage(0.1, 32, false);
        let sparse_runtime = stage(1.0e-5, 4, true);
        // The runtime key runs first; the wide static columns are read only
        // in blocks holding a runtime survivor.
        let staged = model.staged_decode_bytes(64, &[wide_static, sparse_runtime]);
        let expected = 4.0
            + 32.0 * model.deferred_decode_fraction(1.0e-5)
            + 28.0 * model.deferred_decode_fraction(1.0e-6);
        assert!((staged - expected).abs() < 1e-9);
        assert_eq!(
            staged,
            model.staged_decode_bytes(64, &[sparse_runtime, wide_static])
        );
    }

    #[test]
    fn deferred_decode_falls_only_once_blocks_have_no_survivors() {
        let model = ScanAccessCostModel::default();
        // One survivor in ten still touches every block.
        assert!((model.deferred_decode_fraction(0.1) - 1.0).abs() < 1e-9);
        assert_eq!(model.deferred_decode_fraction(0.0), 0.0);
        // About one survivor per 30k rows leaves most blocks untouched.
        let sparse = model.deferred_decode_fraction(3.0e-5);
        assert!(sparse > 0.1 && sparse < 0.2, "{sparse}");
        // Every block is decoded once at most.
        assert_eq!(model.deferred_decode_fraction(1.0), 1.0);
        let mut previous = 0.0;
        for step in 0..=1000 {
            let fraction = model.deferred_decode_fraction(step as f64 / 1000.0);
            assert!(fraction >= previous);
            previous = fraction;
        }
    }
}

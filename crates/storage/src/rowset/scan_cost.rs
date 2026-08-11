// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared access-cost policy for rowset scan planning and runtime adaptation.

use paro_common::error::{self as paro_error, Result};
use paro_common::types::{LogicalType, PhysicalType};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScanAccessCostModel {
    unknown_selectivity: f64,
    gather_access_penalty: f64,
    default_variable_width: usize,
    default_nested_width: usize,
}

impl Default for ScanAccessCostModel {
    fn default() -> Self {
        Self::try_new(0.25, 2.0, 32, 64)
            .expect("built-in scan access costs must satisfy public validation")
    }
}

impl ScanAccessCostModel {
    pub fn try_new(
        unknown_selectivity: f64,
        gather_access_penalty: f64,
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
        if default_variable_width == 0 || default_nested_width == 0 {
            return Err(paro_error::invalid_input(
                "scan fallback widths must be positive",
            ));
        }
        Ok(Self {
            unknown_selectivity,
            gather_access_penalty,
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
            _ => ty.physical_size().max(1),
        }
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

    pub fn sequential_materialization_is_cheaper(
        self,
        selected_rows: usize,
        physical_rows: usize,
    ) -> bool {
        physical_rows != 0
            && selected_rows as f64 * self.gather_access_penalty >= physical_rows as f64
    }
}

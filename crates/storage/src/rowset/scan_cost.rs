// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared access-cost policy for rowset scan planning and runtime adaptation.

use paro_common::types::{LogicalType, PhysicalType};

#[derive(Debug, Clone, Copy)]
pub struct ScanAccessCostModel {
    pub unknown_selectivity: f64,
    pub gather_access_penalty: f64,
    pub default_variable_width: usize,
    pub default_nested_width: usize,
}

impl Default for ScanAccessCostModel {
    fn default() -> Self {
        Self {
            unknown_selectivity: 0.25,
            gather_access_penalty: 2.0,
            default_variable_width: 32,
            default_nested_width: 64,
        }
    }
}

impl ScanAccessCostModel {
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

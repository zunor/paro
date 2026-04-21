// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_external_abi::{ColumnDescriptor, ColumnPopulationMode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptorView {
    pub descriptor: ColumnDescriptor,
}

impl DescriptorView {
    pub fn estimated_payload_bytes(&self, row_count: u32) -> u64 {
        match &self.descriptor.layout {
            paro_external_abi::ColumnLayout::FixedWidth { stride, .. } => {
                u64::from(*stride) * u64::from(row_count)
            }
            paro_external_abi::ColumnLayout::VarLen { offsets, data, .. } => offsets.len + data.len,
            paro_external_abi::ColumnLayout::List { offsets, .. } => offsets.len,
            paro_external_abi::ColumnLayout::Struct => self
                .descriptor
                .children
                .iter()
                .map(|child| {
                    DescriptorView {
                        descriptor: child.clone(),
                    }
                    .estimated_payload_bytes(row_count)
                })
                .sum(),
            paro_external_abi::ColumnLayout::Dictionary {
                indices,
                dictionary,
            } => {
                indices.len
                    + DescriptorView {
                        descriptor: dictionary.as_ref().clone(),
                    }
                    .estimated_payload_bytes(row_count)
            }
            paro_external_abi::ColumnLayout::Sequence { .. }
            | paro_external_abi::ColumnLayout::Constant { .. } => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LazyPopulationHeuristics {
    pub predicted_varlen_ratio: f64,
    pub projected_pages_touched: usize,
    pub page_fault_cost_us: u64,
    pub eager_decode_cost_us: u64,
    pub linux_fast_path_available: bool,
}

impl LazyPopulationHeuristics {
    pub fn choose(&self, descriptor: &ColumnDescriptor) -> ColumnPopulationMode {
        if !self.linux_fast_path_available {
            return ColumnPopulationMode::Eager;
        }

        let descriptor_is_candidate =
            descriptor.logical_type.is_varlen() || descriptor.logical_type.is_nested();
        let predicted_lazy_gain = self
            .eager_decode_cost_us
            .saturating_sub(self.page_fault_cost_us * self.projected_pages_touched as u64);

        if descriptor_is_candidate
            && self.predicted_varlen_ratio >= 0.25
            && predicted_lazy_gain > 0
            && self.projected_pages_touched <= 8
        {
            ColumnPopulationMode::LazyLinuxUffd
        } else {
            ColumnPopulationMode::Eager
        }
    }
}

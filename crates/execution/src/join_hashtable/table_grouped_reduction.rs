// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Stable grouped-reduction mode selection for one hash-table generation.

use std::sync::Arc;

use paro_common::error::{self as paro_error, ErrorClass, Result};

use super::super::reduction_extrema::GroupedReductionExtrema;
use super::JoinHashTable;

pub(super) enum GroupedReductionExtremaState {
    Unconfigured,
    Requested { channel_count: usize },
    Unavailable { channel_count: usize },
    Ready(Arc<GroupedReductionExtrema>),
}

impl JoinHashTable {
    pub(super) fn reset_grouped_reduction_extrema(&self) {
        let mut state = self.grouped_reduction_extrema.lock().unwrap();
        *state = match &*state {
            GroupedReductionExtremaState::Unconfigured => {
                GroupedReductionExtremaState::Unconfigured
            }
            GroupedReductionExtremaState::Requested { channel_count }
            | GroupedReductionExtremaState::Unavailable { channel_count } => {
                GroupedReductionExtremaState::Requested {
                    channel_count: *channel_count,
                }
            }
            GroupedReductionExtremaState::Ready(extrema) => {
                GroupedReductionExtremaState::Requested {
                    channel_count: extrema.channel_count(),
                }
            }
        };
    }

    pub(crate) fn configure_grouped_reduction_extrema(&self, channel_count: usize) -> Result<()> {
        if channel_count == 0 || channel_count > u8::BITS as usize {
            return Err(paro_error::internal(
                "grouped reduction extrema channel count is invalid",
            ));
        }
        let mut state = self.grouped_reduction_extrema.lock().unwrap();
        match &*state {
            GroupedReductionExtremaState::Unconfigured => {
                *state = GroupedReductionExtremaState::Requested { channel_count };
                Ok(())
            }
            GroupedReductionExtremaState::Requested {
                channel_count: existing,
            }
            | GroupedReductionExtremaState::Unavailable {
                channel_count: existing,
            } if *existing == channel_count => Ok(()),
            GroupedReductionExtremaState::Ready(extrema) => {
                if extrema.channel_count() != channel_count {
                    return Err(paro_error::internal(
                        "grouped reduction extrema channel count changed after initialization",
                    ));
                }
                Ok(())
            }
            _ => Err(paro_error::internal(
                "grouped reduction extrema configuration changed",
            )),
        }
    }

    pub(super) fn finalize_grouped_reduction_extrema(&self) -> Result<()> {
        let mut state = self.grouped_reduction_extrema.lock().unwrap();
        let GroupedReductionExtremaState::Requested { channel_count } = *state else {
            return Ok(());
        };
        let group_count = self
            .integer_index
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|index| index.ranked_group_count());
        let Some(group_count) = group_count else {
            *state = GroupedReductionExtremaState::Unavailable { channel_count };
            return Ok(());
        };
        let extrema = match GroupedReductionExtrema::try_new(
            group_count,
            channel_count,
            self.allocator.clone(),
            &self.pointer_memory,
        ) {
            Ok(extrema) => Arc::new(extrema),
            Err(error) if error.error_class() == ErrorClass::Resource => {
                *state = GroupedReductionExtremaState::Unavailable { channel_count };
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        *state = GroupedReductionExtremaState::Ready(extrema);
        Ok(())
    }

    pub(crate) fn grouped_reduction_extrema(&self) -> Option<Arc<GroupedReductionExtrema>> {
        match &*self.grouped_reduction_extrema.lock().unwrap() {
            GroupedReductionExtremaState::Ready(extrema) => Some(Arc::clone(extrema)),
            GroupedReductionExtremaState::Unconfigured
            | GroupedReductionExtremaState::Requested { .. }
            | GroupedReductionExtremaState::Unavailable { .. } => None,
        }
    }
}

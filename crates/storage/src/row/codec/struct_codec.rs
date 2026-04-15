// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_common::vector::Vector;

use crate::row::codec::ColumnCodec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructCodec {
    fields: Vec<(String, ColumnCodec)>,
}

impl StructCodec {
    pub fn new(fields: Vec<(String, ColumnCodec)>) -> Self {
        Self { fields }
    }

    #[inline]
    pub fn fields(&self) -> &[(String, ColumnCodec)] {
        &self.fields
    }
}

pub(crate) fn scatter(
    _codec: &StructCodec,
    source: &Vector,
    output: &mut Vector,
    output_positions: &[usize],
) -> Result<()> {
    for (src_idx, dst_idx) in output_positions.iter().copied().enumerate() {
        output.copy_at(dst_idx, source, src_idx);
    }
    Ok(())
}

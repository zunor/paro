// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_common::vector::Vector;

use crate::row::codec::ColumnCodec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayCodec {
    child: Box<ColumnCodec>,
    width: usize,
}

impl ArrayCodec {
    pub fn new(child: ColumnCodec, width: usize) -> Self {
        Self {
            child: Box::new(child),
            width,
        }
    }

    #[inline]
    pub fn child(&self) -> &ColumnCodec {
        &self.child
    }

    #[inline]
    pub fn width(&self) -> usize {
        self.width
    }
}

pub(crate) fn scatter(
    _codec: &ArrayCodec,
    source: &Vector,
    output: &mut Vector,
    output_positions: &[usize],
) -> Result<()> {
    for (src_idx, dst_idx) in output_positions.iter().copied().enumerate() {
        output.try_copy_at(dst_idx, source, src_idx)?;
    }
    Ok(())
}

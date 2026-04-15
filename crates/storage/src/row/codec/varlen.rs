use paro_common::error::Result;
use paro_common::vector::Vector;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarlenCodec {
    InlineHeap16,
}

pub(crate) fn scatter(
    _codec: &VarlenCodec,
    source: &Vector,
    output: &mut Vector,
    output_positions: &[usize],
) -> Result<()> {
    for (src_idx, dst_idx) in output_positions.iter().copied().enumerate() {
        output.copy_at(dst_idx, source, src_idx);
    }
    Ok(())
}

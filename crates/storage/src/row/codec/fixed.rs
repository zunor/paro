use std::ptr;

use paro_common::error::Result;
use paro_common::vector::{Vector, VectorType};

fn scatter_fixed_inline<const N: usize>(
    source: &Vector,
    output: &mut Vector,
    output_positions: &[usize],
) -> Result<()> {
    let decoded = source.decode(output_positions.len());
    if output.vector_type() != VectorType::Flat {
        output.flatten();
    }

    unsafe {
        let src_base = decoded.data();
        let dst_base = output.flat_data_mut::<u8>();
        for (src_idx, dst_idx) in output_positions.iter().copied().enumerate() {
            if !decoded.is_valid(src_idx) {
                output.set_null(dst_idx, true);
                continue;
            }

            output.set_null(dst_idx, false);
            let src_ptr = src_base.add(decoded.physical_index(src_idx) * N);
            let dst_ptr = dst_base.add(dst_idx * N);
            ptr::copy_nonoverlapping(src_ptr, dst_ptr, N);
        }
    }

    Ok(())
}

fn scatter_fixed_memcpy(
    width: usize,
    source: &Vector,
    output: &mut Vector,
    output_positions: &[usize],
) -> Result<()> {
    let decoded = source.decode(output_positions.len());
    if output.vector_type() != VectorType::Flat {
        output.flatten();
    }

    unsafe {
        let src_base = decoded.data();
        let dst_base = output.flat_data_mut::<u8>();
        for (src_idx, dst_idx) in output_positions.iter().copied().enumerate() {
            if !decoded.is_valid(src_idx) {
                output.set_null(dst_idx, true);
                continue;
            }

            output.set_null(dst_idx, false);
            let src_ptr = src_base.add(decoded.physical_index(src_idx) * width);
            let dst_ptr = dst_base.add(dst_idx * width);
            ptr::copy_nonoverlapping(src_ptr, dst_ptr, width);
        }
    }

    Ok(())
}

pub(crate) fn scatter_fixed(
    width: usize,
    source: &Vector,
    output: &mut Vector,
    output_positions: &[usize],
) -> Result<()> {
    match width {
        1 => scatter_fixed_inline::<1>(source, output, output_positions),
        2 => scatter_fixed_inline::<2>(source, output, output_positions),
        4 => scatter_fixed_inline::<4>(source, output, output_positions),
        8 => scatter_fixed_inline::<8>(source, output, output_positions),
        16 => scatter_fixed_inline::<16>(source, output, output_positions),
        n => scatter_fixed_memcpy(n, source, output, output_positions),
    }
}

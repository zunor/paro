// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Dense fixed-width membership lookup and selection-compaction kernels.

use super::FixedPhysical;
use crate::rowset::BatchRowOrdinal;

#[inline(always)]
pub(super) fn contains_i64_bits(value: i64, base: i64, span: usize, bits: &[u64]) -> bool {
    let offset = value.wrapping_sub(base) as u64;
    if offset >= span as u64 {
        return false;
    }
    let offset = offset as usize;
    // SAFETY: the unsigned range check proves that the bit word belongs to
    // the dense representation. Wrapping subtraction folds both signed
    // boundary checks into that single comparison.
    unsafe {
        *bits.get_unchecked(offset / u64::BITS as usize) & (1_u64 << (offset % u64::BITS as usize))
            != 0
    }
}

#[inline(always)]
pub(super) fn contains_i64_bytes(value: i64, base: i64, present: &[u8]) -> bool {
    let offset = value.wrapping_sub(base) as u64;
    offset < present.len() as u64 && unsafe { *present.get_unchecked(offset as usize) != 0 }
}

#[inline(always)]
pub(super) fn contains_i32_bits(value: i32, base: i32, span: usize, bits: &[u64]) -> bool {
    let offset = value.wrapping_sub(base) as u32;
    if u64::from(offset) >= span as u64 {
        return false;
    }
    let offset = offset as usize;
    // SAFETY: see the i64 kernel above.
    unsafe {
        *bits.get_unchecked(offset / u64::BITS as usize) & (1_u64 << (offset % u64::BITS as usize))
            != 0
    }
}

#[inline(always)]
pub(super) fn contains_i32_bytes(value: i32, base: i32, present: &[u8]) -> bool {
    let offset = value.wrapping_sub(base) as u32;
    u64::from(offset) < present.len() as u64
        && unsafe { *present.get_unchecked(offset as usize) != 0 }
}

#[inline(always)]
pub(super) fn contains_bytes<T: FixedPhysical>(value: T, base: T, present: &[u8]) -> bool {
    value
        .offset_from(base)
        .and_then(|offset| present.get(offset))
        .is_some_and(|present| *present != 0)
}

#[cfg(test)]
#[inline(always)]
pub(super) fn contains_bits<T: FixedPhysical>(
    value: T,
    base: T,
    span: usize,
    bits: &[u64],
) -> bool {
    let Some(offset) = value.offset_from(base).filter(|offset| *offset < span) else {
        return false;
    };
    bits[offset / u64::BITS as usize] & (1_u64 << (offset % u64::BITS as usize)) != 0
}

/// Compact a dense fixed-width membership predicate without a per-row output
/// branch. Dense domains back analytical runtime filters: the lookup is
/// scalar because current SIMD ISAs have no general byte gather, while the
/// selection write uses the architecture's table compaction kernel.
pub(super) fn filter_seed<T, F>(
    input: *const u8,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    nulls: Option<&[u8]>,
    contains: F,
) -> bool
where
    T: FixedPhysical,
    F: Fn(T) -> bool + Copy,
{
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        unsafe { filter_neon(input, rows, selection, nulls, contains) }
    }

    #[cfg(all(target_arch = "x86_64", target_endian = "little"))]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            return unsafe { filter_avx2(input, rows, selection, nulls, contains) };
        }
        false
    }

    #[cfg(not(any(
        all(target_arch = "aarch64", target_endian = "little"),
        all(target_arch = "x86_64", target_endian = "little")
    )))]
    {
        let _ = (input, rows, selection, nulls, contains);
        false
    }
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
unsafe fn filter_neon<T, F>(
    input: *const u8,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    nulls: Option<&[u8]>,
    contains: F,
) -> bool
where
    T: FixedPhysical,
    F: Fn(T) -> bool + Copy,
{
    match nulls {
        Some(nulls) => unsafe {
            filter_neon_inner::<T, F, true>(input, rows, selection, nulls.as_ptr(), contains)
        },
        None => unsafe {
            filter_neon_inner::<T, F, false>(input, rows, selection, std::ptr::null(), contains)
        },
    }
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
unsafe fn filter_neon_inner<T, F, const NULLABLE: bool>(
    input: *const u8,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    nulls: *const u8,
    contains: F,
) -> bool
where
    T: FixedPhysical,
    F: Fn(T) -> bool + Copy,
{
    use core::arch::aarch64::{vaddq_u32, vdupq_n_u32, vld1q_u32};

    selection.reserve(rows + 4);
    let start = selection.len();
    let output = selection
        .spare_capacity_mut()
        .as_mut_ptr()
        .cast::<BatchRowOrdinal>();
    let ordinal_offsets = unsafe { vld1q_u32([0u32, 1, 2, 3].as_ptr()) };
    let mut row = 0usize;
    let mut written = 0usize;
    while row + 4 <= rows {
        let mut mask = 0usize;
        let all_valid = !NULLABLE || unsafe { nulls.add(row).cast::<u32>().read_unaligned() == 0 };
        if all_valid {
            for lane in 0..4 {
                let value = T::from_le(unsafe {
                    input
                        .add((row + lane) * std::mem::size_of::<T>())
                        .cast::<T>()
                        .read_unaligned()
                });
                mask |= usize::from(contains(value)) << lane;
            }
        } else {
            for lane in 0..4 {
                let value = T::from_le(unsafe {
                    input
                        .add((row + lane) * std::mem::size_of::<T>())
                        .cast::<T>()
                        .read_unaligned()
                });
                mask |=
                    usize::from(unsafe { *nulls.add(row + lane) == 0 } && contains(value)) << lane;
            }
        }
        if mask != 0 {
            let ordinals = unsafe {
                vaddq_u32(
                    ordinal_offsets,
                    vdupq_n_u32(row.try_into().expect("validated rows")),
                )
            };
            written +=
                unsafe { super::compact_ordinals_mask_neon(mask, ordinals, output, written) };
        }
        row += 4;
    }
    while row < rows {
        let value = T::from_le(unsafe {
            input
                .add(row * std::mem::size_of::<T>())
                .cast::<T>()
                .read_unaligned()
        });
        let valid = !NULLABLE || unsafe { *nulls.add(row) == 0 };
        if valid && contains(value) {
            unsafe {
                output
                    .add(written)
                    .write(BatchRowOrdinal::from_validated_index(row))
            };
            written += 1;
        }
        row += 1;
    }
    unsafe { selection.set_len(start + written) };
    true
}

#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
#[target_feature(enable = "avx2")]
unsafe fn filter_avx2<T, F>(
    input: *const u8,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    nulls: Option<&[u8]>,
    contains: F,
) -> bool
where
    T: FixedPhysical,
    F: Fn(T) -> bool + Copy,
{
    match nulls {
        Some(nulls) => unsafe {
            filter_avx2_inner::<T, F, true>(input, rows, selection, nulls.as_ptr(), contains)
        },
        None => unsafe {
            filter_avx2_inner::<T, F, false>(input, rows, selection, std::ptr::null(), contains)
        },
    }
}

#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
#[target_feature(enable = "avx2")]
unsafe fn filter_avx2_inner<T, F, const NULLABLE: bool>(
    input: *const u8,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    nulls: *const u8,
    contains: F,
) -> bool
where
    T: FixedPhysical,
    F: Fn(T) -> bool + Copy,
{
    use core::arch::x86_64::{_mm256_add_epi32, _mm256_loadu_si256, _mm256_set1_epi32};

    selection.reserve(rows + 8);
    let start = selection.len();
    let output = selection
        .spare_capacity_mut()
        .as_mut_ptr()
        .cast::<BatchRowOrdinal>();
    let ordinal_offsets =
        unsafe { _mm256_loadu_si256([0i32, 1, 2, 3, 4, 5, 6, 7].as_ptr().cast()) };
    let mut row = 0usize;
    let mut written = 0usize;
    while row + 8 <= rows {
        let mut mask = 0u32;
        let all_valid = !NULLABLE || unsafe { nulls.add(row).cast::<u64>().read_unaligned() == 0 };
        if all_valid {
            for lane in 0..8 {
                let value = T::from_le(unsafe {
                    input
                        .add((row + lane) * std::mem::size_of::<T>())
                        .cast::<T>()
                        .read_unaligned()
                });
                mask |= u32::from(contains(value)) << lane;
            }
        } else {
            for lane in 0..8 {
                let value = T::from_le(unsafe {
                    input
                        .add((row + lane) * std::mem::size_of::<T>())
                        .cast::<T>()
                        .read_unaligned()
                });
                mask |=
                    u32::from(unsafe { *nulls.add(row + lane) == 0 } && contains(value)) << lane;
            }
        }
        if mask != 0 {
            let ordinal_base = _mm256_set1_epi32(row.try_into().expect("validated rows"));
            let ordinals = _mm256_add_epi32(ordinal_offsets, ordinal_base);
            written += unsafe { super::compact_ordinals_avx2(mask, ordinals, output, written) };
        }
        row += 8;
    }
    while row < rows {
        let value = T::from_le(unsafe {
            input
                .add(row * std::mem::size_of::<T>())
                .cast::<T>()
                .read_unaligned()
        });
        let valid = !NULLABLE || unsafe { *nulls.add(row) == 0 };
        if valid && contains(value) {
            unsafe {
                output
                    .add(written)
                    .write(BatchRowOrdinal::from_validated_index(row))
            };
            written += 1;
        }
        row += 1;
    }
    unsafe { selection.set_len(start + written) };
    true
}

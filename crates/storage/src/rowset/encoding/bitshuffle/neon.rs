// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded 64-row inverse bit transpose for the BSH2 numeric page layout.
//! This changes no stored representation. Each byte column is transposed in
//! NEON lanes, then interleaving stores write contiguous logical rows. Sparse
//! gathers still use the scalar eight-row primitive in the parent module.

use std::arch::aarch64::*;

/// Return the number of complete eight-row groups written.
///
/// # Safety
/// NEON must be available; WIDTH must be 4 or 8. Input and output must both
/// contain exactly `plane_bytes * 8 * WIDTH` bytes and must not overlap.
#[target_feature(enable = "neon")]
pub(super) unsafe fn unshuffle<const WIDTH: usize>(
    input: &[u8],
    plane_bytes: usize,
    output: &mut [u8],
) -> usize {
    debug_assert!(WIDTH == 4 || WIDTH == 8);
    debug_assert_eq!(input.len(), plane_bytes * 8 * WIDTH);
    debug_assert_eq!(input.len(), output.len());
    let groups = plane_bytes / 8 * 8;
    // SAFETY: all loads address eight consecutive bytes in a complete plane
    // tile. Stores cover exactly 64 logical rows for each eight-group tile.
    // The wrapper validates lengths and dispatches only the two fixed widths.
    unsafe {
        for group in (0..groups).step_by(8) {
            let columns: [[uint8x8_t; 8]; WIDTH] = std::array::from_fn(|column| {
                let base = input.as_ptr().add(column * 8 * plane_bytes + group);
                let mut p: [uint8x8_t; 8] =
                    std::array::from_fn(|bit| vld1_u8(base.add(bit * plane_bytes)));
                // Each lane independently transposes one 8x8 bit matrix.
                // Exchange bit fields, then byte-interleave the eight row
                // vectors into one register per contiguous eight-row group.
                macro_rules! exchange {
                    ($a:expr, $b:expr, $shift:literal, $mask:expr) => {{
                        let swap =
                            vand_u8(veor_u8(vshr_n_u8::<$shift>(p[$a]), p[$b]), vdup_n_u8($mask));
                        p[$a] = veor_u8(p[$a], vshl_n_u8::<$shift>(swap));
                        p[$b] = veor_u8(p[$b], swap);
                    }};
                }
                for i in 0..4 {
                    exchange!(i, i + 4, 4, 0x0f);
                }
                for i in [0, 1, 4, 5] {
                    exchange!(i, i + 2, 2, 0x33);
                }
                for i in [0, 2, 4, 6] {
                    exchange!(i, i + 1, 1, 0x55);
                }

                let pairs: [uint8x8_t; 8] = std::array::from_fn(|i| {
                    let a = p[(i / 2) * 2];
                    let b = p[(i / 2) * 2 + 1];
                    if i % 2 == 0 {
                        vzip1_u8(a, b)
                    } else {
                        vzip2_u8(a, b)
                    }
                });
                let quads: [uint16x4_t; 8] = std::array::from_fn(|i| {
                    let start = (i / 4) * 4;
                    let lane = (i % 4) / 2;
                    let a = vreinterpret_u16_u8(pairs[start + lane]);
                    let b = vreinterpret_u16_u8(pairs[start + 2 + lane]);
                    if i % 2 == 0 {
                        vzip1_u16(a, b)
                    } else {
                        vzip2_u16(a, b)
                    }
                });
                std::array::from_fn(|i| {
                    let a = vreinterpret_u32_u16(quads[i / 2]);
                    let b = vreinterpret_u32_u16(quads[4 + i / 2]);
                    vreinterpret_u8_u32(if i % 2 == 0 {
                        vzip1_u32(a, b)
                    } else {
                        vzip2_u32(a, b)
                    })
                })
            });
            for lane in 0..8 {
                let destination = output.as_mut_ptr().add((group + lane) * 8 * WIDTH);
                if WIDTH == 4 {
                    vst4_u8(
                        destination,
                        uint8x8x4_t(
                            columns[0][lane],
                            columns[1][lane],
                            columns[2][lane],
                            columns[3][lane],
                        ),
                    );
                } else {
                    // Pair byte columns, then store four half-word streams.
                    // This avoids scalar strided stores for 64-bit values.
                    for half in 0..2 {
                        let pairs: [uint16x4_t; 4] = std::array::from_fn(|i| {
                            let a = columns[i * 2][lane];
                            let b = columns[i * 2 + 1][lane];
                            vreinterpret_u16_u8(if half == 0 {
                                vzip1_u8(a, b)
                            } else {
                                vzip2_u8(a, b)
                            })
                        });
                        vst4_u16(
                            destination.add(half * 4 * WIDTH).cast(),
                            uint16x4x4_t(pairs[0], pairs[1], pairs[2], pairs[3]),
                        );
                    }
                }
            }
        }
    }
    groups
}

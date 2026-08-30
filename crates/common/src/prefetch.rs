// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Best-effort cache hints for immutable byte regions.
//!
//! Callers retain ownership and bounds proof by passing an ordinary slice.
//! Unsupported architectures deliberately degrade to a no-op; cache hints
//! must never be required for correctness.

/// Request the cache lines covering `bytes` in L1 before they are consumed.
///
/// The function only issues non-faulting read hints. It does not dereference
/// the region and has no observable semantics beyond possible cache state.
#[inline]
pub fn read_l1(bytes: &[u8]) {
    const CACHE_LINE_BYTES: usize = 64;

    for offset in (0..bytes.len()).step_by(CACHE_LINE_BYTES) {
        let address = unsafe { bytes.as_ptr().add(offset) };
        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!(
                "prfm pldl1keep, [{address}]",
                address = in(reg) address,
                options(readonly, nostack, preserves_flags)
            );
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(address.cast::<i8>(), std::arch::x86_64::_MM_HINT_T0);
        }
        #[cfg(target_arch = "x86")]
        unsafe {
            std::arch::x86::_mm_prefetch(address.cast::<i8>(), std::arch::x86::_MM_HINT_T0);
        }
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86", target_arch = "x86_64")))]
        {
            let _ = address;
        }
    }
}

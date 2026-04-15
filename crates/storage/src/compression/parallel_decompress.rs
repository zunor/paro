//! Parallel decompression helpers for page bodies.
//!
//! This module provides a small utility to decompress multiple page bodies in
//! parallel, while allocating output buffers via BufferAllocator-compatible
//! allocators (Allocator trait).

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use tracing::trace;

use crate::compression::BlockCompressionType;
use crate::metrics::storage_metrics;

const SLOW_DECOMPRESS_BATCH_THRESHOLD: Duration = Duration::from_millis(8);

/// A single decompression task for a page body.
#[derive(Debug, Clone, Copy)]
pub struct ParallelDecompressTask<'a> {
    pub body: &'a [u8],
    pub uncompressed_size: usize,
    pub codec: Option<BlockCompressionType>,
}

/// Parallel decompressor that reuses a BufferAllocator-compatible allocator.
#[derive(Clone)]
pub struct ParallelDecompressor {
    allocator: Arc<dyn Allocator>,
    max_threads: usize,
}

impl ParallelDecompressor {
    /// Create a new decompressor with default parallelism.
    pub fn new(allocator: Arc<dyn Allocator>) -> Self {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self {
            allocator,
            max_threads: threads,
        }
    }

    /// Override the maximum number of threads to use.
    /// If set to 0, auto-detects available parallelism.
    pub fn with_max_threads(mut self, max_threads: usize) -> Self {
        self.max_threads = max_threads;
        self
    }

    /// Get the allocator used by this decompressor.
    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        &self.allocator
    }

    /// Decompress a single page body.
    pub fn decompress_one(
        &self,
        body: &[u8],
        uncompressed_size: usize,
        codec: Option<BlockCompressionType>,
    ) -> Result<Bytes> {
        let task = ParallelDecompressTask {
            body,
            uncompressed_size,
            codec,
        };
        decompress_task(&task, &self.allocator)
    }

    /// Decompress a batch of page bodies in parallel.
    ///
    /// Results are returned in the same order as tasks.
    pub fn decompress_batch<'a>(&self, tasks: &[ParallelDecompressTask<'a>]) -> Result<Vec<Bytes>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let compressed_bytes: usize = tasks.iter().map(|task| task.body.len()).sum();
        let uncompressed_bytes: usize = tasks.iter().map(|task| task.uncompressed_size).sum();
        let max_threads = if self.max_threads == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            self.max_threads
        };

        let thread_count = std::cmp::min(max_threads.max(1), tasks.len());
        storage_metrics().record_parallel_decompress(thread_count, tasks.len());
        if thread_count <= 1 {
            let start = Instant::now();
            let output = tasks
                .iter()
                .map(|task| decompress_task(task, &self.allocator))
                .collect();
            let elapsed = start.elapsed();
            if elapsed >= SLOW_DECOMPRESS_BATCH_THRESHOLD {
                trace!(
                    workers = 1usize,
                    tasks = tasks.len(),
                    compressed_bytes,
                    uncompressed_bytes,
                    elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                    "slow sequential page decompression batch",
                );
            }
            return output;
        }

        let start = Instant::now();
        let chunk_size = tasks.len().div_ceil(thread_count);
        let mut results: Vec<Option<Result<Bytes>>> = Vec::with_capacity(tasks.len());
        results.resize_with(tasks.len(), || None);

        let mut join_error: Option<paro_common::error::ParoError> = None;
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for (chunk_idx, task_chunk) in tasks.chunks(chunk_size).enumerate() {
                let start = chunk_idx * chunk_size;
                let allocator = self.allocator.clone();
                handles.push(scope.spawn(move || {
                    let mut local = Vec::with_capacity(task_chunk.len());
                    for (offset, task) in task_chunk.iter().enumerate() {
                        local.push((start + offset, decompress_task(task, &allocator)));
                    }
                    local
                }));
            }

            for handle in handles {
                match handle.join() {
                    Ok(local) => {
                        for (idx, result) in local {
                            results[idx] = Some(result);
                        }
                    }
                    Err(_) => {
                        join_error =
                            Some(paro_error::internal("parallel decompression task panicked"));
                        break;
                    }
                }
            }
        });

        if let Some(err) = join_error {
            return Err(err);
        }

        let mut output = Vec::with_capacity(tasks.len());
        for slot in results {
            match slot {
                Some(Ok(bytes)) => output.push(bytes),
                Some(Err(err)) => return Err(err),
                None => return Err(paro_error::internal("parallel decompression task missing")),
            }
        }

        let elapsed = start.elapsed();
        if elapsed >= SLOW_DECOMPRESS_BATCH_THRESHOLD {
            trace!(
                workers = thread_count,
                tasks = tasks.len(),
                compressed_bytes,
                uncompressed_bytes,
                elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                "slow parallel page decompression batch",
            );
        }

        Ok(output)
    }
}

impl std::fmt::Debug for ParallelDecompressor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelDecompressor")
            .field("allocator", &self.allocator.name())
            .field("max_threads", &self.max_threads)
            .finish()
    }
}

struct AllocatedBytes {
    ptr: *mut u8,
    len: usize,
    allocator: Arc<dyn Allocator>,
}

// SAFETY: AllocatedBytes owns an immutable byte buffer that is safe to move
// across threads. The buffer is only freed on drop when the last Bytes handle
// is released.
unsafe impl Send for AllocatedBytes {}

impl AllocatedBytes {
    fn new(allocator: Arc<dyn Allocator>, len: usize) -> Result<Self> {
        if len == 0 {
            return Ok(Self {
                ptr: std::ptr::null_mut(),
                len: 0,
                allocator,
            });
        }

        let ptr = allocator.allocate(len)?;
        Ok(Self {
            ptr,
            len,
            allocator,
        })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        if self.len == 0 {
            &mut []
        } else {
            // SAFETY: ptr is allocated with len bytes.
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
        }
    }
}

impl AsRef<[u8]> for AllocatedBytes {
    fn as_ref(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            // SAFETY: ptr is allocated with len bytes.
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

impl Drop for AllocatedBytes {
    fn drop(&mut self) {
        if !self.ptr.is_null() && self.len > 0 {
            self.allocator.free(self.ptr, self.len);
        }
    }
}

fn decompress_task(
    task: &ParallelDecompressTask<'_>,
    allocator: &Arc<dyn Allocator>,
) -> Result<Bytes> {
    let expected = task.uncompressed_size;
    if expected == 0 {
        return Ok(Bytes::new());
    }

    let body = task.body;
    if body.len() == expected {
        return copy_into_allocated(body, allocator);
    }

    let codec = task.codec.unwrap_or(BlockCompressionType::Lz4);
    match codec {
        BlockCompressionType::None => Err(paro_error::data_corrupted(format!(
            "Bad page: uncompressed size mismatch ({} vs {})",
            body.len(),
            expected
        ))),
        BlockCompressionType::Lz4 => decompress_lz4(body, expected, allocator),
        BlockCompressionType::Zstd => decompress_zstd(body, expected, allocator),
    }
}

fn copy_into_allocated(input: &[u8], allocator: &Arc<dyn Allocator>) -> Result<Bytes> {
    if input.is_empty() {
        return Ok(Bytes::new());
    }

    let mut output = AllocatedBytes::new(allocator.clone(), input.len())?;
    output.as_mut_slice().copy_from_slice(input);
    Ok(Bytes::from_owner(output))
}

fn decompress_lz4(input: &[u8], expected: usize, allocator: &Arc<dyn Allocator>) -> Result<Bytes> {
    if input.len() < 4 {
        return Err(paro_error::data_corrupted(format!(
            "Bad page: too small ({})",
            input.len()
        )));
    }

    let size = u32::from_le_bytes([input[0], input[1], input[2], input[3]]) as usize;
    if size != expected {
        return Err(paro_error::data_corrupted(format!(
            "Bad page: uncompressed size mismatch ({} vs {})",
            size, expected
        )));
    }

    let mut output = AllocatedBytes::new(allocator.clone(), expected)?;
    let written = lz4_flex::decompress_into(&input[4..], output.as_mut_slice())
        .map_err(|e| paro_error::data_corrupted(format!("LZ4 decompression failed: {}", e)))?;

    if written != expected {
        return Err(paro_error::data_corrupted(format!(
            "Bad page: uncompressed size mismatch ({} vs {})",
            written, expected
        )));
    }

    Ok(Bytes::from_owner(output))
}

fn decompress_zstd(input: &[u8], expected: usize, allocator: &Arc<dyn Allocator>) -> Result<Bytes> {
    let mut output = AllocatedBytes::new(allocator.clone(), expected)?;
    let written = zstd::bulk::decompress_to_buffer(input, output.as_mut_slice())
        .map_err(|e| paro_error::data_corrupted(format!("ZSTD decompression failed: {}", e)))?;

    if written != expected {
        return Err(paro_error::data_corrupted(format!(
            "Bad page: uncompressed size mismatch ({} vs {})",
            written, expected
        )));
    }

    Ok(Bytes::from_owner(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::storage_metrics;
    use paro_common::allocator::default_allocator;

    #[test]
    fn parallel_decompress_updates_metrics() {
        storage_metrics().reset_for_tests();
        let decompressor =
            ParallelDecompressor::new(Arc::new(default_allocator())).with_max_threads(4);
        let payload = vec![7u8; 1024];
        let task = ParallelDecompressTask {
            body: &payload,
            uncompressed_size: payload.len(),
            codec: None,
        };
        let tasks = vec![task; 8];

        let output = decompressor.decompress_batch(&tasks).unwrap();
        assert_eq!(output.len(), 8);
        for bytes in output {
            assert_eq!(bytes.len(), payload.len());
        }

        let snap = storage_metrics().snapshot();
        assert!(snap.decompress_parallel_batches >= 1);
        assert!(snap.decompress_parallel_tasks >= 8);
        assert!(snap.decompress_parallelism_last >= 1);
        assert!(snap.decompress_parallelism_peak >= snap.decompress_parallelism_last);
    }

    #[test]
    fn parallel_decompress_preserves_task_order() {
        let decompressor =
            ParallelDecompressor::new(Arc::new(default_allocator())).with_max_threads(4);

        let payloads = [
            vec![1u8; 2048],
            vec![2u8; 2048],
            vec![3u8; 2048],
            vec![4u8; 2048],
        ];
        let compressed: Vec<Vec<u8>> = payloads
            .iter()
            .map(|payload| lz4_flex::compress_prepend_size(payload))
            .collect();

        let tasks: Vec<ParallelDecompressTask<'_>> = compressed
            .iter()
            .zip(payloads.iter())
            .map(|(body, payload)| ParallelDecompressTask {
                body,
                uncompressed_size: payload.len(),
                codec: Some(BlockCompressionType::Lz4),
            })
            .collect();

        let output = decompressor.decompress_batch(&tasks).unwrap();
        assert_eq!(output.len(), payloads.len());
        for (actual, expected) in output.iter().zip(payloads.iter()) {
            assert_eq!(actual.as_ref(), expected.as_slice());
        }
    }

    #[test]
    fn parallel_decompress_error_matches_single_task_error() {
        let decompressor =
            ParallelDecompressor::new(Arc::new(default_allocator())).with_max_threads(4);

        let good_payload = vec![7u8; 1024];
        let bad_payload = vec![9u8; 1024];
        let good_body = lz4_flex::compress_prepend_size(&good_payload);
        let mut bad_body = lz4_flex::compress_prepend_size(&bad_payload);
        bad_body[0] = bad_body[0].wrapping_add(1); // Corrupt encoded uncompressed size prefix.

        let tasks = vec![
            ParallelDecompressTask {
                body: &good_body,
                uncompressed_size: good_payload.len(),
                codec: Some(BlockCompressionType::Lz4),
            },
            ParallelDecompressTask {
                body: &bad_body,
                uncompressed_size: bad_payload.len(),
                codec: Some(BlockCompressionType::Lz4),
            },
        ];

        let batch_err = decompressor.decompress_batch(&tasks).unwrap_err();
        let single_err = decompressor
            .decompress_one(
                &bad_body,
                bad_payload.len(),
                Some(BlockCompressionType::Lz4),
            )
            .unwrap_err();
        let batch_msg = batch_err.to_string();
        let single_msg = single_err.to_string();
        assert!(
            batch_msg.contains("uncompressed size mismatch"),
            "unexpected error: {}",
            batch_msg
        );
        assert_eq!(batch_msg, single_msg);
    }
}

//! Bounded result buffering with backpressure for streaming execution.

use std::collections::VecDeque;
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::ParoError;
use paro_scheduler::task::InterruptState;

/// Buffered data queue for streaming execution.
///
/// This structure provides a bounded buffer that sits between the pipeline sink
/// and the result handler. When the buffer is full, the sink will be blocked,
/// implementing backpressure to prevent unbounded memory growth.
///
/// # Design
///
/// The buffer has a maximum capacity measured in number of chunks. When the
/// buffer reaches capacity, `try_append()` will return `AppendResult::Full`.
/// The sink should then call `block_sink()` to register for notification when
/// space becomes available.
///
/// # Thread Safety
///
/// This structure is NOT thread-safe by itself. It should be wrapped in
/// `Arc<Mutex<BufferedData>>` when shared between threads.
pub struct BufferedData {
    /// Buffered chunks (bounded queue)
    chunks: VecDeque<Chunk>,
    /// Maximum buffer size (number of chunks)
    capacity: usize,
    /// Whether the producer has finished
    closed: bool,
    /// Error state (if any)
    error: Option<ParoError>,
    /// Allocator for memory management
    allocator: Arc<dyn Allocator>,
    /// Blocked sink tasks waiting for buffer space.
    blocked_sinks: VecDeque<InterruptState>,
}

/// Result of attempting to append a chunk to the buffer.
#[derive(Debug)]
pub enum AppendResult {
    /// Chunk was successfully appended
    Success,
    /// Buffer is full, chunk is returned to caller
    Full(Chunk),
    /// Buffer is closed, cannot append
    Closed,
}

impl BufferedData {
    /// Create a new buffered data queue with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of chunks to buffer
    /// * `allocator` - Allocator for memory management
    ///
    /// # Example
    ///
    /// ```ignore
    /// let buffer = BufferedData::new(10, allocator);
    /// ```
    pub fn new(capacity: usize, allocator: Arc<dyn Allocator>) -> Self {
        Self {
            chunks: VecDeque::with_capacity(capacity),
            capacity,
            closed: false,
            error: None,
            allocator,
            blocked_sinks: VecDeque::new(),
        }
    }

    /// Try to append a chunk to the buffer.
    ///
    /// This is the new API that supports blocking. If the buffer is full,
    /// the chunk is returned and the caller should call `block_sink()`.
    ///
    /// # Returns
    ///
    /// * `AppendResult::Success` - Chunk was successfully appended
    /// * `AppendResult::Full(chunk)` - Buffer is full, chunk returned
    /// * `AppendResult::Closed` - Buffer is closed
    ///
    /// # Example
    ///
    /// ```ignore
    /// match buffer.try_append(chunk) {
    ///     AppendResult::Success => { /* continue */ }
    ///     AppendResult::Full(chunk) => {
    ///         buffer.block_sink(interrupt_state);
    ///         // Save chunk for retry
    ///     }
    ///     AppendResult::Closed => { /* error */ }
    /// }
    /// ```
    pub fn try_append(&mut self, chunk: Chunk) -> AppendResult {
        if self.closed {
            return AppendResult::Closed;
        }

        if self.is_full() {
            return AppendResult::Full(chunk);
        }

        self.chunks.push_back(chunk);
        AppendResult::Success
    }

    /// Scan (consume) the next chunk from the buffer.
    ///
    /// # Returns
    ///
    /// * `Some(chunk)` - A chunk was available
    /// * `None` - Buffer is empty
    ///
    /// # Example
    ///
    /// ```ignore
    /// while let Some(chunk) = buffer.scan() {
    ///     // process chunk
    /// }
    /// ```
    pub fn scan(&mut self) -> Option<Chunk> {
        self.chunks.pop_front()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Check if the buffer is full.
    pub fn is_full(&self) -> bool {
        self.chunks.len() >= self.capacity
    }

    /// Close the buffer, indicating no more data will be appended.
    ///
    /// After closing, `try_append()` will return `AppendResult::Closed`.
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Set an error state for the buffer.
    ///
    /// This is used to propagate errors from the producer to the consumer.
    pub fn set_error(&mut self, error: ParoError) {
        self.error = Some(error);
        self.closed = true;
    }

    /// Check if the buffer is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Get the error state, if any.
    pub fn error(&self) -> Option<&ParoError> {
        self.error.as_ref()
    }

    /// Get the current number of buffered chunks.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Get the maximum capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get a reference to the allocator.
    pub fn allocator(&self) -> &Arc<dyn Allocator> {
        &self.allocator
    }

    /// Block a sink task when the buffer is full.
    ///
    /// The sink task will be unblocked when space becomes available.
    ///
    /// # Arguments
    ///
    /// * `interrupt_state` - The interrupt state for the blocked sink
    ///
    /// # Returns
    ///
    /// * `true` - Sink was successfully blocked
    /// * `false` - Cannot block (buffer is closed)
    pub fn block_sink(&mut self, interrupt_state: InterruptState) -> bool {
        if self.closed {
            return false;
        }
        self.blocked_sinks.push_back(interrupt_state);
        true
    }

    /// Unblock sinks that are waiting for buffer space.
    ///
    /// Wakes up blocked sinks when the buffer has space available.
    ///
    /// # Returns
    ///
    /// The number of sinks that were unblocked.
    pub fn unblock_sinks(&mut self) -> usize {
        if self.is_full() {
            return 0;
        }

        let mut unblocked = 0;
        while !self.blocked_sinks.is_empty() && !self.is_full() {
            if let Some(interrupt_state) = self.blocked_sinks.pop_front() {
                // Call the callback to reschedule the blocked sink
                let _ = interrupt_state.callback();
                unblocked += 1;
            }
        }
        unblocked
    }

    /// Get the number of blocked sinks.
    pub fn blocked_sink_count(&self) -> usize {
        self.blocked_sinks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::allocator::default_allocator;
    use paro_scheduler::task::InterruptDoneSignalState;
    use paro_scheduler::task::InterruptState;

    #[test]
    fn test_try_append_success() {
        let allocator = Arc::new(default_allocator().clone());
        let mut buffer = BufferedData::new(2, allocator.clone());

        let chunk = Chunk::with_allocator(allocator.clone());
        match buffer.try_append(chunk) {
            AppendResult::Success => {}
            _ => panic!("Expected Success"),
        }

        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_try_append_full() {
        let allocator = Arc::new(default_allocator().clone());
        let mut buffer = BufferedData::new(1, allocator.clone());

        // Fill the buffer
        let chunk1 = Chunk::with_allocator(allocator.clone());
        buffer.try_append(chunk1).unwrap_success();

        // Try to append when full
        let chunk2 = Chunk::with_allocator(allocator.clone());
        match buffer.try_append(chunk2) {
            AppendResult::Full(_) => {}
            _ => panic!("Expected Full"),
        }
    }

    #[test]
    fn test_block_and_unblock_sinks() {
        let allocator = Arc::new(default_allocator().clone());
        let mut buffer = BufferedData::new(2, allocator.clone());

        // Create interrupt states
        let signal1 = InterruptDoneSignalState::new();
        let signal2 = InterruptDoneSignalState::new();
        let int1 = InterruptState::with_signal(signal1.downgrade());
        let int2 = InterruptState::with_signal(signal2.downgrade());

        // Block two sinks
        assert!(buffer.block_sink(int1));
        assert!(buffer.block_sink(int2));
        assert_eq!(buffer.blocked_sink_count(), 2);

        // Unblock sinks (buffer has space)
        let unblocked = buffer.unblock_sinks();
        assert_eq!(unblocked, 2);
        assert_eq!(buffer.blocked_sink_count(), 0);
    }

    #[test]
    fn test_unblock_sinks_when_full() {
        let allocator = Arc::new(default_allocator().clone());
        let mut buffer = BufferedData::new(1, allocator.clone());

        // Fill the buffer
        let chunk = Chunk::with_allocator(allocator.clone());
        buffer.try_append(chunk).unwrap_success();

        // Block a sink
        let signal = InterruptDoneSignalState::new();
        let int_state = InterruptState::with_signal(signal.downgrade());
        buffer.block_sink(int_state);

        // Try to unblock when buffer is full - should not unblock
        let unblocked = buffer.unblock_sinks();
        assert_eq!(unblocked, 0);
        assert_eq!(buffer.blocked_sink_count(), 1);

        // Scan to make space
        buffer.scan();

        // Now unblock should work
        let unblocked = buffer.unblock_sinks();
        assert_eq!(unblocked, 1);
        assert_eq!(buffer.blocked_sink_count(), 0);
    }
}

// Helper trait for tests
#[cfg(test)]
trait AppendResultExt {
    fn unwrap_success(self);
}

#[cfg(test)]
impl AppendResultExt for AppendResult {
    fn unwrap_success(self) {
        match self {
            AppendResult::Success => {}
            _ => panic!("Expected AppendResult::Success"),
        }
    }
}

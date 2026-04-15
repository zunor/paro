//! Random Engine
//!
//! Session-level random number generator for SQL `random()` and `setseed()` functions.
//!
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Random number generator for session-level randomness.
///
/// This provides deterministic random number generation when seeded,
/// supporting the `random()` and `setseed()` SQL functions.
///
/// # Thread Safety
/// The engine uses internal locking for thread-safe access.
///
/// # Example
/// ```ignore
/// let mut engine = RandomEngine::new();
/// engine.set_seed(42);
/// let value = engine.next_random(); // Deterministic value
/// ```
pub struct RandomEngine {
    /// Internal state protected by mutex for thread safety
    state: Mutex<RandomState>,
}

/// Internal random state using a simple xorshift64 algorithm.
struct RandomState {
    /// Current state value
    state: u64,
}

impl RandomState {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// xorshift64 algorithm for fast, decent quality random numbers
    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

impl RandomEngine {
    /// Creates a new random engine with a random seed.
    pub fn new() -> Self {
        // Use a combination of time and a static counter for initial seed
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let time_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(12345);
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let seed = time_seed.wrapping_add(counter);

        Self {
            state: Mutex::new(RandomState::new(seed)),
        }
    }

    /// Creates a new random engine with a specific seed.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            state: Mutex::new(RandomState::new(seed)),
        }
    }

    /// Sets the seed for the random engine.
    ///
    /// This corresponds to the SQL `setseed()` function.
    /// The seed value should be between 0.0 and 1.0 for PostgreSQL compatibility.
    pub fn set_seed(&self, seed: f64) {
        // Convert float seed to u64 (PostgreSQL compatibility)
        let seed_u64 = (seed * u64::MAX as f64) as u64;
        if let Ok(mut state) = self.state.lock() {
            *state = RandomState::new(seed_u64);
        }
    }

    /// Sets the seed using a raw u64 value.
    pub fn set_seed_raw(&self, seed: u64) {
        if let Ok(mut state) = self.state.lock() {
            *state = RandomState::new(seed);
        }
    }

    /// Generates a random number between 0.0 and 1.0.
    ///
    /// This corresponds to the SQL `random()` function.
    pub fn next_random(&self) -> f64 {
        if let Ok(mut state) = self.state.lock() {
            let value = state.next();
            // Convert to [0, 1) range using ldexp equivalent
            (value as f64) / (u64::MAX as f64)
        } else {
            0.5 // Fallback if lock fails
        }
    }

    /// Generates a random number between min and max.
    pub fn next_random_range(&self, min: f64, max: f64) -> f64 {
        min + (self.next_random() * (max - min))
    }

    /// Generates a random 32-bit integer.
    pub fn next_random_integer(&self) -> u32 {
        if let Ok(mut state) = self.state.lock() {
            state.next() as u32
        } else {
            0
        }
    }

    /// Generates a random 64-bit integer.
    pub fn next_random_integer64(&self) -> u64 {
        if let Ok(mut state) = self.state.lock() {
            state.next()
        } else {
            0
        }
    }

    /// Generates a random integer in the given range [min, max).
    pub fn next_random_integer_range(&self, min: u32, max: u32) -> u32 {
        min + (self.next_random() * (max - min) as f64) as u32
    }

    /// Fills a buffer with random bytes.
    pub fn random_data(&self, data: &mut [u8]) {
        let mut offset = 0;
        while offset < data.len() {
            let random = self.next_random_integer64();
            let bytes = random.to_le_bytes();
            let remaining = data.len() - offset;
            let to_copy = remaining.min(8);
            data[offset..offset + to_copy].copy_from_slice(&bytes[..to_copy]);
            offset += to_copy;
        }
    }
}

impl Default for RandomEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RandomEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RandomEngine").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_engine_deterministic_with_seed() {
        let engine1 = RandomEngine::with_seed(42);
        let engine2 = RandomEngine::with_seed(42);

        // Same seed should produce same sequence
        for _ in 0..10 {
            assert_eq!(engine1.next_random(), engine2.next_random());
        }
    }

    #[test]
    fn test_random_engine_set_seed() {
        let engine = RandomEngine::with_seed(100);
        let v1 = engine.next_random();

        // Reset seed
        engine.set_seed_raw(100);
        let v2 = engine.next_random();

        assert_eq!(v1, v2);
    }

    #[test]
    fn test_random_engine_range() {
        let engine = RandomEngine::with_seed(42);

        for _ in 0..100 {
            let value = engine.next_random();
            assert!((0.0..1.0).contains(&value));
        }
    }

    #[test]
    fn test_random_engine_range_custom() {
        let engine = RandomEngine::with_seed(42);

        for _ in 0..100 {
            let value = engine.next_random_range(10.0, 20.0);
            assert!((10.0..20.0).contains(&value));
        }
    }

    #[test]
    fn test_random_engine_random_data() {
        let engine = RandomEngine::with_seed(42);
        let mut data = [0u8; 32];
        engine.random_data(&mut data);

        // Should not be all zeros
        assert!(data.iter().any(|&b| b != 0));

        // Same seed should produce same data
        let engine2 = RandomEngine::with_seed(42);
        let mut data2 = [0u8; 32];
        engine2.random_data(&mut data2);
        assert_eq!(data, data2);
    }
}

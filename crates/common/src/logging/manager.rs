//! Log manager for dynamic configuration at runtime.
//!
//! Provides the ability to modify logging configuration (level, filters)
//! without restarting the application.

use crate::config::{LogFormat, LogLevel, LoggingConfig};
use crate::logging::layer::MemoryLayer;
use crate::logging::storage::MemoryLogStorage;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::reload::{self, Handle};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

/// Default number of log entries to keep in memory.
const DEFAULT_MEMORY_LOG_CAPACITY: usize = 10000;

/// Log manager that provides runtime control over logging.
///
/// # Features
///
/// - Dynamic log level modification
/// - Dynamic filter rule changes
/// - In-memory log storage for SQL queries
/// - Enable/disable logging at runtime
///
/// # Example
///
/// ```rust,ignore
/// use paro_common::logging::LogManager;
/// use paro_common::config::{LoggingConfig, LogLevel};
///
/// let manager = LogManager::init(LoggingConfig::default())?;
///
/// // Change log level at runtime
/// manager.set_level(LogLevel::Debug)?;
///
/// // Set custom filter
/// manager.set_filter("paro::query=trace,paro::storage=debug")?;
///
/// // Query stored logs
/// let storage = manager.memory_storage();
/// let entries = storage.all();
/// ```
pub struct LogManager {
    /// Current configuration (protected by RwLock for dynamic updates).
    config: RwLock<LoggingConfig>,
    /// Handle to reload the EnvFilter dynamically.
    filter_handle: Handle<EnvFilter, Registry>,
    /// In-memory log storage.
    memory_storage: Arc<MemoryLogStorage>,
}

impl LogManager {
    /// Initialize the log manager and set up the global tracing subscriber.
    ///
    /// This should be called once at application startup.
    pub fn init(config: LoggingConfig) -> Result<Arc<Self>, LogError> {
        Self::init_with_capacity(config, DEFAULT_MEMORY_LOG_CAPACITY)
    }

    /// Initialize with custom memory storage capacity.
    pub fn init_with_capacity(
        config: LoggingConfig,
        memory_capacity: usize,
    ) -> Result<Arc<Self>, LogError> {
        // Create memory storage
        let memory_storage = Arc::new(MemoryLogStorage::new(memory_capacity));

        // Build the initial filter
        let level_filter: LevelFilter = config.level.into();
        let env_filter = EnvFilter::builder()
            .with_default_directive(level_filter.into())
            .from_env_lossy();

        // Create a reloadable filter layer
        let (filter_layer, filter_handle) = reload::Layer::new(env_filter);

        // Create the memory layer
        let memory_layer = MemoryLayer::new(Arc::clone(&memory_storage));

        // Build subscriber based on format
        match config.format {
            LogFormat::Pretty => {
                let fmt_layer = fmt::layer()
                    .with_target(config.with_target)
                    .with_file(config.with_file)
                    .with_line_number(config.with_line_number)
                    .with_ansi(config.ansi);

                tracing_subscriber::registry()
                    .with(filter_layer)
                    .with(memory_layer)
                    .with(fmt_layer)
                    .init();
            }
            LogFormat::Json => {
                let fmt_layer = fmt::layer()
                    .json()
                    .with_target(config.with_target)
                    .with_file(config.with_file)
                    .with_line_number(config.with_line_number)
                    .with_span_events(FmtSpan::CLOSE);

                tracing_subscriber::registry()
                    .with(filter_layer)
                    .with(memory_layer)
                    .with(fmt_layer)
                    .init();
            }
            LogFormat::Compact => {
                let fmt_layer = fmt::layer()
                    .compact()
                    .with_target(config.with_target)
                    .with_file(config.with_file)
                    .with_line_number(config.with_line_number)
                    .with_ansi(config.ansi);

                tracing_subscriber::registry()
                    .with(filter_layer)
                    .with(memory_layer)
                    .with(fmt_layer)
                    .init();
            }
        }

        Ok(Arc::new(Self {
            config: RwLock::new(config),
            filter_handle,
            memory_storage,
        }))
    }

    /// Change the log level at runtime.
    ///
    /// This affects the global filter - all log output will be filtered
    /// to this minimum level.
    pub fn set_level(&self, level: LogLevel) -> Result<(), LogError> {
        let mut config = self.config.write();
        config.level = level;

        let level_filter: LevelFilter = level.into();
        let new_filter = EnvFilter::builder()
            .with_default_directive(level_filter.into())
            .from_env_lossy();

        self.filter_handle
            .reload(new_filter)
            .map_err(|e| LogError::ReloadFailed(e.to_string()))?;

        Ok(())
    }

    /// Set a custom filter expression at runtime.
    ///
    /// The filter string follows the `RUST_LOG` format:
    /// - `debug` - Set all targets to debug
    /// - `paro=debug` - Set paro crate to debug
    /// - `paro::query=trace,paro::storage=debug` - Fine-grained control
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// manager.set_filter("paro::query=trace,paro::executor=debug")?;
    /// ```
    pub fn set_filter(&self, filter_str: &str) -> Result<(), LogError> {
        let new_filter =
            EnvFilter::try_new(filter_str).map_err(|e| LogError::InvalidFilter(e.to_string()))?;

        self.filter_handle
            .reload(new_filter)
            .map_err(|e| LogError::ReloadFailed(e.to_string()))?;

        Ok(())
    }

    /// Enable or disable all logging.
    ///
    /// When disabled, no log events will be processed (equivalent to OFF level).
    pub fn set_enabled(&self, enabled: bool) -> Result<(), LogError> {
        let mut config = self.config.write();
        config.enabled = enabled;

        if enabled {
            // Restore to configured level
            let level_filter: LevelFilter = config.level.into();
            let new_filter = EnvFilter::builder()
                .with_default_directive(level_filter.into())
                .from_env_lossy();
            self.filter_handle
                .reload(new_filter)
                .map_err(|e| LogError::ReloadFailed(e.to_string()))?;
        } else {
            // Set to OFF
            let new_filter = EnvFilter::builder()
                .with_default_directive(LevelFilter::OFF.into())
                .parse("")
                .map_err(|e| LogError::InvalidFilter(e.to_string()))?;
            self.filter_handle
                .reload(new_filter)
                .map_err(|e| LogError::ReloadFailed(e.to_string()))?;
        }

        Ok(())
    }

    /// Get the current logging configuration.
    pub fn config(&self) -> LoggingConfig {
        self.config.read().clone()
    }

    /// Get a reference to the in-memory log storage.
    ///
    /// This can be used to query logged events via SQL or programmatically.
    pub fn memory_storage(&self) -> Arc<MemoryLogStorage> {
        Arc::clone(&self.memory_storage)
    }

    /// Clear all stored log entries.
    pub fn clear_logs(&self) {
        self.memory_storage.clear();
    }

    /// Get the number of stored log entries.
    pub fn log_count(&self) -> usize {
        self.memory_storage.len()
    }
}

/// Errors that can occur during log management operations.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// Invalid filter expression.
    #[error("Invalid filter expression: {0}")]
    InvalidFilter(String),

    /// Failed to reload the filter.
    #[error("Failed to reload filter: {0}")]
    ReloadFailed(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),
}

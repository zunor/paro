// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Configuration type definitions.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

use super::human_bytes;

/// Paro unified configuration.
///
/// This is the root configuration structure that contains all configuration sections.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ParoConfig {
    /// Server configuration
    pub server: ServerConfig,
    /// Cluster configuration
    pub cluster: ClusterConfig,
    /// Logging configuration
    pub logging: LoggingConfig,
    /// Storage configuration
    pub storage: StorageConfig,
}

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Listen host address
    pub host: String,
    /// Listen port
    pub port: u16,
    /// Maximum number of connections (0 = unlimited)
    pub max_connections: usize,
    /// TLS configuration (optional)
    pub tls: Option<TlsConfig>,
    /// Allow starting in plaintext mode when TLS is configured but not implemented yet.
    pub allow_plaintext: bool,
    /// Optional upper bound for buffered COPY FROM STDIN payloads.
    #[serde(default, with = "human_bytes::optional")]
    pub copy_stdin_memory_limit: Option<usize>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 6432,
            max_connections: 0,
            tls: None,
            allow_plaintext: false,
            copy_stdin_memory_limit: None,
        }
    }
}

impl ServerConfig {
    /// Returns the full address string (host:port)
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn effective_copy_stdin_memory_limit(&self, cluster_max_memory: usize) -> usize {
        const GIB: usize = 1024 * 1024 * 1024;

        self.copy_stdin_memory_limit
            .unwrap_or_else(|| (cluster_max_memory / 4).min(GIB))
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to certificate file
    pub cert: PathBuf,
    /// Path to private key file
    pub key: PathBuf,
}

/// Cluster configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// Maximum memory in bytes (supports human-readable format like "2GB")
    #[serde(with = "human_bytes")]
    pub max_memory: usize,
    /// Number of worker threads (None = auto-detect)
    pub num_threads: Option<usize>,
    /// Worker thread pinning strategy (`off`, `on`, `auto`).
    pub pin_threads: ThreadPinMode,
    /// Default database name
    pub default_database: String,
    /// Database access mode
    pub access_mode: AccessMode,
    /// Enable external file system and network access
    pub enable_external_access: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            max_memory: 1024 * 1024 * 1024, // 1GB
            num_threads: None,
            pin_threads: ThreadPinMode::Auto,
            default_database: "postgres".to_string(),
            access_mode: AccessMode::ReadWrite,
            enable_external_access: true,
        }
    }
}

/// Database access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    /// Read and write access
    #[default]
    ReadWrite,
    /// Read-only access
    ReadOnly,
}

/// Thread pinning mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ThreadPinMode {
    /// Do not pin worker threads.
    Off,
    /// Always pin worker threads.
    On,
    /// Auto-detect (pin only on high-core-count systems).
    #[default]
    Auto,
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// Whether logging is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Log level
    pub level: LogLevel,
    /// Log output format
    pub format: LogFormat,
    /// Log file path (None = console only)
    pub file: Option<PathBuf>,
    /// Log file rotation configuration
    pub rotation: Option<RotationConfig>,
    /// Show target in log output
    pub with_target: bool,
    /// Show file location in log output
    pub with_file: bool,
    /// Show line number in log output
    pub with_line_number: bool,
    /// Use ANSI colors (console only)
    pub ansi: bool,
}

fn default_true() -> bool {
    true
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            level: LogLevel::Info,
            format: LogFormat::Pretty,
            file: None,
            rotation: None,
            with_target: true,
            with_file: false,
            with_line_number: false,
            ansi: true,
        }
    }
}

/// Log level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Most verbose
    Trace,
    /// Debug information
    Debug,
    /// General information (default)
    #[default]
    Info,
    /// Warnings
    Warn,
    /// Errors only
    Error,
}

impl LogLevel {
    /// Returns the numeric priority of the log level.
    /// Higher values mean more severe (Error = 5, Trace = 1).
    fn priority(self) -> u8 {
        match self {
            LogLevel::Trace => 1,
            LogLevel::Debug => 2,
            LogLevel::Info => 3,
            LogLevel::Warn => 4,
            LogLevel::Error => 5,
        }
    }

    /// Check if this level is at least as severe as the given level.
    ///
    /// For example, `Error.is_at_least(Info)` is true,
    /// but `Debug.is_at_least(Info)` is false.
    pub fn is_at_least(self, other: LogLevel) -> bool {
        self.priority() >= other.priority()
    }
}

impl From<tracing::Level> for LogLevel {
    fn from(level: tracing::Level) -> Self {
        match level {
            tracing::Level::TRACE => LogLevel::Trace,
            tracing::Level::DEBUG => LogLevel::Debug,
            tracing::Level::INFO => LogLevel::Info,
            tracing::Level::WARN => LogLevel::Warn,
            tracing::Level::ERROR => LogLevel::Error,
        }
    }
}

impl From<LogLevel> for tracing::Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => tracing::Level::TRACE,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Error => tracing::Level::ERROR,
        }
    }
}

impl From<LogLevel> for tracing::level_filters::LevelFilter {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => tracing::level_filters::LevelFilter::TRACE,
            LogLevel::Debug => tracing::level_filters::LevelFilter::DEBUG,
            LogLevel::Info => tracing::level_filters::LevelFilter::INFO,
            LogLevel::Warn => tracing::level_filters::LevelFilter::WARN,
            LogLevel::Error => tracing::level_filters::LevelFilter::ERROR,
        }
    }
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Pretty formatted output (default)
    #[default]
    Pretty,
    /// JSON formatted output
    Json,
    /// Compact single-line output
    Compact,
}

/// Log rotation configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotationConfig {
    /// Rotation policy
    pub policy: RotationPolicy,
    /// Maximum number of files to keep
    pub max_files: Option<usize>,
}

/// Log rotation policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RotationPolicy {
    /// No rotation
    #[default]
    Never,
    /// Rotate daily
    Daily,
    /// Rotate hourly
    Hourly,
}

/// Storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Data directory path
    pub data_dir: PathBuf,
    /// Temporary directory path (None = system temp)
    pub temp_dir: Option<PathBuf>,
    /// WAL configuration
    pub wal: WalConfig,
    /// Buffer pool configuration
    pub buffer_pool: BufferPoolConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            temp_dir: None,
            wal: WalConfig::default(),
            buffer_pool: BufferPoolConfig::default(),
        }
    }
}

/// WAL (Write-Ahead Log) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    /// Enable WAL
    pub enabled: bool,
    /// Checkpoint threshold in bytes
    #[serde(with = "human_bytes")]
    pub checkpoint_threshold: usize,
    /// Checkpoint interval
    #[serde(with = "humantime_serde")]
    pub checkpoint_interval: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            checkpoint_threshold: 10 * 1024 * 1024, // 10MB
            checkpoint_interval: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Buffer pool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BufferPoolConfig {
    /// Buffer pool size in bytes
    #[serde(with = "human_bytes")]
    pub size: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            size: 512 * 1024 * 1024, // 512MB
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ParoConfig::default();
        assert_eq!(config.server.port, 6432);
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.cluster.max_memory, 1024 * 1024 * 1024);
        assert_eq!(config.cluster.pin_threads, ThreadPinMode::Auto);
        assert_eq!(config.logging.level, LogLevel::Info);
    }

    #[test]
    fn test_server_address() {
        let config = ServerConfig::default();
        assert_eq!(config.address(), "0.0.0.0:6432");
    }

    #[test]
    fn test_deserialize_from_toml() {
        let toml_str = r#"
            [server]
            host = "127.0.0.1"
            port = 5432
            copy_stdin_memory_limit = "256MiB"

            [cluster]
            max_memory = "2GiB"
            pin_threads = "on"
            default_database = "mydb"

            [logging]
            level = "debug"
            format = "json"
        "#;

        let config: ParoConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 5432);
        assert_eq!(
            config.server.copy_stdin_memory_limit,
            Some(256 * 1024 * 1024)
        );
        // Note: bytesize uses GiB for binary units (2^30), GB for SI units (10^9)
        assert_eq!(config.cluster.max_memory, 2 * 1024 * 1024 * 1024);
        assert_eq!(config.cluster.pin_threads, ThreadPinMode::On);
        assert_eq!(config.cluster.default_database, "mydb");
        assert_eq!(config.logging.level, LogLevel::Debug);
        assert_eq!(config.logging.format, LogFormat::Json);
    }

    #[test]
    fn copy_stdin_limit_defaults_to_quarter_cluster_memory_capped_at_one_gib() {
        let mut config = ParoConfig::default();
        config.cluster.max_memory = 512 * 1024 * 1024;
        assert_eq!(
            config
                .server
                .effective_copy_stdin_memory_limit(config.cluster.max_memory),
            128 * 1024 * 1024
        );

        config.cluster.max_memory = 8 * 1024 * 1024 * 1024;
        assert_eq!(
            config
                .server
                .effective_copy_stdin_memory_limit(config.cluster.max_memory),
            1024 * 1024 * 1024
        );
    }
}

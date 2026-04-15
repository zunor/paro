// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logging initialization functions.

use crate::config::{LogFormat, LoggingConfig, RotationPolicy};
use tracing::level_filters::LevelFilter;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

/// Initialize logging with the given configuration.
///
/// This sets up the global tracing subscriber based on the logging configuration.
/// It should be called once at the start of the application.
///
/// # Features
///
/// - Console output (default)
/// - File output with optional rotation (daily, hourly)
/// - Multiple formats: pretty, json, compact
/// - Environment variable override via RUST_LOG
///
/// # Example
///
/// ```rust,ignore
/// use paro_common::config::LoggingConfig;
/// use paro_common::logging;
///
/// let config = LoggingConfig::default();
/// logging::init(&config);
///
/// tracing::info!("Logging initialized!");
/// ```
pub fn init(config: &LoggingConfig) {
    let level_filter: LevelFilter = config.level.into();
    let env_filter = EnvFilter::builder()
        .with_default_directive(level_filter.into())
        .from_env_lossy();

    // Check if file output is configured
    if let Some(ref file_path) = config.file {
        init_with_file(config, env_filter, file_path);
    } else {
        init_console_only(config, env_filter);
    }
}

/// Initialize logging with console output only.
fn init_console_only(config: &LoggingConfig, env_filter: EnvFilter) {
    match config.format {
        LogFormat::Pretty => {
            let fmt_layer = fmt::layer()
                .with_target(config.with_target)
                .with_file(config.with_file)
                .with_line_number(config.with_line_number)
                .with_ansi(config.ansi);

            tracing_subscriber::registry()
                .with(env_filter)
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
                .with(env_filter)
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
                .with(env_filter)
                .with(fmt_layer)
                .init();
        }
    }
}

/// Initialize logging with file output (and optional console).
fn init_with_file(config: &LoggingConfig, env_filter: EnvFilter, file_path: &std::path::Path) {
    // Determine rotation policy
    let rotation = config
        .rotation
        .as_ref()
        .map(|r| match r.policy {
            RotationPolicy::Never => Rotation::NEVER,
            RotationPolicy::Daily => Rotation::DAILY,
            RotationPolicy::Hourly => Rotation::HOURLY,
        })
        .unwrap_or(Rotation::NEVER);

    // Extract directory and filename
    let directory = file_path.parent().unwrap_or(std::path::Path::new("."));
    let filename = file_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("paro.log");

    // Create file appender
    let file_appender = RollingFileAppender::new(rotation, directory, filename);

    // Create layers based on format
    match config.format {
        LogFormat::Pretty | LogFormat::Compact => {
            // Console layer
            let console_layer = fmt::layer()
                .with_target(config.with_target)
                .with_file(config.with_file)
                .with_line_number(config.with_line_number)
                .with_ansi(config.ansi);

            // File layer (no ANSI colors)
            let file_layer = fmt::layer()
                .with_target(config.with_target)
                .with_file(config.with_file)
                .with_line_number(config.with_line_number)
                .with_ansi(false)
                .with_writer(file_appender);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(console_layer)
                .with(file_layer)
                .init();
        }
        LogFormat::Json => {
            // Console layer (JSON)
            let console_layer = fmt::layer()
                .json()
                .with_target(config.with_target)
                .with_file(config.with_file)
                .with_line_number(config.with_line_number)
                .with_span_events(FmtSpan::CLOSE);

            // File layer (JSON)
            let file_layer = fmt::layer()
                .json()
                .with_target(config.with_target)
                .with_file(config.with_file)
                .with_line_number(config.with_line_number)
                .with_span_events(FmtSpan::CLOSE)
                .with_writer(file_appender);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(console_layer)
                .with(file_layer)
                .init();
        }
    }
}

/// Initialize logging with default configuration.
///
/// Uses INFO level, pretty format, console output only.
/// This is a convenience function for quick initialization during development.
///
/// # Example
///
/// ```rust,ignore
/// use paro_common::logging;
///
/// logging::init_default();
/// tracing::info!("Ready!");
/// ```
pub fn init_default() {
    init(&LoggingConfig::default());
}

/// Initialize logging from environment variable (RUST_LOG).
///
/// Falls back to INFO level if no environment variable is set.
/// This is useful for development when you want to control log level
/// via environment variable without a config file.
///
/// # Example
///
/// ```bash
/// RUST_LOG=debug cargo run
/// RUST_LOG="paro::query=trace,paro::storage=debug" cargo run
/// ```
pub fn init_from_env() {
    let config = LoggingConfig::default();
    let env_filter = EnvFilter::from_default_env();

    let fmt_layer = fmt::layer()
        .with_target(config.with_target)
        .with_ansi(config.ansi);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LogLevel;

    #[test]
    fn test_log_level_conversion() {
        let level: LevelFilter = LogLevel::Debug.into();
        assert_eq!(level, LevelFilter::DEBUG);
    }
}

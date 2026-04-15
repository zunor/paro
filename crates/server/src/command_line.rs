//! Command-line interface for parod.

use clap::Parser;
use paro_common::config::{LogLevel, ParoConfig};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Command-line options for `parod`.
#[derive(Parser, Debug, Clone, Serialize, Deserialize, Default)]
#[command(name = "parod")]
#[command(version, about = "Paro Database Server - PostgreSQL compatible")]
pub struct CommandLineArgs {
    /// Configuration file path
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Server listen address (host:port or just port)
    #[arg(long, value_name = "ADDR")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen: Option<String>,

    /// Server listen port
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    /// Data directory path
    #[arg(long, value_name = "DIR")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<PathBuf>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, value_name = "LEVEL", value_parser = parse_log_level)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_level: Option<LogLevel>,

    /// Maximum memory for buffer pool (e.g., "2GiB", "512MiB")
    #[arg(long, value_name = "SIZE")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,

    /// Number of worker threads
    #[arg(long, value_name = "N")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<usize>,

    /// Print sample configuration and exit
    #[arg(long)]
    #[serde(skip)]
    pub print_config: bool,
}

fn parse_log_level(s: &str) -> Result<LogLevel, String> {
    match s.to_lowercase().as_str() {
        "trace" => Ok(LogLevel::Trace),
        "debug" => Ok(LogLevel::Debug),
        "info" => Ok(LogLevel::Info),
        "warn" | "warning" => Ok(LogLevel::Warn),
        "error" => Ok(LogLevel::Error),
        _ => Err(format!(
            "Invalid log level '{}'. Valid values: trace, debug, info, warn, error",
            s
        )),
    }
}

impl CommandLineArgs {
    pub fn parse_args() -> Self {
        Self::parse()
    }

    /// Apply CLI overrides to a `ParoConfig`.
    pub fn apply_to(&self, config: &mut ParoConfig) {
        if let Some(ref listen) = self.listen {
            if let Some((host, port)) = listen.rsplit_once(':') {
                config.server.host = host.to_string();
                if let Ok(p) = port.parse::<u16>() {
                    config.server.port = p;
                }
            } else if let Ok(port) = listen.parse::<u16>() {
                config.server.port = port;
            } else {
                config.server.host = listen.clone();
            }
        }

        if let Some(port) = self.port {
            config.server.port = port;
        }

        if let Some(ref data_dir) = self.data_dir {
            config.storage.data_dir = data_dir.clone();
        }

        if let Some(log_level) = self.log_level {
            config.logging.level = log_level;
        }

        if let Some(ref max_memory) = self.max_memory {
            if let Ok(size) = max_memory.parse::<bytesize::ByteSize>() {
                config.cluster.max_memory = size.as_u64() as usize;
            }
        }

        if let Some(threads) = self.threads {
            config.cluster.num_threads = Some(threads);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_to_overrides_listen_and_port_fields() {
        let mut config = ParoConfig::default();
        CommandLineArgs {
            listen: Some("127.0.0.1:5432".to_string()),
            ..Default::default()
        }
        .apply_to(&mut config);
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 5432);

        CommandLineArgs {
            listen: Some("9999".to_string()),
            ..Default::default()
        }
        .apply_to(&mut config);
        assert_eq!(config.server.port, 9999);

        CommandLineArgs {
            port: Some(15432),
            ..Default::default()
        }
        .apply_to(&mut config);
        assert_eq!(config.server.port, 15432);
    }
}

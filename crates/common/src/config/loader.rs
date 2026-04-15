//! Configuration loader with layered configuration support.

use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use std::path::PathBuf;

use super::ParoConfig;

/// Configuration loader.
///
/// Loads configuration from multiple sources with the following priority (low to high):
///
/// 1. Code defaults
/// 2. System config file (`/etc/paro/paro.toml`)
/// 3. User config file (`~/.config/paro/paro.toml`)
/// 4. Current directory (`./paro.toml`)
/// 5. Custom config file (specified path)
/// 6. Environment variables (`PARO_*`)
pub struct ConfigLoader;

impl ConfigLoader {
    /// Load configuration from all sources.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = ConfigLoader::load()?;
    /// println!("Server: {}:{}", config.server.host, config.server.port);
    /// ```
    pub fn load() -> Result<ParoConfig, ConfigError> {
        Self::load_with_options(None)
    }

    /// Load configuration with an optional custom config file path.
    ///
    /// # Arguments
    ///
    /// * `config_file` - Optional path to a custom configuration file
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = ConfigLoader::load_with_options(Some("/etc/paro/custom.toml".into()))?;
    /// ```
    pub fn load_with_options(config_file: Option<PathBuf>) -> Result<ParoConfig, ConfigError> {
        let mut figment = Figment::new()
            // 1. Code defaults
            .merge(Serialized::defaults(ParoConfig::default()))
            // 2. System config file
            .merge(Toml::file("/etc/paro/paro.toml").nested())
            // 3. User config file
            .merge(Toml::file(Self::user_config_path()).nested())
            // 4. Current directory config file
            .merge(Toml::file("paro.toml").nested());

        // 5. Custom config file (if specified)
        if let Some(path) = config_file {
            figment = figment.merge(Toml::file(path).nested());
        }

        // 6. Environment variables (PARO_SERVER__PORT=6432)
        figment = figment.merge(Env::prefixed("PARO_").split("__"));

        let config: ParoConfig = figment
            .extract()
            .map_err(|e| ConfigError::Load(Box::new(e)))?;

        // Validate configuration
        config.validate()?;

        Ok(config)
    }

    /// Load configuration from a specific file only (plus defaults).
    ///
    /// This is useful for testing or when you want to load from a specific file
    /// without the layered configuration.
    pub fn load_from_file(path: &PathBuf) -> Result<ParoConfig, ConfigError> {
        let figment = Figment::new()
            .merge(Serialized::defaults(ParoConfig::default()))
            .merge(Toml::file(path).nested());

        let config: ParoConfig = figment
            .extract()
            .map_err(|e| ConfigError::Load(Box::new(e)))?;
        config.validate()?;

        Ok(config)
    }

    /// Load configuration from a TOML string (useful for testing).
    pub fn load_from_str(toml_str: &str) -> Result<ParoConfig, ConfigError> {
        let config: ParoConfig =
            toml::from_str(toml_str).map_err(|e| ConfigError::Parse(Box::new(e)))?;
        config.validate()?;
        Ok(config)
    }

    /// Get the user config file path.
    ///
    /// Returns `~/.config/paro/paro.toml` on Unix-like systems.
    pub fn user_config_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("paro")
            .join("paro.toml")
    }

    /// Get the system config file path.
    pub fn system_config_path() -> PathBuf {
        PathBuf::from("/etc/paro/paro.toml")
    }

    /// Generate a sample configuration file content.
    pub fn sample_config() -> &'static str {
        include_str!("sample_config.toml")
    }
}

/// Configuration error types.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Failed to load configuration from sources
    #[error("Failed to load configuration: {0}")]
    Load(Box<figment::Error>),

    /// Failed to parse TOML
    #[error("Failed to parse configuration: {0}")]
    Parse(Box<toml::de::Error>),

    /// Configuration validation failed
    #[error("Invalid configuration: {0}")]
    Validation(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<figment::Error> for ConfigError {
    fn from(err: figment::Error) -> Self {
        Self::Load(Box::new(err))
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(err: toml::de::Error) -> Self {
        Self::Parse(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_defaults() {
        // Test that default config is valid and has expected values
        // Use load_from_str with empty string to test defaults without env vars
        let config = ConfigLoader::load_from_str("").unwrap();
        assert_eq!(config.server.port, 6432);
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.cluster.max_memory, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_load_from_str() {
        let toml_str = r#"
            [server]
            port = 5432
            
            [logging]
            level = "debug"
        "#;

        let config = ConfigLoader::load_from_str(toml_str).unwrap();
        assert_eq!(config.server.port, 5432);
        assert_eq!(config.logging.level, super::super::LogLevel::Debug);
    }

    #[test]
    fn test_env_override() {
        // Use a unique environment variable to avoid test conflicts
        // (tests run in parallel)
        let unique_var = "PARO_TEST_CLUSTER__NUM_THREADS";
        std::env::set_var(unique_var, "42");

        let figment = Figment::new()
            .merge(Serialized::defaults(ParoConfig::default()))
            .merge(Env::prefixed("PARO_TEST_").split("__"));

        let config: ParoConfig = figment.extract().unwrap();
        assert_eq!(config.cluster.num_threads, Some(42));

        // Clean up
        std::env::remove_var(unique_var);
    }

    #[test]
    fn test_user_config_path() {
        let path = ConfigLoader::user_config_path();
        assert!(path.ends_with("paro/paro.toml"));
    }
}

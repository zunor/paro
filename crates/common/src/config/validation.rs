// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Configuration validation.

use super::{ConfigError, ParoConfig};

impl ParoConfig {
    /// Validate the configuration.
    ///
    /// Returns an error if any configuration value is invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_server()?;
        self.validate_cluster()?;
        self.validate_storage()?;
        Ok(())
    }

    fn validate_server(&self) -> Result<(), ConfigError> {
        // Port must be valid (non-zero, not reserved unless running as root)
        if self.server.port == 0 {
            return Err(ConfigError::Validation(
                "server.port cannot be 0".to_string(),
            ));
        }

        // Host must not be empty
        if self.server.host.is_empty() {
            return Err(ConfigError::Validation(
                "server.host cannot be empty".to_string(),
            ));
        }

        // Validate TLS config if present
        if let Some(ref tls) = self.server.tls {
            if !tls.cert.exists() {
                return Err(ConfigError::Validation(format!(
                    "TLS certificate file not found: {:?}",
                    tls.cert
                )));
            }
            if !tls.key.exists() {
                return Err(ConfigError::Validation(format!(
                    "TLS key file not found: {:?}",
                    tls.key
                )));
            }
            if !self.server.allow_plaintext {
                return Err(ConfigError::Validation(
                    "TLS is configured but not yet implemented. Remove [server.tls] or set allow_plaintext = true to start without TLS.".to_string(),
                ));
            }
        }

        const PG_PACKET_LIMIT: usize = 0x3fffffff - 1;
        if let Some(limit) = self.server.copy_stdin_memory_limit {
            if limit == 0 {
                return Err(ConfigError::Validation(
                    "server.copy_stdin_memory_limit must be greater than 0".to_string(),
                ));
            }
            if limit > PG_PACKET_LIMIT {
                return Err(ConfigError::Validation(format!(
                    "server.copy_stdin_memory_limit must be <= {} bytes",
                    PG_PACKET_LIMIT
                )));
            }
        }

        Ok(())
    }

    fn validate_cluster(&self) -> Result<(), ConfigError> {
        // Minimum memory: 64MB
        const MIN_MEMORY: usize = 64 * 1024 * 1024;
        if self.cluster.max_memory < MIN_MEMORY {
            return Err(ConfigError::Validation(format!(
                "cluster.max_memory must be at least 64MB, got {} bytes",
                self.cluster.max_memory
            )));
        }

        // Thread count must be reasonable if specified
        if let Some(threads) = self.cluster.num_threads {
            if threads == 0 {
                return Err(ConfigError::Validation(
                    "cluster.num_threads cannot be 0".to_string(),
                ));
            }
            // Warn if using more than 256 threads (likely a mistake)
            if threads > 256 {
                tracing::warn!(
                    "cluster.num_threads is set to {}, which is unusually high",
                    threads
                );
            }
        }

        // Default database name must not be empty
        if self.cluster.default_database.is_empty() {
            return Err(ConfigError::Validation(
                "cluster.default_database cannot be empty".to_string(),
            ));
        }

        Ok(())
    }

    fn validate_storage(&self) -> Result<(), ConfigError> {
        // Data directory must not be empty
        if self.storage.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::Validation(
                "storage.data_dir cannot be empty".to_string(),
            ));
        }

        // Buffer pool size must be reasonable
        const MIN_BUFFER_POOL: usize = 16 * 1024 * 1024; // 16MB
        if self.storage.buffer_pool.size < MIN_BUFFER_POOL {
            return Err(ConfigError::Validation(format!(
                "storage.buffer_pool.size must be at least 16MB, got {} bytes",
                self.storage.buffer_pool.size
            )));
        }

        // Checkpoint threshold must be reasonable
        const MIN_CHECKPOINT: usize = 1024 * 1024; // 1MB
        if self.storage.wal.checkpoint_threshold < MIN_CHECKPOINT {
            return Err(ConfigError::Validation(format!(
                "storage.wal.checkpoint_threshold must be at least 1MB, got {} bytes",
                self.storage.wal.checkpoint_threshold
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_config() {
        let config = ParoConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_port() {
        let mut config = ParoConfig::default();
        config.server.port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_memory() {
        let mut config = ParoConfig::default();
        config.cluster.max_memory = 1024; // Too small
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_empty_database() {
        let mut config = ParoConfig::default();
        config.cluster.default_database = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_buffer_pool() {
        let mut config = ParoConfig::default();
        config.storage.buffer_pool.size = 1024; // Too small
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_tls_requires_explicit_plaintext_override() {
        let dir = std::env::temp_dir().join(format!(
            "paro-config-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let cert = dir.join("server.crt");
        let key = dir.join("server.key");
        let mut config = ParoConfig::default();
        config.server.tls = Some(super::super::TlsConfig {
            cert: cert.clone(),
            key: key.clone(),
        });
        std::fs::write(&cert, b"crt").unwrap();
        std::fs::write(&key, b"key").unwrap();
        let err = config
            .validate()
            .expect_err("tls without allow_plaintext should fail");
        assert!(err
            .to_string()
            .contains("TLS is configured but not yet implemented"));
        let _ = std::fs::remove_file(cert);
        let _ = std::fs::remove_file(key);
        let _ = std::fs::remove_dir(dir);
    }

    #[test]
    fn test_invalid_copy_stdin_memory_limit() {
        let mut config = ParoConfig::default();
        config.server.copy_stdin_memory_limit = Some(0);
        assert!(config.validate().is_err());

        config.server.copy_stdin_memory_limit = Some(0x3fffffff);
        assert!(config.validate().is_err());
    }
}

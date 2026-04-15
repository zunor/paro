//! Build/version metadata shared across crates.

pub const PG_COMPAT_SERVER_VERSION_NUM: &str = "150000";

/// Returns the Paro build version derived from Cargo package metadata.
pub fn paro_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Returns the PostgreSQL-compatible server_version string advertised at startup.
pub fn pg_compat_server_version() -> String {
    format!("15.0 (Paro {})", paro_version())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paro_version_is_non_empty() {
        assert!(!paro_version().is_empty());
    }

    #[test]
    fn pg_compat_server_version_keeps_pg_prefix() {
        let version = pg_compat_server_version();
        assert!(version.starts_with("15.0 "));
        assert!(version.contains(paro_version()));
    }
}

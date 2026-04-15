//! File Opener
//!
//! Abstract interface for providing client-specific context to FileSystem.
//!

use paro_common::runtime_value::Value;

/// Information about the file being opened.
#[derive(Debug, Clone, Default)]
pub struct FileOpenerInfo {
    /// Path to the file being opened
    pub file_path: String,
}

/// Result of a setting lookup operation.
#[derive(Debug, Clone)]
pub enum SettingLookupResult {
    /// Setting was found
    Found(Value),
    /// Setting was not found
    NotFound,
    /// Setting lookup is not supported
    NotSupported,
}

impl SettingLookupResult {
    /// Returns true if the setting was found.
    pub fn is_found(&self) -> bool {
        matches!(self, Self::Found(_))
    }

    /// Returns the value if found, None otherwise.
    pub fn value(self) -> Option<Value> {
        match self {
            Self::Found(v) => Some(v),
            _ => None,
        }
    }
}

/// Abstract interface for providing client-specific context to FileSystem.
///
/// This trait allows the file system to access session-specific settings
/// and context when opening files.
pub trait FileOpener: Send + Sync + std::fmt::Debug {
    /// Tries to get a current setting value.
    fn try_get_current_setting(&self, key: &str) -> SettingLookupResult;

    /// Tries to get a current setting value with file info context.
    fn try_get_current_setting_with_info(
        &self,
        key: &str,
        _info: &FileOpenerInfo,
    ) -> SettingLookupResult {
        // Default implementation ignores file info
        self.try_get_current_setting(key)
    }
}

/// Test helper that returns `NotSupported` for every setting lookup.
#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct DefaultFileOpener;

#[cfg(test)]
impl FileOpener for DefaultFileOpener {
    fn try_get_current_setting(&self, _key: &str) -> SettingLookupResult {
        SettingLookupResult::NotSupported
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_file_opener() {
        let opener = DefaultFileOpener;
        let result = opener.try_get_current_setting("any_key");
        assert!(matches!(result, SettingLookupResult::NotSupported));
    }

    #[test]
    fn test_setting_lookup_result() {
        let found = SettingLookupResult::Found(Value::Integer(42));
        assert!(found.is_found());
        assert!(matches!(found.value(), Some(Value::Integer(42))));

        let not_found = SettingLookupResult::NotFound;
        assert!(!not_found.is_found());
        assert!(not_found.value().is_none());
    }
}

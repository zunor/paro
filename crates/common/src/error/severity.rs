//! Error severity levels.

/// Error severity level.
///
/// Error severity level representing different tiers of messages and error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    /// Debugging messages, in categories of decreasing detail.
    Debug,
    /// Server operational messages; sent only to server log by default.
    Log,
    /// Messages specifically requested by user (e.g., VACUUM VERBOSE output).
    Info,
    /// Helpful messages to users about query operation.
    Notice,
    /// Warnings for unexpected messages.
    Warning,
    /// User error - abort transaction; return to known state.
    Error,
    /// Fatal error - abort process.
    Fatal,
    /// Take down the other backends with me.
    Panic,
}

impl Severity {
    /// Returns the wire protocol string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Log => "LOG",
            Self::Info => "INFO",
            Self::Notice => "NOTICE",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            Self::Panic => "PANIC",
        }
    }

    /// Returns true if this severity level should abort the transaction.
    pub fn aborts_transaction(&self) -> bool {
        matches!(self, Self::Error | Self::Fatal | Self::Panic)
    }

    /// Returns true if this is a fatal or panic level error.
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal | Self::Panic)
    }
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_as_str() {
        assert_eq!(Severity::Error.as_str(), "ERROR");
        assert_eq!(Severity::Warning.as_str(), "WARNING");
        assert_eq!(Severity::Fatal.as_str(), "FATAL");
    }

    #[test]
    fn test_aborts_transaction() {
        assert!(!Severity::Warning.aborts_transaction());
        assert!(Severity::Error.aborts_transaction());
        assert!(Severity::Fatal.aborts_transaction());
        assert!(Severity::Panic.aborts_transaction());
    }
}

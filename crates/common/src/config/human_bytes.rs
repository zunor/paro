//! Human-readable byte size serialization/deserialization.
//!
//! Supports formats like "2GB", "512MB", "64KB", or plain numbers.

use crate::runtime_value::Value;
use bytesize::ByteSize;
use serde::{Deserialize, Deserializer, Serializer};

/// Parse a human-readable byte string or plain number.
///
/// Accepts both SI and IEC units:
/// - `2GB`, `512MB`, `64KB` (SI)
/// - `2GiB`, `512MiB`, `64KiB` (IEC)
/// - `1024` (plain bytes)
///
/// Surrounding single/double quotes are ignored.
pub fn parse_human_bytes(input: &str) -> Result<usize, String> {
    let normalized = input.trim().trim_matches('\'').trim_matches('"').trim();
    if normalized.is_empty() {
        return Err("byte size cannot be empty".to_string());
    }

    if let Ok(raw) = normalized.parse::<u64>() {
        return usize::try_from(raw)
            .map_err(|_| format!("Byte size '{}' exceeds platform usize", normalized));
    }

    let parsed = normalized
        .parse::<ByteSize>()
        .map_err(|e| format!("Invalid byte size '{}': {}", input, e))?;
    usize::try_from(parsed.as_u64())
        .map_err(|_| format!("Byte size '{}' exceeds platform usize", normalized))
}

/// Format a byte count using PostgreSQL-style decimal units when it is an exact multiple.
pub fn format_human_bytes(bytes: u64) -> String {
    const UNITS: &[(u64, &str)] = &[
        (1_000_000_000_000, "TB"),
        (1_000_000_000, "GB"),
        (1_000_000, "MB"),
        (1_000, "KB"),
    ];

    for (unit, suffix) in UNITS {
        if bytes >= *unit && bytes.is_multiple_of(*unit) {
            return format!("{}{suffix}", bytes / *unit);
        }
    }

    bytes.to_string()
}

/// Render a runtime setting value using the display conventions expected by SQL front-end APIs.
pub fn format_setting_value(name: &str, value: &Value) -> String {
    match value {
        Value::Varchar(v) => v.clone(),
        Value::Integer(v) if matches!(name, "memory_limit" | "max_temp_directory_size") => {
            format_human_bytes(*v as u64)
        }
        Value::BigInt(v) if matches!(name, "memory_limit" | "max_temp_directory_size") => {
            format_human_bytes(*v as u64)
        }
        _ => value.to_string(),
    }
}

/// Serialize a usize as a human-readable byte string.
pub fn serialize<S>(bytes: &usize, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&ByteSize(*bytes as u64).to_string_as(true))
}

/// Deserialize a human-readable byte string or plain number to usize.
pub fn deserialize<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BytesOrString {
        Bytes(u64),
        String(String),
    }

    match BytesOrString::deserialize(deserializer)? {
        BytesOrString::Bytes(n) => Ok(n as usize),
        BytesOrString::String(s) => {
            // Try to parse as human-readable format
            s.parse::<ByteSize>()
                .map(|bs| bs.as_u64() as usize)
                .map_err(|e| D::Error::custom(format!("Invalid byte size '{}': {}", s, e)))
        }
    }
}

/// Serde helpers for optional byte-size fields.
pub mod optional {
    use super::parse_human_bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Option<usize>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bytes {
            Some(bytes) => {
                serializer.serialize_some(&bytesize::ByteSize(*bytes as u64).to_string_as(true))
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum OptionalBytesOrString {
            None(Option<()>),
            Bytes(u64),
            String(String),
        }

        match OptionalBytesOrString::deserialize(deserializer)? {
            OptionalBytesOrString::None(None) => Ok(None),
            OptionalBytesOrString::Bytes(n) => Ok(Some(n as usize)),
            OptionalBytesOrString::String(s) => {
                parse_human_bytes(&s).map(Some).map_err(D::Error::custom)
            }
            OptionalBytesOrString::None(Some(())) => {
                unreachable!("unit variant is only used for None")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime_value::Value;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestStruct {
        #[serde(with = "super")]
        size: usize,
    }

    #[test]
    fn test_deserialize_human_readable() {
        // Note: bytesize uses SI units (GB = 10^9) and binary units (GiB = 2^30)
        let cases = [
            (r#"{"size": "1GiB"}"#, 1024 * 1024 * 1024), // Binary: 1 GiB
            (r#"{"size": "512MiB"}"#, 512 * 1024 * 1024), // Binary: 512 MiB
            (r#"{"size": "64KiB"}"#, 64 * 1024),         // Binary: 64 KiB
            (r#"{"size": "1GB"}"#, 1_000_000_000),       // SI: 1 GB
            (r#"{"size": "1024"}"#, 1024),               // Plain number
            (r#"{"size": 2048}"#, 2048),                 // JSON number
        ];

        for (json, expected) in cases {
            let result: TestStruct = serde_json::from_str(json).unwrap();
            assert_eq!(result.size, expected, "Failed for input: {}", json);
        }
    }

    #[test]
    fn test_deserialize_toml() {
        // Use binary units for database config (more intuitive for memory sizes)
        let toml_str = r#"size = "2GiB""#;
        let result: TestStruct = toml::from_str(toml_str).unwrap();
        assert_eq!(result.size, 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_serialize() {
        let test = TestStruct {
            size: 1024 * 1024 * 1024,
        };
        let json = serde_json::to_string(&test).unwrap();
        assert!(json.contains("1.0 GiB") || json.contains("1 GiB"));
    }

    #[test]
    fn test_parse_human_bytes() {
        assert_eq!(super::parse_human_bytes("1024").unwrap(), 1024);
        assert_eq!(
            super::parse_human_bytes("1GiB").unwrap(),
            1024 * 1024 * 1024
        );
        assert_eq!(super::parse_human_bytes("2GB").unwrap(), 2_000_000_000);
        assert_eq!(
            super::parse_human_bytes("'512MiB'").unwrap(),
            512 * 1024 * 1024
        );
        assert!(super::parse_human_bytes("").is_err());
        assert!(super::parse_human_bytes("not-a-size").is_err());
    }

    #[test]
    fn test_optional_deserialize_human_readable() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestOptionalStruct {
            #[serde(default, with = "super::optional")]
            size: Option<usize>,
        }

        let result: TestOptionalStruct = serde_json::from_str(r#"{"size":"256MiB"}"#).unwrap();
        assert_eq!(result.size, Some(256 * 1024 * 1024));

        let result: TestOptionalStruct = serde_json::from_str(r#"{"size":null}"#).unwrap();
        assert_eq!(result.size, None);
    }

    #[test]
    fn test_format_human_bytes() {
        assert_eq!(super::format_human_bytes(1_000_000_000), "1GB");
        assert_eq!(super::format_human_bytes(512_000_000), "512MB");
        assert_eq!(super::format_human_bytes(1024), "1024");
    }

    #[test]
    fn test_format_setting_value_uses_human_units_for_byte_settings() {
        assert_eq!(
            super::format_setting_value("memory_limit", &Value::BigInt(1_000_000_000)),
            "1GB"
        );
        assert_eq!(
            super::format_setting_value("max_temp_directory_size", &Value::BigInt(512_000_000),),
            "512MB"
        );
        assert_eq!(
            super::format_setting_value("application_name", &Value::Varchar("paro".to_string())),
            "paro"
        );
    }
}

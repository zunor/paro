use paro_common::config::format_setting_value;
use paro_common::runtime_value::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct EffectiveSettings {
    raw: HashMap<String, Value>,
}

impl EffectiveSettings {
    pub fn new(raw: HashMap<String, Value>) -> Self {
        Self { raw }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.raw.get(&key.to_ascii_lowercase())
    }

    pub fn raw(&self) -> &HashMap<String, Value> {
        &self.raw
    }

    pub fn render(&self, key: &str) -> Option<String> {
        self.get(key)
            .map(|value| format_setting_value(&key.to_ascii_lowercase(), value))
    }

    pub fn optimizer_verify(&self) -> bool {
        matches!(self.get("optimizer_verify"), Some(Value::Boolean(true)))
    }

    pub fn force_external(&self) -> bool {
        matches!(self.get("force_external"), Some(Value::Boolean(true)))
    }

    pub fn threads(&self) -> Option<usize> {
        value_to_usize(self.get("threads"))
    }

    pub fn memory_limit(&self) -> Option<usize> {
        value_to_usize(self.get("memory_limit"))
    }

    pub fn temp_directory(&self) -> Option<String> {
        match self.get("temp_directory") {
            Some(Value::Varchar(value)) => Some(value.clone()),
            _ => None,
        }
    }

    pub fn max_temp_directory_size(&self) -> Option<Option<usize>> {
        match self.get("max_temp_directory_size") {
            Some(Value::Varchar(value)) if value.eq_ignore_ascii_case("unlimited") => Some(None),
            Some(value) => value_to_usize(Some(value)).map(Some),
            None => None,
        }
    }

    pub fn statement_timeout(&self) -> Option<Duration> {
        value_to_usize(self.get("statement_timeout"))
            .map(|millis| Duration::from_millis(millis as u64))
    }

    pub fn fingerprint(&self) -> u64 {
        let mut keys = self.raw.keys().cloned().collect::<Vec<_>>();
        keys.sort();

        let mut hasher = DefaultHasher::new();
        for key in keys {
            key.hash(&mut hasher);
            self.render(&key)
                .unwrap_or_else(|| self.raw[&key].to_string())
                .hash(&mut hasher);
        }
        hasher.finish()
    }
}

fn value_to_usize(value: Option<&Value>) -> Option<usize> {
    match value {
        Some(Value::Integer(value)) if *value > 0 => Some(*value as usize),
        Some(Value::BigInt(value)) if *value > 0 => Some(*value as usize),
        Some(Value::Varchar(value)) => value.parse::<usize>().ok().filter(|value| *value > 0),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_order_insensitive_but_value_sensitive() {
        let mut first = HashMap::new();
        first.insert("threads".to_string(), Value::Integer(4));
        first.insert("memory_limit".to_string(), Value::BigInt(1024));

        let mut second = HashMap::new();
        second.insert("memory_limit".to_string(), Value::BigInt(1024));
        second.insert("threads".to_string(), Value::Integer(4));

        let mut third = second.clone();
        third.insert("threads".to_string(), Value::Integer(8));

        assert_eq!(
            EffectiveSettings::new(first).fingerprint(),
            EffectiveSettings::new(second).fingerprint()
        );
        assert_ne!(
            EffectiveSettings::new(third).fingerprint(),
            EffectiveSettings::new(HashMap::from([
                ("memory_limit".to_string(), Value::BigInt(1024)),
                ("threads".to_string(), Value::Integer(4)),
            ]))
            .fingerprint()
        );
    }
}

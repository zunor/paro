// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::{ExpressionState, FunctionStability, ScalarFunction, ScalarFunctionSet};

fn current_setting_impl(
    input: &Chunk,
    state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let name_vec = input
        .column(0)
        .ok_or_else(|| paro_error::internal("Missing setting name argument"))?;

    result.set_count(count);
    for row in 0..count {
        if name_vec.is_null(row) {
            result.validity_mut().set_null(row);
            continue;
        }

        let Some(setting_name) = name_vec.get_string(row) else {
            result.validity_mut().set_null(row);
            continue;
        };

        if let Some(setting_value) = state.current_setting(setting_name) {
            result.set_string(row, &setting_value);
        } else {
            result.validity_mut().set_null(row);
        }
    }

    Ok(())
}

pub fn get_current_setting_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("current_setting".to_string());
    set.add_function(
        ScalarFunction::new(
            "current_setting".to_string(),
            vec![LogicalType::Varchar],
            LogicalType::Varchar,
            current_setting_impl,
        )
        .with_stability(FunctionStability::ConsistentWithinQuery),
    );
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::collections::HashMap;

    struct MockState {
        settings: HashMap<String, String>,
    }

    impl MockState {
        fn new(settings: &[(&str, &str)]) -> Self {
            let mut map = HashMap::new();
            for (key, value) in settings {
                map.insert((*key).to_string(), (*value).to_string());
            }
            Self { settings: map }
        }
    }

    impl ExpressionState for MockState {
        fn current_database(&self) -> Option<&str> {
            None
        }

        fn current_schema(&self) -> Option<&str> {
            None
        }

        fn current_user(&self) -> Option<&str> {
            None
        }

        fn current_setting(&self, key: &str) -> Option<String> {
            self.settings.get(key).cloned()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_current_setting_basic() {
        let input = Chunk::from_vectors(vec![Vector::from_strings(&["memory_limit"])]);
        let mut result = Vector::new(LogicalType::Varchar);
        let state = MockState::new(&[("memory_limit", "2097152")]);

        current_setting_impl(&input, &state, &mut result).unwrap();
        assert_eq!(result.get_string(0), Some("2097152"));
    }

    #[test]
    fn test_current_setting_unknown_returns_null() {
        let input = Chunk::from_vectors(vec![Vector::from_strings(&["unknown_setting"])]);
        let mut result = Vector::new(LogicalType::Varchar);
        let state = MockState::new(&[]);

        current_setting_impl(&input, &state, &mut result).unwrap();
        assert!(result.is_null(0));
    }

    #[test]
    fn test_current_setting_function_set() {
        let set = get_current_setting_functions();
        assert_eq!(set.name, "current_setting");
        assert_eq!(set.functions.len(), 1);
    }
}

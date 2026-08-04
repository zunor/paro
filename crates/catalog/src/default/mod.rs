// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Lazy generators for built-in catalog entries.

pub mod default_functions;
pub mod default_schemas;
pub mod default_views;

pub(crate) use default_schemas::DefaultSchemaGenerator;

use crate::entry::CatalogEntryEnum;
use std::sync::Arc;

pub trait DefaultGenerator: Send + Sync {
    fn is_default_entry(&self, name: &str) -> bool;

    fn create_default_entry(&self, name: &str) -> Option<Arc<CatalogEntryEnum>>;

    fn get_default_entries(&self) -> Vec<String>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{CatalogObjectIdAllocator, SchemaEntry};

    struct TestGenerator {
        entries: Vec<String>,
    }

    impl TestGenerator {
        fn new(entries: Vec<String>) -> Self {
            Self { entries }
        }
    }

    impl DefaultGenerator for TestGenerator {
        fn is_default_entry(&self, name: &str) -> bool {
            let lower = name.to_lowercase();
            self.entries.iter().any(|e| e.to_lowercase() == lower)
        }

        fn create_default_entry(&self, name: &str) -> Option<Arc<CatalogEntryEnum>> {
            if !self.is_default_entry(name) {
                return None;
            }

            let schema = SchemaEntry::new(
                "test_catalog".to_string(),
                name.to_lowercase(),
                Arc::new(CatalogObjectIdAllocator::default()),
                Arc::new(std::sync::atomic::AtomicU64::new(0)),
                0, // timestamp = 0 (committed)
            );

            Some(Arc::new(CatalogEntryEnum::Schema(Arc::new(schema))))
        }

        fn get_default_entries(&self) -> Vec<String> {
            self.entries.clone()
        }
    }

    #[test]
    fn test_default_entry_lookup_is_case_insensitive() {
        let gen = TestGenerator::new(vec![
            "pg_catalog".to_string(),
            "information_schema".to_string(),
        ]);

        assert!(gen.is_default_entry("pg_catalog"));
        assert!(gen.is_default_entry("PG_CATALOG"));
        assert!(gen.is_default_entry("information_schema"));
        assert!(!gen.is_default_entry("public"));
        assert!(!gen.is_default_entry("unknown"));
    }

    #[test]
    fn test_create_default_entry() {
        let gen = TestGenerator::new(vec!["pg_catalog".to_string()]);

        let entry = gen.create_default_entry("pg_catalog");
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.name(), "pg_catalog");
        assert_eq!(entry.timestamp(), 0);

        assert!(gen.create_default_entry("PG_CATALOG").is_some());
        assert!(gen.create_default_entry("unknown").is_none());
    }

    #[test]
    fn test_get_default_entries() {
        let gen = TestGenerator::new(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let entries = gen.get_default_entries();
        assert_eq!(entries.len(), 3);
        assert!(entries.contains(&"a".to_string()));
        assert!(entries.contains(&"b".to_string()));
        assert!(entries.contains(&"c".to_string()));
    }

    #[test]
    fn test_trait_object_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Box<dyn DefaultGenerator>>();
    }
}

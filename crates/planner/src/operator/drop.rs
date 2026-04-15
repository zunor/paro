//! Logical `DROP` for tables, schemas, indexes, views, etc. Cascade/restrict behavior is incomplete in execution.

use crate::binder::ir::statement::{BoundDropInfo, DropType};

/// Logical `DROP` for catalog objects described by [`BoundDropInfo`].
#[derive(Debug, Clone)]
pub struct Drop {
    /// The bound drop information containing all details.
    pub info: BoundDropInfo,
}

impl Drop {
    /// Create a new Drop operator.
    pub fn new(info: BoundDropInfo) -> Self {
        Self { info }
    }

    /// Get the type of object being dropped.
    pub fn drop_type(&self) -> DropType {
        self.info.drop_type
    }

    /// Get the full qualified name of the object.
    pub fn full_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.info.database_name, self.info.schema_name, self.info.object_name
        )
    }

    /// Get the object name.
    pub fn object_name(&self) -> &str {
        &self.info.object_name
    }

    /// Get the schema name.
    pub fn schema_name(&self) -> &str {
        &self.info.schema_name
    }

    /// Check if IF EXISTS was specified.
    pub fn if_exists(&self) -> bool {
        self.info.if_exists
    }

    /// Check if CASCADE was specified.
    pub fn cascade(&self) -> bool {
        self.info.cascade
    }

    /// Get the operator name for display.
    pub fn name(&self) -> &'static str {
        match self.info.drop_type {
            DropType::Table => "DROP_TABLE",
            DropType::Schema => "DROP_SCHEMA",
            DropType::Index => "DROP_INDEX",
            DropType::View => "DROP_VIEW",
            DropType::Sequence => "DROP_SEQUENCE",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_drop_info(drop_type: DropType, name: &str) -> BoundDropInfo {
        BoundDropInfo {
            drop_type,
            database_name: "test".to_string(),
            schema_name: "main".to_string(),
            object_name: name.to_string(),
            if_exists: false,
            cascade: false,
        }
    }

    #[test]
    fn test_logical_drop_table() {
        let info = create_test_drop_info(DropType::Table, "users");
        let op = Drop::new(info);

        assert_eq!(op.drop_type(), DropType::Table);
        assert_eq!(op.object_name(), "users");
        assert_eq!(op.schema_name(), "main");
        assert_eq!(op.name(), "DROP_TABLE");
        assert_eq!(op.full_name(), "test.main.users");
    }

    #[test]
    fn test_logical_drop_schema() {
        let info = create_test_drop_info(DropType::Schema, "my_schema");
        let op = Drop::new(info);

        assert_eq!(op.drop_type(), DropType::Schema);
        assert_eq!(op.object_name(), "my_schema");
        assert_eq!(op.name(), "DROP_SCHEMA");
    }

    #[test]
    fn test_logical_drop_if_exists() {
        let mut info = create_test_drop_info(DropType::Table, "maybe_exists");
        info.if_exists = true;
        let op = Drop::new(info);

        assert!(op.if_exists());
        assert!(!op.cascade());
    }

    #[test]
    fn test_logical_drop_cascade() {
        let mut info = create_test_drop_info(DropType::Table, "parent_table");
        info.cascade = true;
        let op = Drop::new(info);

        assert!(!op.if_exists());
        assert!(op.cascade());
    }

    #[test]
    fn test_logical_drop_index() {
        let info = create_test_drop_info(DropType::Index, "idx_users_name");
        let op = Drop::new(info);

        assert_eq!(op.drop_type(), DropType::Index);
        assert_eq!(op.name(), "DROP_INDEX");
    }

    #[test]
    fn test_logical_drop_view() {
        let info = create_test_drop_info(DropType::View, "user_view");
        let op = Drop::new(info);

        assert_eq!(op.drop_type(), DropType::View);
        assert_eq!(op.name(), "DROP_VIEW");
    }
}

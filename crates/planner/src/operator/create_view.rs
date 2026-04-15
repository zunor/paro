//! Logical operator for `CREATE VIEW`.

use crate::binder::ir::statement::BoundCreateViewInfo;

/// CreateView represents a CREATE VIEW operation.
///
/// This operator wraps the bound view information and will be
/// converted to CreateView during physical plan generation.
///
#[derive(Debug, Clone)]
pub struct CreateView {
    /// The bound view creation information
    pub info: BoundCreateViewInfo,
}

impl CreateView {
    /// Create a new CreateView operator.
    pub fn new(info: BoundCreateViewInfo) -> Self {
        Self { info }
    }

    /// Get the schema name for the view.
    pub fn schema_name(&self) -> &str {
        &self.info.schema_name
    }

    /// Get the view name.
    pub fn view_name(&self) -> &str {
        &self.info.view_name
    }

    /// Get the full name (schema.view).
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.info.schema_name, self.info.view_name)
    }

    /// Check if this is an OR REPLACE operation.
    pub fn or_replace(&self) -> bool {
        self.info.or_replace
    }

    /// Check if this is an IF NOT EXISTS operation.
    pub fn if_not_exists(&self) -> bool {
        self.info.if_not_exists
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_parser::parse_one;

    fn create_test_info() -> BoundCreateViewInfo {
        // Parse a real query to get a valid AST
        let sql = "SELECT 1 AS a, 'hello' AS b";
        let query = match parse_one(sql).expect("Failed to parse SQL").stmt {
            paro_parser::ast::Statement::Query(q) => q,
            _ => panic!("Expected Query statement"),
        };

        BoundCreateViewInfo {
            schema_name: "public".to_string(),
            view_name: "test_view".to_string(),
            query,
            column_types: vec![LogicalType::Integer, LogicalType::Varchar],
            column_names: vec!["a".to_string(), "b".to_string()],
            aliases: vec![],
            or_replace: false,
            if_not_exists: false,
            temporary: false,
            sql: Some("CREATE VIEW test_view AS SELECT 1 AS a, 'hello' AS b".to_string()),
            dependencies: paro_catalog::entry::DependencyList::new(),
        }
    }

    #[test]
    fn test_logical_create_view_new() {
        let info = create_test_info();
        let op = CreateView::new(info);

        assert_eq!(op.schema_name(), "public");
        assert_eq!(op.view_name(), "test_view");
        assert_eq!(op.full_name(), "public.test_view");
    }

    #[test]
    fn test_logical_create_view_or_replace() {
        let mut info = create_test_info();
        info.or_replace = true;
        let op = CreateView::new(info);

        assert!(op.or_replace());
        assert!(!op.if_not_exists());
    }

    #[test]
    fn test_logical_create_view_if_not_exists() {
        let mut info = create_test_info();
        info.if_not_exists = true;
        let op = CreateView::new(info);

        assert!(!op.or_replace());
        assert!(op.if_not_exists());
    }
}

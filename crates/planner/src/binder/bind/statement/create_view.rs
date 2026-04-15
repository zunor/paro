//! Binds `CREATE VIEW`. `TEMPORARY` views are not fully supported yet.

use crate::binder::Binder;
use paro_catalog::dependency::DependencyExtractor;
use paro_catalog::entry::CreateViewInfo;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{CreateOption, CreateViewStmt};

/// Bound information for CREATE VIEW statement
#[derive(Debug, Clone)]
pub struct BoundCreateViewInfo {
    /// Schema name
    pub schema_name: String,
    /// View name
    pub view_name: String,
    /// The SELECT query defining the view (original AST)
    pub query: Box<paro_parser::ast::Query>,
    /// Column types (derived from binding the query)
    pub column_types: Vec<LogicalType>,
    /// Column names (derived from binding the query)
    pub column_names: Vec<String>,
    /// Column aliases (user-specified, overrides query column names)
    pub aliases: Vec<String>,
    /// OR REPLACE flag
    pub or_replace: bool,
    /// IF NOT EXISTS flag
    pub if_not_exists: bool,
    /// Whether this is a temporary view
    pub temporary: bool,
    /// Original SQL statement (for SHOW CREATE VIEW)
    pub sql: Option<String>,
    /// Direct dependencies extracted from the bound query.
    pub dependencies: paro_catalog::entry::DependencyList,
}

impl BoundCreateViewInfo {
    /// Convert to CreateViewInfo for catalog storage
    pub fn to_create_view_info(self) -> CreateViewInfo {
        let dependencies = self.dependencies.clone();
        let BoundCreateViewInfo {
            schema_name,
            view_name,
            query,
            column_types,
            column_names,
            aliases,
            or_replace,
            if_not_exists,
            temporary,
            sql,
            dependencies: _,
        } = self;

        let mut info = CreateViewInfo::new(schema_name, view_name, query)
            .with_aliases(aliases)
            .with_column_types(column_types)
            .with_column_names(column_names)
            .with_dependencies(dependencies);

        if or_replace {
            info = info.with_or_replace();
        }
        if if_not_exists {
            info = info.with_if_not_exists();
        }
        if temporary {
            info = info.with_temporary();
        }
        if let Some(sql) = sql {
            info = info.with_sql(sql);
        }

        info
    }
}

impl DependencyExtractor for BoundCreateViewInfo {
    fn extract_dependencies(&self) -> paro_catalog::entry::DependencyList {
        self.dependencies.clone()
    }
}

/// Bind a CREATE VIEW statement.
///
/// This function:
/// 1. Resolves the schema and view name
/// 2. Creates a child binder to bind the view's SELECT query
/// 3. Extracts column types and names from the bound query
/// 4. Validates that aliases count matches column count (if provided)
/// 5. Returns BoundCreateViewInfo
///
pub fn bind_create_view(binder: &mut Binder, stmt: CreateViewStmt) -> Result<BoundCreateViewInfo> {
    // 1. Build the original SQL for storage first (before moving fields)
    let sql = Some(stmt.to_string());

    // 2. Resolve schema name
    let schema_name = stmt
        .schema
        .map(|s| s.name)
        .unwrap_or_else(|| binder.session_context().current_schema().to_string());

    let view_name = stmt.view.name;

    // 3. Parse create options
    let (or_replace, if_not_exists) = match stmt.create_option {
        CreateOption::Create => (false, false),
        CreateOption::CreateOrReplace => (true, false),
        CreateOption::CreateIfNotExists => (false, true),
    };

    // 4. Extract column aliases from the statement
    let aliases: Vec<String> = stmt.columns.iter().map(|c| c.name.clone()).collect();

    // 5. Create a child binder to bind the view's SELECT query
    // This validates the query and extracts column types/names
    let mut view_binder = binder.create_child();
    view_binder.enable_dependency_collection();

    // Make a copy of the query for binding (we keep the original for storage)
    let query_copy = stmt.query.clone();

    // Bind the query to validate it and extract types/names
    let bound_query = view_binder.bind_query(*query_copy)?;

    // 6. Extract column types and names from the bound query
    let column_types = bound_query.types();
    let column_names = bound_query.names();

    // 7. Validate aliases count
    if !aliases.is_empty() && aliases.len() != column_names.len() {
        return Err(paro_error::syntax(format!(
            "CREATE VIEW specifies {} column names, but query returns {} columns",
            aliases.len(),
            column_names.len()
        )));
    }

    let dependencies = view_binder.collected_dependencies();

    Ok(BoundCreateViewInfo {
        schema_name,
        view_name,
        query: stmt.query,
        column_types,
        column_names,
        aliases,
        or_replace,
        if_not_exists,
        temporary: false, // TODO: support temporary views
        sql,
        dependencies,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::test_binder;
    use paro_catalog::catalog::Catalog;
    use paro_catalog::collection::InstallMode;
    use paro_catalog::entry::{
        CatalogEntryEnum, CatalogObjectId, CatalogObjectRef, CatalogType, ColumnDefinition,
        DependencyList, TableCatalogEntry, ViewCatalogEntry,
    };
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_common::types::LogicalType;
    use paro_parser::ast::Statement;
    use paro_parser::parse_one;
    use paro_storage::table::table_factory::TableFactory;
    use std::sync::Arc;

    fn parse_query(sql: &str) -> Box<paro_parser::ast::Query> {
        match parse_one(sql).expect("query should parse").stmt {
            Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        }
    }

    fn parse_create_view(sql: &str) -> CreateViewStmt {
        match parse_one(sql).expect("statement should parse").stmt {
            Statement::CreateView(stmt) => stmt,
            _ => panic!("expected CREATE VIEW statement"),
        }
    }

    fn install_base_table_and_view(binder: &mut Binder) {
        let catalog = binder.catalog();
        catalog.initialize(false);
        let txn = CatalogSnapshot::read_only(u64::MAX);
        let schema = catalog.get_schema(&txn, "public").unwrap();
        let schema_id = schema.base.object_id;

        let storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("table storage"),
        );
        let table = Arc::new(CatalogEntryEnum::Table(Arc::new(
            TableCatalogEntry::with_object_id(
                catalog.name().to_string(),
                "public".to_string(),
                "base_table".to_string(),
                vec![ColumnDefinition::new(
                    "id".to_string(),
                    LogicalType::Integer,
                )],
                storage,
                CatalogObjectId::from_raw(101),
                0,
            ),
        )));
        schema
            .collection(CatalogType::Table)
            .expect("table collection")
            .install_committed(table, InstallMode::RejectExisting)
            .expect("base table install");

        let mut dependencies = DependencyList::new();
        dependencies.add_regular(CatalogObjectRef::in_schema(
            CatalogObjectId::from_raw(101),
            CatalogType::Table,
            catalog.name().to_string(),
            Some(schema_id),
            "public".to_string(),
            "base_table".to_string(),
        ));
        let view = Arc::new(CatalogEntryEnum::View(Arc::new(
            ViewCatalogEntry::with_object_id(
                CreateViewInfo::new(
                    "public".to_string(),
                    "base_view".to_string(),
                    parse_query("SELECT id FROM public.base_table"),
                )
                .with_catalog(catalog.name().to_string())
                .with_column_types(vec![LogicalType::Integer])
                .with_column_names(vec!["id".to_string()])
                .with_sql(
                    "CREATE VIEW public.base_view AS SELECT id FROM public.base_table".to_string(),
                )
                .with_dependencies(dependencies),
                0,
                catalog.name().to_string(),
                CatalogObjectId::from_raw(201),
            ),
        )));
        schema
            .collection(CatalogType::View)
            .expect("view collection")
            .install_committed(view, InstallMode::RejectExisting)
            .expect("base view install");
        catalog.rebuild_dependency_graph().unwrap();
    }

    #[test]
    fn bind_create_view_extracts_direct_view_dependency_only() {
        let mut binder = test_binder();
        install_base_table_and_view(&mut binder);

        let bound = bind_create_view(
            &mut binder,
            parse_create_view("CREATE VIEW public.derived_view AS SELECT id FROM public.base_view"),
        )
        .expect("bind create view");

        assert_eq!(bound.dependencies.len(), 1);
        let dependency = &bound.dependencies.dependencies()[0];
        assert_eq!(dependency.entry.id.raw(), 201);
        assert_eq!(dependency.entry.kind, CatalogType::View);
        assert_eq!(dependency.entry.name, "base_view");
    }
}

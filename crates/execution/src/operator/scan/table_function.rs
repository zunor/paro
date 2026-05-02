// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Table Function Operator
//!
//!
//! ## Dependencies Check
//! - TableFunction: ✅ `paro_function::table`
//! - Chunk: ✅ `paro_common::chunk`
//!
//! - PhysicalTableFunction is a Source operator that executes table functions
//! - Table functions generate rows dynamically (e.g., generate_series, range)
//!   - `GlobalTableFunctionState`: Shared across threads, initialized once
//!   - `LocalTableFunctionState`: Thread-local, initialized per thread
//! - Progress reporting via `table_scan_progress` callback

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::table::{
    GlobalTableFunctionState, LocalTableFunctionState, TableFunction, TableFunctionBindData,
    TableFunctionInitInput, TableFunctionInput, TableFunctionResult,
};

use crate::execution_context::ExecutionContext;
use crate::operator::state::ProgressData;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::{OrderPreservationType, PhysicalOperator};
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_storage::index::graph::GraphStatsProvider;
use paro_storage::search::SearchIndexKind;
use serde_json::json;

/// Bind data for table function execution.
pub struct TableFunctionBindDataWrapper {
    /// The table function definition.
    pub function: Arc<TableFunction>,
    /// Bind data returned from the bind phase.
    pub bind_data: Option<Box<dyn TableFunctionBindData>>,
    /// Input values (constants passed to the function).
    pub input_values: Vec<Value>,
    /// Column IDs to scan (for projection pushdown).
    pub column_ids: Vec<usize>,
    /// Output column types.
    pub output_types: Vec<LogicalType>,
    /// Output column names.
    pub output_names: Vec<String>,
    /// Input table types (for table-in-out functions).
    pub input_table_types: Vec<LogicalType>,
    /// Input table column names (for table-in-out functions).
    pub input_table_names: Vec<String>,
    /// When true, adds an `ordinality` column numbering rows from 1.
    pub with_ordinality: bool,
}

impl fmt::Debug for TableFunctionBindDataWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunctionBindDataWrapper")
            .field("function", &self.function.name)
            .field("has_bind_data", &self.bind_data.is_some())
            .field("input_values", &self.input_values)
            .field("column_ids", &self.column_ids)
            .field("output_types", &self.output_types)
            .field("output_names", &self.output_names)
            .field("input_table_types", &self.input_table_types)
            .field("input_table_names", &self.input_table_names)
            .field("is_in_out_function", &self.function.is_in_out_function())
            .field("with_ordinality", &self.with_ordinality)
            .finish()
    }
}

impl TableFunctionBindDataWrapper {
    /// Create new bind data wrapper.
    pub fn new(
        function: Arc<TableFunction>,
        bind_data: Option<Box<dyn TableFunctionBindData>>,
        input_values: Vec<Value>,
        column_ids: Vec<usize>,
        output_types: Vec<LogicalType>,
        output_names: Vec<String>,
    ) -> Self {
        Self {
            function,
            bind_data,
            input_values,
            column_ids,
            output_types,
            output_names,
            input_table_types: Vec::new(),
            input_table_names: Vec::new(),
            with_ordinality: false,
        }
    }

    /// Create new bind data wrapper with input table info (for table-in-out functions).
    pub fn with_input_table(
        function: Arc<TableFunction>,
        bind_data: Option<Box<dyn TableFunctionBindData>>,
        input_values: Vec<Value>,
        column_ids: Vec<usize>,
        output_types: Vec<LogicalType>,
        output_names: Vec<String>,
        input_table_types: Vec<LogicalType>,
        input_table_names: Vec<String>,
    ) -> Self {
        Self {
            function,
            bind_data,
            input_values,
            column_ids,
            output_types,
            output_names,
            input_table_types,
            input_table_names,
            with_ordinality: false,
        }
    }

    /// Set WITH ORDINALITY flag.
    pub fn with_ordinality_flag(mut self, with_ordinality: bool) -> Self {
        self.with_ordinality = with_ordinality;
        self
    }

    /// Get estimated cardinality from bind data.
    pub fn estimated_cardinality(&self) -> Option<usize> {
        self.bind_data.as_ref().and_then(|bd| bd.cardinality())
    }

    /// Check if this is a table-in-out function.
    pub fn is_in_out_function(&self) -> bool {
        self.function.is_in_out_function()
    }

    /// Check if WITH ORDINALITY is specified.
    pub fn has_ordinality(&self) -> bool {
        self.with_ordinality
    }
}

/// Global source state for table function.
///
/// Contains the global execution state shared across all threads.
pub struct TableFunctionGlobalState {
    /// Global state from the table function (if any).
    /// This is initialized via `init_global` and shared across threads.
    global_state: Option<Box<dyn GlobalTableFunctionState>>,
    /// Reference to bind data for function calls.
    bind_data: Arc<TableFunctionBindDataWrapper>,
    /// Maximum threads for parallel execution.
    max_threads: usize,
}

impl fmt::Debug for TableFunctionGlobalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunctionGlobalState")
            .field("function", &self.bind_data.function.name)
            .field("max_threads", &self.max_threads)
            .field("has_global_state", &self.global_state.is_some())
            .finish()
    }
}

impl TableFunctionGlobalState {
    /// Get the global state reference.
    pub fn global_state(&self) -> Option<&dyn GlobalTableFunctionState> {
        self.global_state.as_ref().map(|s| s.as_ref())
    }

    /// Get the bind data reference.
    pub fn bind_data(&self) -> &TableFunctionBindDataWrapper {
        &self.bind_data
    }
}

impl GlobalSourceState for TableFunctionGlobalState {
    fn max_threads(&self) -> usize {
        self.max_threads
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local source state for table function.
///
/// Contains thread-local execution state. Each worker thread gets its own instance.
pub struct TableFunctionLocalState {
    /// Local state from the table function (if any).
    /// This is initialized via `init_local` and is thread-local.
    local_state: Option<Box<dyn LocalTableFunctionState>>,
    /// Whether this local state has finished execution.
    finished: bool,
    /// Current ordinality counter (for WITH ORDINALITY).
    /// Starts at 1 and increments for each row.
    ordinality_counter: i64,
}

impl fmt::Debug for TableFunctionLocalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunctionLocalState")
            .field("has_local_state", &self.local_state.is_some())
            .field("finished", &self.finished)
            .finish()
    }
}

impl TableFunctionLocalState {
    /// Get the local state reference.
    pub fn local_state(&self) -> Option<&dyn LocalTableFunctionState> {
        self.local_state.as_ref().map(|s| s.as_ref())
    }

    /// Get the local state mutable reference.
    pub fn local_state_mut(&mut self) -> Option<&mut dyn LocalTableFunctionState> {
        match &mut self.local_state {
            Some(s) => Some(s.as_mut()),
            None => None,
        }
    }

    /// Check if execution is finished.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Mark execution as finished.
    pub fn set_finished(&mut self) {
        self.finished = true;
    }

    /// Get the current ordinality counter value.
    pub fn ordinality_counter(&self) -> i64 {
        self.ordinality_counter
    }

    /// Advance the ordinality counter by the given count and return the starting value.
    pub fn advance_ordinality(&mut self, count: usize) -> i64 {
        let start = self.ordinality_counter;
        self.ordinality_counter += count as i64;
        start
    }
}

impl LocalSourceState for TableFunctionLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Physical table function operator.
///
/// Executes table functions that generate rows dynamically.
/// Examples: `generate_series(1, 10)`, `range(0, 100, 5)`
pub struct PhysicalTableFunction {
    /// Output types of this operator.
    output_types: Vec<LogicalType>,
    /// Bind data containing the function and its state.
    bind_data: Arc<TableFunctionBindDataWrapper>,
    /// Estimated cardinality.
    estimated_cardinality: usize,
}

impl PhysicalTableFunction {
    /// Create a new table function operator.
    pub fn new(bind_data: TableFunctionBindDataWrapper) -> Self {
        let output_types = bind_data.output_types.clone();
        let estimated_cardinality = bind_data.estimated_cardinality().unwrap_or(1000);

        Self {
            output_types,
            bind_data: Arc::new(bind_data),
            estimated_cardinality,
        }
    }

    /// Get the bind data.
    pub fn bind_data(&self) -> &TableFunctionBindDataWrapper {
        &self.bind_data
    }

    /// Get the function name.
    pub fn function_name(&self) -> &str {
        &self.bind_data.function.name
    }

    /// Execute the table function.
    ///
    /// This is the core execution logic that calls the table function's main function.
    /// If WITH ORDINALITY is specified, adds an ordinality column to the result.
    fn execute_function(
        bind_data: &TableFunctionBindDataWrapper,
        gstate: &TableFunctionGlobalState,
        lstate: &mut TableFunctionLocalState,
        chunk: &mut Chunk,
    ) -> Result<SourceResultType> {
        use paro_common::vector::Vector;

        // Check if already finished
        if lstate.is_finished() {
            return Ok(SourceResultType::Finished);
        }

        // Get the main function
        let function = bind_data.function.function.ok_or_else(|| {
            paro_error::internal("Table function has no main function".to_string())
        })?;

        // Build the function input
        let mut func_input = TableFunctionInput {
            bind_data: bind_data.bind_data.as_ref().map(|b| b.as_ref()),
            local_state: lstate.local_state_mut(),
            global_state: gstate.global_state(),
        };

        // Call the function
        let result = function(&mut func_input, chunk)?;

        // Add ordinality column if WITH ORDINALITY is specified
        if bind_data.with_ordinality && chunk.size() > 0 {
            let row_count = chunk.size();
            let start_ordinality = lstate.advance_ordinality(row_count);

            // Generate ordinality values: start_ordinality, start_ordinality+1, ...
            let ordinality_values: Vec<i64> = (0..row_count as i64)
                .map(|i| start_ordinality + i)
                .collect();

            // Create ordinality vector and add to chunk
            let ordinality_vec =
                Vector::try_from_i64(&ordinality_values, chunk.allocator().clone())?;

            // The ordinality column should be the last column
            let ordinality_col_idx = chunk.column_count() - 1;
            if let Some(col) = chunk.column_mut(ordinality_col_idx) {
                *col = ordinality_vec;
            }
        }

        match result {
            TableFunctionResult::HaveMoreOutput => Ok(SourceResultType::HaveMoreOutput),
            TableFunctionResult::Finished => {
                lstate.set_finished();
                if chunk.size() > 0 {
                    Ok(SourceResultType::HaveMoreOutput)
                } else {
                    Ok(SourceResultType::Finished)
                }
            }
        }
    }
}

impl fmt::Debug for PhysicalTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalTableFunction")
            .field("function", &self.bind_data.function.name)
            .field("output_types", &self.output_types)
            .field("estimated_cardinality", &self.estimated_cardinality)
            .finish()
    }
}

impl PhysicalOperator for PhysicalTableFunction {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::TableFunctionScan
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn estimated_cardinality(&self) -> usize {
        self.estimated_cardinality
    }

    fn is_source(&self) -> bool {
        true
    }

    fn is_sink(&self) -> bool {
        false
    }

    fn parallel_source(&self) -> bool {
        // Check if the function supports parallel execution
        // For now, we default to single-threaded unless the function
        // explicitly supports parallelism via init_global/init_local
        self.bind_data.function.init_global.is_some()
            && self.bind_data.function.init_local.is_some()
    }

    fn source_order(&self) -> OrderPreservationType {
        // Table functions typically don't preserve any specific order
        OrderPreservationType::NoOrder
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let bind_data = &self.bind_data;

        // Initialize global state if the function provides init_global
        let mut global_state = if let Some(init_global) = bind_data.function.init_global {
            let input = TableFunctionInitInput::new(
                bind_data.bind_data.as_ref().map(|b| b.as_ref()),
                &bind_data.column_ids,
            )
            .with_max_threads(ctx.session.limits.max_threads);
            init_global(&input)?
        } else {
            None
        };

        if let Some(ref mut gs) = global_state {
            populate_system_table_function_data(&self.bind_data.function.name, gs.as_mut(), ctx);
        }

        // Determine max threads from global state
        let max_threads = global_state
            .as_ref()
            .map(|gs| gs.max_threads())
            .unwrap_or(1);

        Ok(Box::new(TableFunctionGlobalState {
            global_state,
            bind_data: self.bind_data.clone(),
            max_threads,
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        let bind_data = &self.bind_data;

        // Downcast global state
        let gstate = gstate
            .as_any()
            .downcast_ref::<TableFunctionGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global state type".to_string()))?;

        // Initialize local state if the function provides init_local
        let local_state = if let Some(init_local) = bind_data.function.init_local {
            let input = TableFunctionInitInput::new(
                bind_data.bind_data.as_ref().map(|b| b.as_ref()),
                &bind_data.column_ids,
            );
            init_local(&input, gstate.global_state())?
        } else {
            None
        };

        Ok(Box::new(TableFunctionLocalState {
            local_state,
            finished: false,
            ordinality_counter: 1, // Start from 1 per SQL standard
        }))
    }

    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        // Downcast global state
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<TableFunctionGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global state type".to_string()))?;

        // Downcast local state
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<TableFunctionLocalState>()
            .ok_or_else(|| paro_error::internal("Invalid local state type".to_string()))?;

        // Execute the function
        Self::execute_function(&self.bind_data, gstate, lstate, chunk)
    }

    fn get_progress(&self, gstate: &dyn GlobalSourceState) -> ProgressData {
        // Try to get progress from the function's progress callback
        if let Some(progress_fn) = self.bind_data.function.table_scan_progress {
            let gstate = gstate.as_any().downcast_ref::<TableFunctionGlobalState>();

            if let Some(gstate) = gstate {
                let progress = progress_fn(
                    self.bind_data.bind_data.as_ref().map(|b| b.as_ref()),
                    gstate.global_state(),
                );

                if progress >= 0.0 {
                    // Progress is a percentage (0.0 to 100.0), convert to 0.0-1.0
                    return ProgressData::new(progress / 100.0, 0);
                }
            }
        }

        // Try to get progress from global state's get_progress method
        if let Some(gstate) = gstate.as_any().downcast_ref::<TableFunctionGlobalState>() {
            if let Some(gs) = gstate.global_state() {
                let progress = gs.get_progress();
                if progress >= 0.0 {
                    // Progress is a percentage (0.0 to 100.0), convert to 0.0-1.0
                    return ProgressData::new(progress / 100.0, 0);
                }
            }
        }

        // Progress not available
        ProgressData::invalid()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ============================================================================
// ============================================================================

/// Populate data for system table functions from ExecutionContext.
///
/// This function injects real data into system table functions like:
/// - `paro_databases()` - Injects database list from DatabaseManager
/// - `paro_schemas()` - Injects schema list from Catalog
/// - `paro_tables()` - Injects table list from Catalog
/// - `paro_columns()` - Injects column list from Catalog
/// - `paro_views()` - Injects view list from Catalog
/// - `paro_indexes()` - Injects index list from Catalog
fn populate_system_table_function_data(
    function_name: &str,
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    match function_name {
        "paro_databases" => populate_paro_databases(global_state, ctx),
        "paro_schemas" => populate_paro_schemas(global_state, ctx),
        "paro_tables" => populate_paro_tables(global_state, ctx),
        "paro_columns" => populate_paro_columns(global_state, ctx),
        "paro_views" => populate_paro_views(global_state, ctx),
        "paro_indexes" => populate_paro_indexes(global_state, ctx),
        "paro_pg_settings" => populate_paro_pg_settings(global_state, ctx),
        "paro_pg_prepared_statements" => populate_paro_pg_prepared_statements(global_state, ctx),
        "paro_pg_cursors" => populate_paro_pg_cursors(global_state, ctx),
        "paro_optimizers" => populate_paro_optimizers(global_state),
        "paro_storage_info" => populate_paro_storage_info(global_state, ctx),
        "paro_wal_metrics" => populate_paro_wal_metrics(global_state, ctx),
        "paro_transaction_metrics" => populate_paro_transaction_metrics(global_state, ctx),
        "paro_commit_frontiers" => populate_paro_commit_frontiers(global_state, ctx),
        "paro_commit_poison" => populate_paro_commit_poison(global_state, ctx),
        "paro_property_graphs" => populate_paro_property_graphs(global_state, ctx),
        "paro_graph_statistics" => populate_paro_graph_statistics(global_state, ctx),
        _ => {} // Not a system table function, no data to inject
    }
}

fn resolve_table_reference(
    ctx: &ExecutionContext,
    reference: &str,
) -> Result<(String, String, Arc<paro_catalog::entry::TableCatalogEntry>)> {
    let parts: Vec<&str> = reference.split('.').collect();
    let candidates: Vec<(String, String)> = match parts.as_slice() {
        [_table_name] => ctx
            .session
            .search_path()
            .iter()
            .map(|entry| {
                let catalog_name = if entry.catalog.is_empty() {
                    ctx.session.current_database().to_string()
                } else {
                    entry.catalog.clone()
                };
                (catalog_name, entry.schema.clone())
            })
            .collect(),
        [schema_name, _table_name] => {
            vec![(
                ctx.session.current_database().to_string(),
                (*schema_name).to_string(),
            )]
        }
        [catalog_name, schema_name, _table_name] => {
            vec![(catalog_name.to_string(), schema_name.to_string())]
        }
        _ => {
            return Err(paro_error::invalid_input(format!(
                "invalid table reference '{}': expected table, schema.table, or catalog.schema.table",
                reference
            )));
        }
    };

    let table_name = *parts.last().unwrap_or(&reference);
    let txn = ctx.catalog_txn_view();

    for (catalog_name, schema_name) in candidates {
        let Some(database) = ctx.session.database(&catalog_name) else {
            continue;
        };
        let Ok(entry) = database.catalog.get_table(&txn, &schema_name, table_name) else {
            continue;
        };
        if let paro_catalog::entry::CatalogEntryEnum::Table(table) = &*entry {
            return Ok((catalog_name, schema_name, table.clone()));
        }
    }

    Err(paro_error::table_not_found(reference))
}

fn visible_storage_size_bytes(storage: &paro_storage::table::table_handle::TableHandle) -> i64 {
    let tablet = storage.tablet();
    tablet
        .capture_consistent_rowsets(storage.max_version())
        .map(|rowsets| {
            rowsets
                .into_iter()
                .map(|rowset| rowset.total_disk_size())
                .sum::<u64>()
        })
        .unwrap_or(0)
        .min(i64::MAX as u64) as i64
}

fn visible_storage_row_count(storage: &paro_storage::table::table_handle::TableHandle) -> i64 {
    let tablet = storage.tablet();
    tablet
        .capture_consistent_rowsets(storage.max_version())
        .map(|rowsets| {
            rowsets
                .into_iter()
                .map(|rowset| rowset.num_rows())
                .sum::<u64>()
        })
        .unwrap_or(0)
        .min(i64::MAX as u64) as i64
}

fn populate_paro_databases(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_databases::{
        populate_database_data, DatabaseData, ParoDatabasesGlobalState,
    };

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoDatabasesGlobalState>()
    {
        let mut databases = Vec::new();

        for db in ctx.session.databases.iter() {
            databases.push(DatabaseData {
                database_oid: db.identity.id,
                database_name: db.identity.name.clone(),
                owner_oid: 0, // Stub: no user management yet
                encoding: 6,  // UTF8
                collate: "C".to_string(),
                ctype: "C".to_string(),
                is_template: false,
                allow_conn: true,
                conn_limit: -1,
                acl: None,
            });
        }

        // Keep output deterministic for SHOW DATABASES:
        // non-system databases in name order, then system database last.
        databases.sort_by(|a, b| {
            let a_system = a.database_name.eq_ignore_ascii_case("system");
            let b_system = b.database_name.eq_ignore_ascii_case("system");
            match a_system.cmp(&b_system) {
                std::cmp::Ordering::Equal => a.database_name.cmp(&b.database_name),
                other => other,
            }
        });

        populate_database_data(state, databases);
    }
}

/// Populate paro_schemas() with data from Catalog.
fn populate_paro_schemas(global_state: &mut dyn GlobalTableFunctionState, ctx: &ExecutionContext) {
    use paro_function::table::system::paro_schemas::{
        populate_schema_data, ParoSchemasGlobalState, SchemaData,
    };

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoSchemasGlobalState>()
    {
        let mut schemas = Vec::new();

        // Get schemas from all attached databases
        for db in ctx.session.databases.iter() {
            let db_name = db.identity.name.clone();
            let db_oid = db.identity.id;

            // Create catalog transaction
            let txn = ctx.catalog_txn_view();

            // Use list_schemas to get schema names, then get_schema for each
            let schema_names: Vec<String> = {
                use paro_catalog::catalog::Catalog;
                db.catalog.list_schemas(&txn)
            };
            for schema_name in schema_names {
                if let Ok(schema) = db.catalog.get_schema(&txn, &schema_name) {
                    schemas.push(SchemaData {
                        oid: schema.base.object_id.raw(),
                        database_name: db_name.to_string(),
                        database_oid: db_oid,
                        schema_name: schema.base.name.clone(),
                        internal: schema.base.internal,
                    });
                }
            }
        }

        populate_schema_data(state, schemas);
    }
}

fn populate_paro_pg_settings(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_pg_settings::{
        populate_settings_data, ParoPgSettingsGlobalState, SettingRowData,
    };

    let provider = ctx.session.session_metadata_provider();

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoPgSettingsGlobalState>()
    {
        populate_settings_data(
            state,
            provider
                .current_settings()
                .into_iter()
                .map(|row| SettingRowData {
                    name: row.name,
                    setting: row.setting,
                    unit: row.unit,
                    category: row.category,
                    short_desc: row.short_desc,
                    source: row.source,
                    vartype: row.vartype,
                    context: row.context,
                })
                .collect(),
        );
    }
}

fn populate_paro_pg_prepared_statements(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_pg_prepared_statements::{
        populate_prepared_statement_data, ParoPgPreparedStatementsGlobalState,
        PreparedStatementSummaryData,
    };

    let provider = ctx.session.session_metadata_provider();

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoPgPreparedStatementsGlobalState>()
    {
        populate_prepared_statement_data(
            state,
            provider
                .current_prepared_statements()
                .into_iter()
                .map(|row| PreparedStatementSummaryData {
                    name: row.name,
                    statement: row.statement,
                    parameter_types: row.parameter_types,
                    from_sql: row.from_sql,
                    generic_plans: row.generic_plans,
                    custom_plans: row.custom_plans,
                })
                .collect(),
        );
    }
}

fn populate_paro_pg_cursors(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_pg_cursors::{
        populate_cursor_data, CursorSummaryData, ParoPgCursorsGlobalState,
    };

    let provider = ctx.session.session_metadata_provider();

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoPgCursorsGlobalState>()
    {
        populate_cursor_data(
            state,
            provider
                .current_cursors()
                .into_iter()
                .map(|row| CursorSummaryData {
                    name: row.name,
                    statement: row.statement,
                    is_holdable: row.is_holdable,
                    is_binary: row.is_binary,
                    is_scrollable: row.is_scrollable,
                    snapshot_read_ts: row.snapshot_read_ts,
                    snapshot_pin_duration_us: row.snapshot_pin_duration_us,
                    snapshot_owner_session_id: row.snapshot_owner_session_id,
                    snapshot_portal_id: row.snapshot_portal_id,
                    snapshot_retention_policy: row.snapshot_retention_policy,
                })
                .collect(),
        );
    }
}

/// Populate paro_tables() with data from Catalog.
fn populate_paro_tables(global_state: &mut dyn GlobalTableFunctionState, ctx: &ExecutionContext) {
    use paro_catalog::entry::TableType;
    use paro_function::table::system::paro_tables::{
        populate_table_data, ParoTablesGlobalState, TableData,
    };

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoTablesGlobalState>()
    {
        let mut tables = Vec::new();

        // Get tables from all attached databases
        for db in ctx.session.databases.iter() {
            let db_name = db.identity.name.clone();
            let db_oid = db.identity.id;

            // Create catalog transaction
            let txn = ctx.catalog_txn_view();

            let schema_names: Vec<String> = {
                use paro_catalog::catalog::Catalog;
                db.catalog.list_schemas(&txn)
            };
            for schema_name in schema_names {
                if let Ok(schema) = db.catalog.get_schema(&txn, &schema_name) {
                    let schema_oid = schema.base.object_id.raw();

                    // Use tables.scan() to get all table entries
                    for table_entry in schema
                        .collection(paro_catalog::entry::CatalogType::Table)
                        .expect("table collection")
                        .scan(txn.transaction_id, txn.start_time)
                    {
                        if let paro_catalog::entry::CatalogEntryEnum::Table(table) = &*table_entry {
                            tables.push(TableData {
                                database_name: db_name.to_string(),
                                database_oid: db_oid,
                                schema_name: schema_name.clone(),
                                schema_oid,
                                table_name: table.base.base.name.clone(),
                                table_oid: table.base.base.object_id.raw(),
                                internal: table.base.base.internal,
                                temporary: table.table_type == TableType::Temporary,
                                column_count: table.columns.len() as i64,
                                index_count: schema
                                    .collection(paro_catalog::entry::CatalogType::Index)
                                    .expect("index collection")
                                    .scan(txn.transaction_id, txn.start_time)
                                    .into_iter()
                                    .filter_map(|entry| match &*entry {
                                        paro_catalog::entry::CatalogEntryEnum::Index(index)
                                            if index.table_oid
                                                == table.base.base.object_id.raw() =>
                                        {
                                            Some(())
                                        }
                                        _ => None,
                                    })
                                    .count() as i64,
                                estimated_rows: table
                                    .get_storage()
                                    .map(|storage| visible_storage_row_count(storage.as_ref()))
                                    .or_else(|| {
                                        table.statistics.as_ref().map(|stats| {
                                            stats.row_count.min(i64::MAX as u64) as i64
                                        })
                                    })
                                    .unwrap_or(0),
                                estimated_size_bytes: table
                                    .get_storage()
                                    .map(|storage| visible_storage_size_bytes(storage.as_ref()))
                                    .unwrap_or(0),
                            });
                        }
                    }
                }
            }
        }

        populate_table_data(state, tables);
    }
}

/// Populate paro_columns() with data from Catalog.
fn populate_paro_columns(global_state: &mut dyn GlobalTableFunctionState, ctx: &ExecutionContext) {
    use paro_function::table::system::paro_columns::{
        populate_column_data, ColumnData, ParoColumnsGlobalState,
    };

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoColumnsGlobalState>()
    {
        let mut columns = Vec::new();

        // Get columns from all attached databases
        for db in ctx.session.databases.iter() {
            let db_name = db.identity.name.clone();
            let db_oid = db.identity.id;

            // Create catalog transaction
            let txn = ctx.catalog_txn_view();

            let schema_names: Vec<String> = {
                use paro_catalog::catalog::Catalog;
                db.catalog.list_schemas(&txn)
            };
            for schema_name in schema_names {
                if let Ok(schema) = db.catalog.get_schema(&txn, &schema_name) {
                    let schema_oid = schema.base.object_id.raw();

                    // Use tables.scan() to get all table entries
                    for table_entry in schema
                        .collection(paro_catalog::entry::CatalogType::Table)
                        .expect("table collection")
                        .scan(txn.transaction_id, txn.start_time)
                    {
                        if let paro_catalog::entry::CatalogEntryEnum::Table(table) = &*table_entry {
                            for (col_idx, col) in table.columns.iter().enumerate() {
                                columns.push(ColumnData {
                                    database_name: db_name.to_string(),
                                    database_oid: db_oid,
                                    schema_name: schema_name.clone(),
                                    schema_oid,
                                    table_name: table.base.base.name.clone(),
                                    table_oid: table.base.base.object_id.raw(),
                                    column_name: col.name.clone(),
                                    column_index: (col_idx + 1) as i32,
                                    internal: table.base.base.internal,
                                    data_type: col.logical_type.to_string(),
                                    data_type_id: col.logical_type.type_id() as u64,
                                    is_nullable: true, // ColumnDefinition doesn't track nullability
                                    column_default: None, // ColumnDefinition doesn't track defaults
                                    character_maximum_length: None,
                                    character_octet_length: None,
                                    numeric_precision: None,
                                    numeric_precision_radix: None,
                                    numeric_scale: None,
                                    datetime_precision: None,
                                });
                            }
                        }
                    }
                }
            }
        }

        populate_column_data(state, columns);
    }
}

/// Populate paro_views() with data from Catalog.
fn populate_paro_views(global_state: &mut dyn GlobalTableFunctionState, ctx: &ExecutionContext) {
    use paro_function::table::system::paro_views::{
        populate_view_data, ParoViewsGlobalState, ViewData,
    };

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoViewsGlobalState>()
    {
        let mut views = Vec::new();

        // Get views from all attached databases
        for db in ctx.session.databases.iter() {
            let db_name = db.identity.name.clone();
            let db_oid = db.identity.id;

            // Create catalog transaction
            let txn = ctx.catalog_txn_view();

            let schema_names: Vec<String> = {
                use paro_catalog::catalog::Catalog;
                db.catalog.list_schemas(&txn)
            };
            for schema_name in schema_names {
                if let Ok(schema) = db.catalog.get_schema(&txn, &schema_name) {
                    let schema_oid = schema.base.object_id.raw();

                    // Use views.scan() to get all view entries
                    for view_entry in schema
                        .collection(paro_catalog::entry::CatalogType::View)
                        .expect("view collection")
                        .scan(txn.transaction_id, txn.start_time)
                    {
                        if let paro_catalog::entry::CatalogEntryEnum::View(view) = &*view_entry {
                            views.push(ViewData {
                                database_name: db_name.to_string(),
                                database_oid: db_oid,
                                schema_name: schema_name.clone(),
                                schema_oid,
                                view_name: view.base.base.name.clone(),
                                view_oid: view.base.base.object_id.raw(),
                                internal: view.base.base.internal,
                                temporary: view.base.base.temporary,
                                column_count: view.column_types.len() as i64,
                                sql: view.sql.clone(),
                            });
                        }
                    }
                }
            }
        }

        populate_view_data(state, views);
    }
}

/// Populate paro_indexes() with data from Catalog.
fn populate_paro_indexes(global_state: &mut dyn GlobalTableFunctionState, ctx: &ExecutionContext) {
    use paro_catalog::entry::{IndexType, TableCatalogEntry};
    use paro_function::table::system::paro_indexes::{
        populate_index_data, IndexData, ParoIndexesGlobalState,
    };

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoIndexesGlobalState>()
    {
        let mut indexes = Vec::new();

        // Get indexes from all attached databases
        for db in ctx.session.databases.iter() {
            let db_name = db.identity.name.clone();
            let db_oid = db.identity.id;

            // Create catalog transaction
            let txn = ctx.catalog_txn_view();

            let schema_names: Vec<String> = {
                use paro_catalog::catalog::Catalog;
                db.catalog.list_schemas(&txn)
            };
            for schema_name in schema_names {
                if let Ok(schema) = db.catalog.get_schema(&txn, &schema_name) {
                    let schema_oid = schema.base.object_id.raw();
                    let table_entries = schema
                        .collection(paro_catalog::entry::CatalogType::Table)
                        .expect("table collection")
                        .scan(txn.transaction_id, txn.start_time)
                        .into_iter()
                        .filter_map(|entry| match &*entry {
                            paro_catalog::entry::CatalogEntryEnum::Table(table) => {
                                Some(table.clone())
                            }
                            _ => None,
                        })
                        .map(|table| (table.base.base.object_id.raw(), table))
                        .collect::<std::collections::HashMap<u64, Arc<TableCatalogEntry>>>();

                    // Use indexes.scan() to get all index entries
                    for index_entry in schema
                        .collection(paro_catalog::entry::CatalogType::Index)
                        .expect("index collection")
                        .scan(txn.transaction_id, txn.start_time)
                    {
                        if let paro_catalog::entry::CatalogEntryEnum::Index(idx) = &*index_entry {
                            let table_entry = table_entries.get(&idx.table_oid);
                            let column_names = table_entry
                                .map(|table| {
                                    idx.get_column_ids()
                                        .iter()
                                        .filter_map(|column_id| {
                                            table
                                                .columns
                                                .get(column_id.index as usize)
                                                .map(|column| column.name.clone())
                                        })
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();
                            let mut entry_count = table_entry
                                .and_then(|table| table.get_storage())
                                .map(|storage| visible_storage_row_count(storage.as_ref()))
                                .unwrap_or(0);
                            let extra_info = match idx.index_type {
                                IndexType::HNSW => {
                                    let stats = table_entry
                                        .and_then(|table| table.get_storage())
                                        .and_then(|storage| {
                                            idx.get_column_ids().first().and_then(|column_id| {
                                                storage.hnsw_index_statistics(column_id.index)
                                            })
                                        });
                                    if let Some(stats) = stats {
                                        entry_count =
                                            stats.num_indexed_vectors.min(i64::MAX as usize) as i64;
                                        json!({
                                            "column_ids": idx.get_column_ids().iter().map(|column_id| column_id.index).collect::<Vec<_>>(),
                                            "column_names": column_names,
                                            "dimension": stats.dimension,
                                            "graph_size_bytes": stats.graph_size_bytes,
                                            "storage_size_bytes": stats.storage_size_bytes,
                                            "max_level": stats.max_level,
                                            "m": stats.m,
                                            "ef_construction": stats.ef_construction,
                                            "failure_reason": idx.failure_reason(),
                                            "coverage": idx.coverage().map(|coverage| {
                                                json!({
                                                    "visible_version": coverage.visible_version,
                                                    "visible_segment_count": coverage.visible_segment_count,
                                                    "indexed_segment_count": coverage.indexed_segment_count,
                                                    "complete": coverage.is_complete(),
                                                })
                                            }),
                                        })
                                    } else {
                                        json!({
                                            "column_ids": idx.get_column_ids().iter().map(|column_id| column_id.index).collect::<Vec<_>>(),
                                            "column_names": column_names,
                                            "failure_reason": idx.failure_reason(),
                                            "coverage": idx.coverage().map(|coverage| {
                                                json!({
                                                    "visible_version": coverage.visible_version,
                                                    "visible_segment_count": coverage.visible_segment_count,
                                                    "indexed_segment_count": coverage.indexed_segment_count,
                                                    "complete": coverage.is_complete(),
                                                })
                                            }),
                                        })
                                    }
                                }
                                IndexType::Sparse => {
                                    let stats = table_entry
                                        .and_then(|table| table.get_storage())
                                        .and_then(|storage| {
                                            idx.get_column_ids().first().and_then(|column_id| {
                                                storage.sparse_index_statistics(column_id.index)
                                            })
                                        });
                                    if let Some(stats) = stats {
                                        entry_count =
                                            stats.num_indexed_vectors.min(i64::MAX as usize) as i64;
                                        json!({
                                            "column_ids": idx.get_column_ids().iter().map(|column_id| column_id.index).collect::<Vec<_>>(),
                                            "column_names": column_names,
                                            "num_unique_dimensions": stats.num_unique_dimensions,
                                            "num_posting_lists": stats.num_posting_lists,
                                            "total_postings": stats.total_postings,
                                            "avg_vector_nnz": stats.avg_vector_nnz,
                                            "failure_reason": idx.failure_reason(),
                                        })
                                    } else {
                                        json!({
                                            "column_ids": idx.get_column_ids().iter().map(|column_id| column_id.index).collect::<Vec<_>>(),
                                            "column_names": column_names,
                                            "failure_reason": idx.failure_reason(),
                                        })
                                    }
                                }
                                IndexType::FullText => {
                                    let stats = table_entry
                                        .and_then(|table| table.get_storage())
                                        .and_then(|storage| {
                                            idx.fulltext_binding().and_then(|binding| {
                                                storage
                                                    .fulltext_capability(
                                                        binding.column_id.index,
                                                        &binding.config,
                                                    )
                                                    .and_then(|capability| {
                                                        capability
                                                            .generation_stats
                                                            .fulltext_index_statistics()
                                                    })
                                            })
                                        });
                                    if let Some(stats) = stats {
                                        entry_count = stats.total_docs.min(i64::MAX as u32) as i64;
                                        json!({
                                            "column_ids": idx.get_column_ids().iter().map(|column_id| column_id.index).collect::<Vec<_>>(),
                                            "column_names": column_names,
                                            "total_terms": stats.total_terms,
                                            "unique_terms": stats.unique_terms,
                                            "total_postings": stats.total_postings,
                                            "avg_doc_length": stats.avg_doc_length,
                                            "tokenizer": format!("{:?}", stats.tokenizer_kind).to_uppercase(),
                                            "config": idx.fulltext_binding().map(|binding| binding.config.clone()),
                                            "coverage": idx.coverage().map(|coverage| {
                                                json!({
                                                    "visible_version": coverage.visible_version,
                                                    "visible_segment_count": coverage.visible_segment_count,
                                                    "indexed_segment_count": coverage.indexed_segment_count,
                                                    "complete": coverage.is_complete(),
                                                })
                                            }),
                                            "failure_reason": idx.failure_reason(),
                                        })
                                    } else {
                                        json!({
                                            "column_ids": idx.get_column_ids().iter().map(|column_id| column_id.index).collect::<Vec<_>>(),
                                            "column_names": column_names,
                                            "config": idx.fulltext_binding().map(|binding| binding.config.clone()),
                                            "failure_reason": idx.failure_reason(),
                                        })
                                    }
                                }
                                _ => json!({
                                    "column_ids": idx.get_column_ids().iter().map(|column_id| column_id.index).collect::<Vec<_>>(),
                                    "column_names": column_names,
                                    "failure_reason": idx.failure_reason(),
                                }),
                            };
                            indexes.push(IndexData {
                                database_name: db_name.to_string(),
                                database_oid: db_oid,
                                schema_name: schema_name.clone(),
                                schema_oid,
                                table_name: idx.table_name.clone(),
                                table_oid: idx.table_oid,
                                index_name: idx.base.base.name.clone(),
                                index_oid: idx.base.base.object_id.raw(),
                                is_unique: idx.is_unique(),
                                is_primary: idx.is_primary(),
                                index_type: idx.index_type.as_str().to_string(),
                                build_state: format!("{:?}", idx.build_state()).to_uppercase(),
                                entry_count,
                                extra_info: extra_info.to_string(),
                                sql: idx.sql.clone(),
                            });
                        }
                    }
                }
            }
        }

        populate_index_data(state, indexes);
    }
}

fn populate_paro_optimizers(global_state: &mut dyn GlobalTableFunctionState) {
    use paro_function::table::system::paro_optimizers::{
        populate_optimizer_data, OptimizerData, ParoOptimizersGlobalState,
    };

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoOptimizersGlobalState>()
    {
        let snapshot = paro_optimizer::profiler::latest_optimizer_profile_snapshot();
        let entries = snapshot
            .entries
            .into_iter()
            .map(|entry| OptimizerData {
                name: entry.optimizer_type.as_str().to_string(),
                enabled: entry.enabled,
                last_elapsed_us: entry.last_elapsed.as_micros().min(i64::MAX as u128) as i64,
                invocation_count: entry.invocation_count.min(i64::MAX as u64) as i64,
            })
            .collect();
        populate_optimizer_data(state, entries);
    }
}

fn populate_paro_storage_info(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_storage_info::{
        populate_storage_info_data, ParoStorageInfoGlobalState, StorageInfoData,
    };

    let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoStorageInfoGlobalState>()
    else {
        return;
    };

    let resolved = resolve_table_reference(ctx, &state.table_name);
    let Ok((database_name, schema_name, table)) = resolved else {
        populate_storage_info_data(state, Vec::new(), Some(state.table_name.clone()));
        return;
    };

    let Some(storage) = table.get_storage() else {
        populate_storage_info_data(state, Vec::new(), Some(state.table_name.clone()));
        return;
    };

    let tablet = storage.tablet();
    let Ok(rowsets) = tablet.capture_consistent_rowsets(storage.max_version()) else {
        populate_storage_info_data(state, Vec::new(), Some(state.table_name.clone()));
        return;
    };

    let mut entries = Vec::new();
    for rowset in rowsets {
        if rowset.load().is_err() {
            populate_storage_info_data(state, Vec::new(), Some(state.table_name.clone()));
            return;
        }
        for segment in rowset.segments() {
            for meta in segment.column_metas() {
                let stats = meta.column_stats.as_ref();
                let base_stats = stats.map(|stats| stats.statistics());
                let column_name = table
                    .columns
                    .get(meta.column_id as usize)
                    .map(|column| column.name.clone())
                    .unwrap_or_else(|| format!("column_{}", meta.column_id));
                let column_type = table
                    .columns
                    .get(meta.column_id as usize)
                    .map(|column| column.logical_type.to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_string());
                let rowset_id = rowset.rowset_id();
                let segment_id = segment.segment_id();
                entries.push(StorageInfoData {
                    database_name: database_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name: table.base.base.name.clone(),
                    rowset_id: rowset_id.min(i64::MAX as u64) as i64,
                    segment_id: segment_id as i64,
                    column_id: meta.column_id as i64,
                    column_name,
                    column_type,
                    num_rows: meta.num_rows.min(i64::MAX as u64) as i64,
                    segment_file_size_bytes: segment.file_size().min(i64::MAX as u64) as i64,
                    column_size_bytes: meta.total_mem_footprint.min(i64::MAX as u64) as i64,
                    encoding: format!("{:?}", meta.encoding).to_uppercase(),
                    compression: format!("{:?}", meta.compression).to_uppercase(),
                    null_count: meta
                        .null_count
                        .map(|value| value.min(i64::MAX as u64) as i64),
                    distinct_count: stats
                        .map(|stats| stats.get_distinct_count().min(i64::MAX as usize) as i64),
                    min_value: base_stats
                        .and_then(|stats| stats.min_value())
                        .map(|value| value.to_string()),
                    max_value: base_stats
                        .and_then(|stats| stats.max_value())
                        .map(|value| value.to_string()),
                    has_hnsw_index: segment.hnsw_index(meta.column_id).is_some()
                        || storage.has_queryable_search_artifact(
                            SearchIndexKind::Hnsw,
                            rowset_id,
                            segment_id,
                            meta.column_id,
                        ),
                    has_sparse_index: segment.sparse_index(meta.column_id).is_some()
                        || storage.has_queryable_search_artifact(
                            SearchIndexKind::Sparse,
                            rowset_id,
                            segment_id,
                            meta.column_id,
                        ),
                    has_fulltext_index: segment.fulltext_index(meta.column_id).is_some()
                        || storage.has_queryable_search_artifact(
                            SearchIndexKind::FullText,
                            rowset_id,
                            segment_id,
                            meta.column_id,
                        ),
                });
            }
        }
    }

    populate_storage_info_data(state, entries, None);
}

fn populate_paro_wal_metrics(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_wal_metrics::{
        populate_wal_metric_data, ParoWalMetricsGlobalState, WalMetricData,
    };

    let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoWalMetricsGlobalState>()
    else {
        return;
    };

    let mut entries = Vec::new();
    for db in ctx.session.databases.iter() {
        let metrics = db.wal_metrics();
        entries.push(WalMetricData {
            database_oid: db.identity.id,
            database_name: db.identity.name.clone(),
            recovery_mode: metrics.recovery_mode.clone(),
            checkpoint_success_total: metrics.checkpoint_success_total.min(i64::MAX as u64) as i64,
            checkpoint_failure_total: metrics.checkpoint_failure_total.min(i64::MAX as u64) as i64,
            wal_health_check_total: metrics.wal_health_check_total.min(i64::MAX as u64) as i64,
            wal_keep_from: metrics.wal_keep_from.min(i64::MAX as u64) as i64,
            main_wal_needs_truncation: metrics.main_wal_needs_truncation,
            checkpoint_wal_needs_truncation: metrics.checkpoint_wal_needs_truncation,
            recovery_wal_needs_truncation: metrics.recovery_wal_needs_truncation,
            journal_apply_queue_depth: metrics.journal_apply_queue_depth.min(i64::MAX as u64)
                as i64,
            journal_apply_queue_depth_peak: metrics
                .journal_apply_queue_depth_peak
                .min(i64::MAX as u64) as i64,
            journal_apply_active_workers: metrics.journal_apply_active_workers.min(i64::MAX as u64)
                as i64,
            journal_apply_active_workers_peak: metrics
                .journal_apply_active_workers_peak
                .min(i64::MAX as u64) as i64,
            journal_apply_mailbox_count: metrics.journal_apply_mailbox_count.min(i64::MAX as u64)
                as i64,
            journal_apply_applied_lag: metrics.journal_apply_applied_lag.min(i64::MAX as u64)
                as i64,
            journal_apply_published_lag: metrics.journal_apply_published_lag.min(i64::MAX as u64)
                as i64,
            journal_apply_durable_wait_count: metrics
                .journal_apply_durable_wait_count
                .min(i64::MAX as u64) as i64,
            journal_apply_durable_wait_micros: metrics
                .journal_apply_durable_wait_micros
                .min(i64::MAX as u64) as i64,
            journal_apply_applied_wait_count: metrics
                .journal_apply_applied_wait_count
                .min(i64::MAX as u64) as i64,
            journal_apply_applied_wait_micros: metrics
                .journal_apply_applied_wait_micros
                .min(i64::MAX as u64) as i64,
            journal_apply_published_wait_count: metrics
                .journal_apply_published_wait_count
                .min(i64::MAX as u64) as i64,
            journal_apply_published_wait_micros: metrics
                .journal_apply_published_wait_micros
                .min(i64::MAX as u64) as i64,
            journal_commit_bytes_total: metrics.journal_commit_bytes_total.min(i64::MAX as u64)
                as i64,
            journal_group_count: metrics.journal_group_count.min(i64::MAX as u64) as i64,
            journal_group_size_last: metrics.journal_group_size_last.min(i64::MAX as u64) as i64,
            journal_group_size_peak: metrics.journal_group_size_peak.min(i64::MAX as u64) as i64,
            journal_sync_latency_micros_total: metrics
                .journal_sync_latency_micros_total
                .min(i64::MAX as u64) as i64,
            journal_sync_latency_micros_peak: metrics
                .journal_sync_latency_micros_peak
                .min(i64::MAX as u64) as i64,
            journal_replay_rowsets_total: metrics.journal_replay_rowsets_total.min(i64::MAX as u64)
                as i64,
            journal_replay_delete_patches_total: metrics
                .journal_replay_delete_patches_total
                .min(i64::MAX as u64) as i64,
            journal_inline_patch_ratio: if metrics.journal_delete_patch_count == 0 {
                0.0
            } else {
                metrics.journal_inline_delete_patch_count as f64
                    / metrics.journal_delete_patch_count as f64
            },
        });
    }

    populate_wal_metric_data(state, entries);
}

fn populate_paro_transaction_metrics(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_transaction_metrics::{
        populate_transaction_metric_data, ParoTransactionMetricsGlobalState, TransactionMetricData,
    };

    let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoTransactionMetricsGlobalState>()
    else {
        return;
    };

    let mut entries = Vec::new();
    for db in ctx.session.databases.iter() {
        let metrics = db.transaction_metrics();
        entries.push(TransactionMetricData {
            database_oid: db.identity.id,
            database_name: db.identity.name.clone(),
            txn_begin_count: u64_to_i64(metrics.txn_begin_count),
            txn_begin_latency_us_total: u64_to_i64(metrics.txn_begin_latency_us_total),
            txn_begin_latency_us_peak: u64_to_i64(metrics.txn_begin_latency_us_peak),
            txn_commit_count: u64_to_i64(metrics.txn_commit_count),
            txn_commit_latency_us_total: u64_to_i64(metrics.txn_commit_latency_us_total),
            txn_commit_latency_us_peak: u64_to_i64(metrics.txn_commit_latency_us_peak),
            txn_commit_prepare_latency_us_total: u64_to_i64(
                metrics.txn_commit_prepare_latency_us_total,
            ),
            txn_commit_prepare_latency_us_peak: u64_to_i64(
                metrics.txn_commit_prepare_latency_us_peak,
            ),
            txn_commit_validate_latency_us_total: u64_to_i64(
                metrics.txn_commit_validate_latency_us_total,
            ),
            txn_commit_validate_latency_us_peak: u64_to_i64(
                metrics.txn_commit_validate_latency_us_peak,
            ),
            group_commit_fence_us_total: u64_to_i64(metrics.group_commit_fence_us_total),
            group_commit_fence_us_peak: u64_to_i64(metrics.group_commit_fence_us_peak),
            txn_commit_durable_latency_us_total: u64_to_i64(
                metrics.txn_commit_durable_latency_us_total,
            ),
            txn_commit_durable_latency_us_peak: u64_to_i64(
                metrics.txn_commit_durable_latency_us_peak,
            ),
            commit_required_publish_wait_us_total: u64_to_i64(
                metrics.commit_required_publish_wait_us_total,
            ),
            commit_required_publish_wait_us_peak: u64_to_i64(
                metrics.commit_required_publish_wait_us_peak,
            ),
            txn_commit_publish_latency_us_total: u64_to_i64(
                metrics.txn_commit_publish_latency_us_total,
            ),
            txn_commit_publish_latency_us_peak: u64_to_i64(
                metrics.txn_commit_publish_latency_us_peak,
            ),
            commit_ack_mode: metrics.commit_ack_mode.clone(),
            write_conflict_index_size: u64_to_i64(metrics.write_conflict_index_size),
            write_conflict_index_fine_entries: u64_to_i64(
                metrics.write_conflict_index_fine_entries,
            ),
            write_conflict_index_fine_summary_entries: u64_to_i64(
                metrics.write_conflict_index_fine_summary_entries,
            ),
            write_conflict_index_coarse_entries: u64_to_i64(
                metrics.write_conflict_index_coarse_entries,
            ),
            lock_wait_count: u64_to_i64(metrics.lock_wait_count),
            lock_wait_duration_us: u64_to_i64(metrics.lock_wait_duration_us),
            lock_wound_wait_abort_count: u64_to_i64(metrics.lock_wound_wait_abort_count),
            lock_deadlock_abort_count: u64_to_i64(metrics.lock_deadlock_abort_count),
            durable_published_lag_commits: u64_to_i64(metrics.durable_published_lag_commits),
            durable_published_lag_ms: u64_to_i64(metrics.durable_published_lag_ms),
            backpressure_throttle_count: u64_to_i64(metrics.backpressure_throttle_count),
            ssi_validation_abort_count: u64_to_i64(metrics.ssi_validation_abort_count),
            ssi_abort_due_to_coarse_scan_marker: u64_to_i64(
                metrics.ssi_abort_due_to_coarse_scan_marker,
            ),
            read_tracker_record_count: u64_to_i64(metrics.read_tracker_record_count),
            read_tracker_coarsened_count: u64_to_i64(metrics.read_tracker_coarsened_count),
            read_tracking_hint_count: u64_to_i64(metrics.read_tracking_hint_count),
            read_tracking_policy_escalation_count: u64_to_i64(
                metrics.read_tracking_policy_escalation_count,
            ),
            read_tracking_point_critical_count: u64_to_i64(
                metrics.read_tracking_point_critical_count,
            ),
            read_tracking_range_critical_count: u64_to_i64(
                metrics.read_tracking_range_critical_count,
            ),
            read_tracking_analytical_scan_count: u64_to_i64(
                metrics.read_tracking_analytical_scan_count,
            ),
            read_tracking_safe_snapshot_preferred_count: u64_to_i64(
                metrics.read_tracking_safe_snapshot_preferred_count,
            ),
            derived_index_lag_ts: u64_to_i64(metrics.derived_index_lag_ts),
            derived_delta_merge_cost: u64_to_i64(metrics.derived_delta_merge_cost),
            commit_participant_count: u64_to_i64(metrics.commit_participant_count),
            inflight_batch_conflict_reject_count: u64_to_i64(
                metrics.inflight_batch_conflict_reject_count,
            ),
            retention_watermark_lag_ms: u64_to_i64(metrics.retention_watermark_lag_ms),
            oldest_active_rw_lag_ms: u64_to_i64(metrics.oldest_active_rw_lag_ms),
            read_snapshot_lease_count: u64_to_i64(metrics.read_snapshot_lease_count),
            active_rw_txn_count: u64_to_i64(metrics.active_rw_txn_count),
        });
    }

    populate_transaction_metric_data(state, entries);
}

fn populate_paro_commit_frontiers(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_commit_frontiers::{
        populate_commit_frontier_data, CommitFrontierData, ParoCommitFrontiersGlobalState,
    };

    let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoCommitFrontiersGlobalState>()
    else {
        return;
    };

    let mut entries = Vec::new();
    for db in ctx.session.databases.iter() {
        let frontier = db.commit_frontier();
        entries.push(CommitFrontierData {
            database_oid: db.identity.id,
            database_name: db.identity.name.clone(),
            durable_commit_id: frontier.durable_commit_id,
            published_commit_id: frontier.published_commit_id,
            durable_commit_bytes: frontier.durable_commit_bytes,
            published_commit_bytes: frontier.published_commit_bytes,
            durable_to_published_bytes_lag: frontier.durable_to_published_bytes_lag,
            stale_bytes_at_poison: frontier.stale_bytes_at_poison,
            publish_failure_watermark: frontier.publish_failure_watermark,
            publish_failure_cause: frontier.publish_failure_cause.clone(),
            wait_count: frontier.wait_count,
            wait_wake_count: frontier.wait_wake_count,
            notify_all_count: frontier.notify_all_count,
            notify_suppressed_count: frontier.notify_suppressed_count,
            publish_failure_count: frontier.publish_failure_count,
        });
    }

    populate_commit_frontier_data(state, entries);
}

fn populate_paro_commit_poison(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_commit_poison::{
        populate_commit_poison_data, CommitPoisonData, ParoCommitPoisonGlobalState,
    };

    let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoCommitPoisonGlobalState>()
    else {
        return;
    };

    let mut entries = Vec::new();
    for db in ctx.session.databases.iter() {
        let poison = db.commit_poison();
        entries.push(CommitPoisonData {
            database_oid: db.identity.id,
            database_name: db.identity.name.clone(),
            admission_state: poison.admission_state.clone(),
            admission_open: poison.admission_open,
            poisoned: poison.poisoned,
            poison_cause: poison.poison_cause.clone(),
            first_blocked_commit_ts: poison.first_blocked_commit_ts,
        });
    }

    populate_commit_poison_data(state, entries);
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

/// Populate paro_property_graphs() with data from Catalog and GraphProjectionIndexManager.
fn populate_paro_property_graphs(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_property_graphs::{
        populate_property_graph_data, ParoPropertyGraphsGlobalState, PropertyGraphData,
    };

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoPropertyGraphsGlobalState>()
    {
        let mut graphs = Vec::new();

        for db in ctx.session.databases.iter() {
            let txn = ctx.catalog_txn_view();

            let schema_names: Vec<String> = {
                use paro_catalog::catalog::Catalog;
                db.catalog.list_schemas(&txn)
            };
            for schema_name in schema_names {
                if let Ok(schema) = db.catalog.get_schema(&txn, &schema_name) {
                    for entry in schema
                        .collection(paro_catalog::entry::CatalogType::PropertyGraph)
                        .expect("property graph collection")
                        .scan(txn.transaction_id, txn.start_time)
                    {
                        if let paro_catalog::entry::CatalogEntryEnum::PropertyGraph(pg) = &*entry {
                            let graph_name = pg.info.graph_name.clone();
                            let vertex_tables: Vec<String> = pg
                                .info
                                .vertex_tables
                                .iter()
                                .map(|v| v.label.clone())
                                .collect();
                            let edge_tables: Vec<String> = pg
                                .info
                                .edge_tables
                                .iter()
                                .map(|e| e.label.clone())
                                .collect();

                            let (
                                vertex_count,
                                edge_count,
                                state,
                                delta_size,
                                last_rebuild_micros,
                                fingerprint,
                                index_size,
                            ) = if let Some(snapshot) = ctx.session.services.graph_index.snapshot(
                                &paro_common::identity::GraphId::new(
                                    db.identity.name.clone(),
                                    schema_name.clone(),
                                    graph_name.clone(),
                                ),
                            ) {
                                let index = snapshot.base();
                                let graph_stats = snapshot.statistics();
                                let vc: i64 = index
                                    .vertex_labels()
                                    .iter()
                                    .map(|label| {
                                        graph_stats
                                            .vertex_count(label)
                                            .or_else(|| {
                                                index
                                                    .vertex_map(label)
                                                    .map(|map| map.num_vertices() as u64)
                                            })
                                            .unwrap_or(0)
                                            as i64
                                    })
                                    .sum();
                                let ec: i64 = index
                                    .edge_labels()
                                    .iter()
                                    .map(|label| {
                                        graph_stats
                                            .edge_count(label)
                                            .or_else(|| {
                                                index.forward_csr(label).map(|csr| csr.num_edges())
                                            })
                                            .unwrap_or(0)
                                            as i64
                                    })
                                    .sum();
                                (
                                    vc,
                                    ec,
                                    format!("{:?}", snapshot.manifest().state()).to_uppercase(),
                                    snapshot.delta_size() as i64,
                                    Some(snapshot.manifest().last_rebuild_epoch_ms() as i64 * 1000),
                                    snapshot.manifest().schema_fingerprint().to_string(),
                                    index.memory_usage() as i64,
                                )
                            } else {
                                (0, 0, "MISSING".to_string(), 0, None, String::new(), 0)
                            };

                            graphs.push(PropertyGraphData {
                                graph_name,
                                vertex_tables: vertex_tables.join(", "),
                                edge_tables: edge_tables.join(", "),
                                state,
                                vertex_count,
                                edge_count,
                                delta_size,
                                last_rebuild_micros,
                                fingerprint,
                                index_size_bytes: index_size,
                            });
                        }
                    }
                }
            }
        }

        graphs.sort_by(|a, b| a.graph_name.cmp(&b.graph_name));
        populate_property_graph_data(state, graphs);
    }
}

/// Populate paro_graph_statistics() with data from GraphProjectionIndexManager.
fn populate_paro_graph_statistics(
    global_state: &mut dyn GlobalTableFunctionState,
    ctx: &ExecutionContext,
) {
    use paro_function::table::system::paro_graph_statistics::{
        populate_graph_statistics_data, GraphStatisticsData, ParoGraphStatisticsGlobalState,
    };

    if let Some(state) = global_state
        .as_any_mut()
        .downcast_mut::<ParoGraphStatisticsGlobalState>()
    {
        let graph_name = state.graph_name.clone();
        let mut stats = Vec::new();

        let graph_id = paro_common::identity::GraphId::new(
            ctx.session.current_database(),
            ctx.session.current_schema(),
            &graph_name,
        );
        if let Some(snapshot) = ctx.session.services.graph_index.snapshot(&graph_id) {
            let index = snapshot.base();
            let graph_stats = snapshot.statistics();

            // Vertex labels
            for label in index.vertex_labels() {
                if let Some(vmap) = index.vertex_map(&label) {
                    stats.push(GraphStatisticsData {
                        label: label.clone(),
                        label_type: "vertex".to_string(),
                        count: graph_stats
                            .vertex_count(&label)
                            .unwrap_or(vmap.num_vertices() as u64)
                            as i64,
                        avg_degree: graph_stats.avg_degree(&label),
                        index_size_bytes: vmap.memory_usage() as i64,
                    });
                }
            }

            // Edge labels
            for label in index.edge_labels() {
                if let Some(csr) = index.forward_csr(&label) {
                    let num_edges = graph_stats
                        .edge_count(&label)
                        .unwrap_or_else(|| csr.num_edges());
                    let avg_degree = index
                        .edge_endpoints(&label)
                        .and_then(|(source_label, _)| {
                            graph_stats
                                .vertex_count(source_label)
                                .filter(|count| *count > 0)
                                .map(|count| num_edges as f64 / count as f64)
                        })
                        .unwrap_or(0.0);
                    let mut size = csr.memory_usage();
                    if let Some(bwd) = index.backward_csr(&label) {
                        size += bwd.memory_usage();
                    }
                    stats.push(GraphStatisticsData {
                        label: label.clone(),
                        label_type: "edge".to_string(),
                        count: num_edges as i64,
                        avg_degree: Some(avg_degree),
                        index_size_bytes: size as i64,
                    });
                }
            }
        }

        populate_graph_statistics_data(state, stats);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use paro_function::table::system::paro_optimizers::ParoOptimizersGlobalState;
    use paro_optimizer::optimizer_type::OptimizerType;
    use paro_optimizer::profiler::{
        publish_optimizer_profile_snapshot, OptimizerProfileSnapshot, OptimizerProfileSnapshotEntry,
    };

    #[test]
    fn populate_paro_optimizers_reads_latest_snapshot() {
        publish_optimizer_profile_snapshot(OptimizerProfileSnapshot {
            entries: vec![
                OptimizerProfileSnapshotEntry {
                    optimizer_type: OptimizerType::FilterPushdown,
                    enabled: true,
                    last_elapsed: Duration::from_micros(33),
                    invocation_count: 5,
                },
                OptimizerProfileSnapshotEntry {
                    optimizer_type: OptimizerType::JoinOrder,
                    enabled: false,
                    last_elapsed: Duration::from_micros(0),
                    invocation_count: 0,
                },
            ],
        });

        let mut state = ParoOptimizersGlobalState {
            entries: Vec::new(),
            offset: AtomicUsize::new(99),
        };

        populate_paro_optimizers(&mut state);

        assert_eq!(state.offset.load(Ordering::Relaxed), 0);
        assert_eq!(state.entries.len(), 2);
        assert_eq!(state.entries[0].name, "filter_pushdown");
        assert!(state.entries[0].enabled);
        assert_eq!(state.entries[0].last_elapsed_us, 33);
        assert_eq!(state.entries[0].invocation_count, 5);
        assert_eq!(state.entries[1].name, "join_order");
        assert!(!state.entries[1].enabled);
    }
}

//! paro_logs() Table Function
//!
//! # Overview
//!
//! Returns log entries stored in the in-memory log storage.
//!
//! ## Return Columns
//!
//!
//! ## Example
//!
//! ```sql
//! SELECT * FROM paro_logs() LIMIT 100;
//! SELECT * FROM paro_logs() WHERE level = 'ERROR';
//! SELECT * FROM paro_logs() WHERE target LIKE 'paro::query%';
//! ```

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use paro_common::chunk::Chunk;
use paro_common::config::LogLevel;
use paro_common::error::Result;
use paro_common::logging::{LogEntry, MemoryLogStorage};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::table::{
    GlobalTableFunctionState, TableFunction, TableFunctionBindData, TableFunctionBindInput,
    TableFunctionInitInput, TableFunctionInput, TableFunctionResult, TableFunctionSet,
};

// ============================================================================
// Global Log Storage Registry
// ============================================================================

/// Global storage for log entries.
///
/// This is set during server initialization when LogManager is created.
static GLOBAL_LOG_STORAGE: OnceLock<Arc<MemoryLogStorage>> = OnceLock::new();

/// Register the global log storage.
///
/// This should be called once during server initialization.
pub fn register_log_storage(storage: Arc<MemoryLogStorage>) {
    let _ = GLOBAL_LOG_STORAGE.set(storage);
}

/// Get the global log storage if registered.
pub fn get_log_storage() -> Option<Arc<MemoryLogStorage>> {
    GLOBAL_LOG_STORAGE.get().cloned()
}

// ============================================================================
// Bind Data
// ============================================================================

/// Bind data for paro_logs().
#[derive(Clone)]
pub struct ParoLogsBindData;

impl TableFunctionBindData for ParoLogsBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn cardinality(&self) -> Option<usize> {
        None
    }
}

// ============================================================================
// Global State
// ============================================================================

/// Global state for paro_logs().
pub struct ParoLogsGlobalState {
    /// Collected log entries
    pub entries: Vec<LogEntry>,
    /// Current offset into entries
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoLogsGlobalState {
    fn max_threads(&self) -> usize {
        1
    }

    fn get_progress(&self) -> f64 {
        if self.entries.is_empty() {
            return 100.0;
        }
        let offset = self.offset.load(Ordering::Relaxed);
        (offset as f64 / self.entries.len() as f64) * 100.0
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ============================================================================
// Function Implementation
// ============================================================================

/// Convert LogLevel to string.
fn level_to_string(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Trace => "TRACE",
        LogLevel::Debug => "DEBUG",
        LogLevel::Info => "INFO",
        LogLevel::Warn => "WARN",
        LogLevel::Error => "ERROR",
    }
}

/// Convert fields to JSON string.
fn fields_to_json(fields: &[(String, String)]) -> String {
    if fields.is_empty() {
        return "{}".to_string();
    }
    let pairs: Vec<String> = fields
        .iter()
        .map(|(k, v)| format!("\"{}\": \"{}\"", k, v.replace('"', "\\\"")))
        .collect();
    format!("{{{}}}", pairs.join(", "))
}

/// Bind function for paro_logs().
fn paro_logs_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    names.push("timestamp".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("level".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("target".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("message".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("fields".to_string());
    return_types.push(LogicalType::Varchar);

    Ok(Some(Box::new(ParoLogsBindData)))
}

/// Init global function for paro_logs().
fn paro_logs_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    // Get log entries from global storage
    let entries = match get_log_storage() {
        Some(storage) => storage.all(),
        None => Vec::new(),
    };

    Ok(Some(Box::new(ParoLogsGlobalState {
        entries,
        offset: AtomicUsize::new(0),
    })))
}

/// Main function for paro_logs().
fn paro_logs_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoLogsGlobalState>());

    let gstate = match gstate {
        Some(gs) => gs,
        None => {
            output.set_cardinality(0);
            return Ok(TableFunctionResult::Finished);
        }
    };

    let offset = gstate.offset.load(Ordering::Relaxed);
    if offset >= gstate.entries.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    // Fill output chunk
    let batch_size = 2048.min(gstate.entries.len() - offset);
    let mut count = 0;

    let mut timestamps = Vec::with_capacity(batch_size);
    let mut levels = Vec::with_capacity(batch_size);
    let mut targets = Vec::with_capacity(batch_size);
    let mut messages = Vec::with_capacity(batch_size);
    let mut fields_json = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        timestamps.push(entry.timestamp as i64);
        levels.push(level_to_string(entry.level).to_string());
        targets.push(entry.target.clone());
        messages.push(entry.message.clone());
        fields_json.push(fields_to_json(&entry.fields));
        count += 1;
    }

    gstate.offset.fetch_add(count, Ordering::Relaxed);

    if count > 0 {
        // Column 0: timestamp (BIGINT)
        let ts_vec = Vector::from_i64(&timestamps);
        if let Some(col) = output.column_mut(0) {
            *col = ts_vec;
        }

        // Column 1: level (VARCHAR)
        let level_refs: Vec<&str> = levels.iter().map(|s| s.as_str()).collect();
        let level_vec = Vector::from_strings(&level_refs);
        if let Some(col) = output.column_mut(1) {
            *col = level_vec;
        }

        // Column 2: target (VARCHAR)
        let target_refs: Vec<&str> = targets.iter().map(|s| s.as_str()).collect();
        let target_vec = Vector::from_strings(&target_refs);
        if let Some(col) = output.column_mut(2) {
            *col = target_vec;
        }

        // Column 3: message (VARCHAR)
        let message_refs: Vec<&str> = messages.iter().map(|s| s.as_str()).collect();
        let message_vec = Vector::from_strings(&message_refs);
        if let Some(col) = output.column_mut(3) {
            *col = message_vec;
        }

        // Column 4: fields (VARCHAR)
        let fields_refs: Vec<&str> = fields_json.iter().map(|s| s.as_str()).collect();
        let fields_vec = Vector::from_strings(&fields_refs);
        if let Some(col) = output.column_mut(4) {
            *col = fields_vec;
        }

        output.set_cardinality(count);
    }

    let new_offset = gstate.offset.load(Ordering::Relaxed);
    if new_offset >= gstate.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

/// Progress function for paro_logs().
fn paro_logs_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    match global_state {
        Some(gs) => gs.get_progress(),
        None => -1.0,
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Create the paro_logs() table function set.
pub fn create_paro_logs_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_logs", vec![]);

    func.bind = Some(paro_logs_bind);
    func.init_global = Some(paro_logs_init_global);
    func.function = Some(paro_logs_function);
    func.table_scan_progress = Some(paro_logs_progress);

    let mut set = TableFunctionSet::new("paro_logs");
    set.add_function(func);
    set
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paro_logs_bind() {
        let empty_map = std::collections::HashMap::new();
        let input = TableFunctionBindInput::new(&[], &empty_map);
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let result = paro_logs_bind(&input, &mut return_types, &mut names);
        assert!(result.is_ok());

        assert_eq!(names.len(), 5);
        assert_eq!(names[0], "timestamp");
        assert_eq!(names[1], "level");
        assert_eq!(names[2], "target");
        assert_eq!(names[3], "message");
        assert_eq!(names[4], "fields");
    }

    #[test]
    fn test_fields_to_json() {
        let fields = vec![
            ("user_id".to_string(), "123".to_string()),
            ("action".to_string(), "login".to_string()),
        ];
        let json = fields_to_json(&fields);
        assert!(json.contains("\"user_id\": \"123\""));
        assert!(json.contains("\"action\": \"login\""));
    }

    #[test]
    fn test_fields_to_json_empty() {
        let fields: Vec<(String, String)> = vec![];
        let json = fields_to_json(&fields);
        assert_eq!(json, "{}");
    }

    #[test]
    fn test_level_to_string() {
        assert_eq!(level_to_string(LogLevel::Trace), "TRACE");
        assert_eq!(level_to_string(LogLevel::Debug), "DEBUG");
        assert_eq!(level_to_string(LogLevel::Info), "INFO");
        assert_eq!(level_to_string(LogLevel::Warn), "WARN");
        assert_eq!(level_to_string(LogLevel::Error), "ERROR");
    }
}

// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Table function scan (`generate_series`, etc.).

use paro_common::types::LogicalType;
use paro_function::table::{BoundTableFunctionData, TableFunction};
use std::sync::Arc;

use crate::expression::Expression;

/// TableFunctionGet represents a table function scan.
///
/// Table functions generate rows dynamically, such as:
/// - `generate_series(1, 10)` - generates a sequence of numbers
/// - `range(0, 100, 10)` - generates a range with step
/// - `read_csv('file.csv')` - reads from a CSV file
///
/// When `is_in_out_function` is true, this represents a table-in-out function
/// that processes input data. The `input_table_types` and `input_table_names`
/// fields describe the input table schema.
///
/// When `with_ordinality` is true, an additional `ordinality` column of type
/// BIGINT is added to the result, numbering rows starting from 1.
#[derive(Debug, Clone)]
pub struct TableFunctionGet {
    /// The table function definition.
    pub function: Arc<TableFunction>,
    /// Bind data produced by a statement-specific planner path.
    pub bind_data: Option<BoundTableFunctionData>,
    /// Unique table index for this function call.
    pub table_index: usize,
    /// Column names returned by the function.
    pub column_names: Vec<String>,
    /// Column types returned by the function.
    pub column_types: Vec<LogicalType>,
    /// Bound arguments passed to the function.
    pub arguments: Vec<Expression>,
    /// Optional projection (column indices to return).
    pub projection_ids: Option<Vec<usize>>,
    /// Input table types (for table-in-out functions).
    pub input_table_types: Vec<LogicalType>,
    /// Input table column names (for table-in-out functions).
    pub input_table_names: Vec<String>,
    /// When true, adds an `ordinality` column numbering rows from 1.
    pub with_ordinality: bool,
}

impl TableFunctionGet {
    /// Create a new TableFunctionGet operator.
    pub fn new(
        function: Arc<TableFunction>,
        table_index: usize,
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
        arguments: Vec<Expression>,
    ) -> Self {
        Self {
            function,
            bind_data: None,
            table_index,
            column_names,
            column_types,
            arguments,
            projection_ids: None,
            input_table_types: Vec::new(),
            input_table_names: Vec::new(),
            with_ordinality: false,
        }
    }

    /// Create a new TableFunctionGet for table-in-out functions.
    pub fn new_in_out(
        function: Arc<TableFunction>,
        table_index: usize,
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
        arguments: Vec<Expression>,
        input_table_types: Vec<LogicalType>,
        input_table_names: Vec<String>,
    ) -> Self {
        Self {
            function,
            bind_data: None,
            table_index,
            column_names,
            column_types,
            arguments,
            projection_ids: None,
            input_table_types,
            input_table_names,
            with_ordinality: false,
        }
    }

    /// Create with projection.
    pub fn with_projection(mut self, projection_ids: Vec<usize>) -> Self {
        self.projection_ids = Some(projection_ids);
        self
    }

    /// Attach bind data already resolved by the statement binder.
    pub fn with_bind_data(mut self, bind_data: BoundTableFunctionData) -> Self {
        self.bind_data = Some(bind_data);
        self
    }

    /// Set input table info (for table-in-out functions).
    pub fn with_input_table(
        mut self,
        input_table_types: Vec<LogicalType>,
        input_table_names: Vec<String>,
    ) -> Self {
        self.input_table_types = input_table_types;
        self.input_table_names = input_table_names;
        self
    }

    /// Set WITH ORDINALITY flag.
    pub fn with_ordinality_flag(mut self, with_ordinality: bool) -> Self {
        self.with_ordinality = with_ordinality;
        self
    }

    /// Check if this is a table-in-out function.
    pub fn is_in_out_function(&self) -> bool {
        self.function.is_in_out_function()
    }

    /// Get the output types.
    pub fn get_types(&self) -> Vec<LogicalType> {
        if let Some(ref proj) = self.projection_ids {
            proj.iter().map(|&i| self.column_types[i].clone()).collect()
        } else {
            self.column_types.clone()
        }
    }

    /// Get the output column names.
    pub fn get_names(&self) -> Vec<String> {
        if let Some(ref proj) = self.projection_ids {
            proj.iter().map(|&i| self.column_names[i].clone()).collect()
        } else {
            self.column_names.clone()
        }
    }

    /// Get the operator name.
    pub fn name(&self) -> String {
        format!("TABLE_FUNCTION({})", self.function.name)
    }

    /// Get the function name.
    pub fn function_name(&self) -> &str {
        &self.function.name
    }

    /// Get the number of arguments.
    pub fn argument_count(&self) -> usize {
        self.arguments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::ConstantExpression;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_function::table::TableFunction;
    use std::sync::Arc;

    fn create_generate_series_function() -> Arc<TableFunction> {
        let mut func = TableFunction::new(
            "generate_series",
            vec![LogicalType::BigInt, LogicalType::BigInt],
        );
        func.bind = Some(|_input, types, names| {
            types.push(LogicalType::BigInt);
            names.push("i".to_string());
            Ok(None)
        });
        Arc::new(func)
    }

    #[test]
    fn test_logical_table_function_get_new() {
        let func = create_generate_series_function();
        let args = vec![
            Expression::Constant(ConstantExpression {
                value: Value::BigInt(1),
                return_type: LogicalType::BigInt,
            }),
            Expression::Constant(ConstantExpression {
                value: Value::BigInt(10),
                return_type: LogicalType::BigInt,
            }),
        ];

        let op = TableFunctionGet::new(
            func,
            0,
            vec!["i".to_string()],
            vec![LogicalType::BigInt],
            args,
        );

        assert_eq!(op.table_index, 0);
        assert_eq!(op.function_name(), "generate_series");
        assert_eq!(op.argument_count(), 2);
        assert!(op.name().contains("generate_series"));
    }

    #[test]
    fn test_logical_table_function_get_types() {
        let func = create_generate_series_function();
        let op = TableFunctionGet::new(
            func,
            0,
            vec!["i".to_string()],
            vec![LogicalType::BigInt],
            vec![],
        );

        let types = op.get_types();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0], LogicalType::BigInt);
    }

    #[test]
    fn test_logical_table_function_get_names() {
        let func = create_generate_series_function();
        let op = TableFunctionGet::new(
            func,
            0,
            vec!["i".to_string()],
            vec![LogicalType::BigInt],
            vec![],
        );

        let names = op.get_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "i");
    }

    #[test]
    fn test_logical_table_function_get_with_projection() {
        let mut func = TableFunction::new("multi_column", vec![]);
        func.bind = Some(|_input, types, names| {
            types.push(LogicalType::Integer);
            types.push(LogicalType::Varchar);
            types.push(LogicalType::BigInt);
            names.push("a".to_string());
            names.push("b".to_string());
            names.push("c".to_string());
            Ok(None)
        });

        let op = TableFunctionGet::new(
            Arc::new(func),
            0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec![
                LogicalType::Integer,
                LogicalType::Varchar,
                LogicalType::BigInt,
            ],
            vec![],
        )
        .with_projection(vec![0, 2]); // Only columns a and c

        let types = op.get_types();
        assert_eq!(types.len(), 2);
        assert_eq!(types[0], LogicalType::Integer);
        assert_eq!(types[1], LogicalType::BigInt);

        let names = op.get_names();
        assert_eq!(names.len(), 2);
        assert_eq!(names[0], "a");
        assert_eq!(names[1], "c");
    }
}

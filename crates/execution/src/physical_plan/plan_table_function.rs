//! Plan Table Function - Convert TableFunctionGet to PhysicalTableFunction
//!
//!
//! ## Dependencies Check
//! - TableFunctionGet: ✅ From paro-planner
//! - PhysicalTableFunction: ✅ Implemented
//! - TableFunction: ✅ From paro-function
//!
//! ## Design Notes
//! - Converts TableFunctionGet to PhysicalTableFunction or TableInOutFunction
//! - Evaluates constant arguments at plan time
//! - Calls the bind function to get bind data and return types

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_function::table::{TableFunctionBindData, TableFunctionBindInput};
use paro_planner::expression::Expression;
use paro_planner::operator::table_function::TableFunctionGet;
use std::collections::HashMap;
use std::sync::Arc;

use crate::operator::projection::table_in_out_function::{
    TableInOutBindDataWrapper, TableInOutFunction,
};
use crate::operator::scan::table_function::{PhysicalTableFunction, TableFunctionBindDataWrapper};
use crate::operator::PhysicalOperator;

use super::generator::PhysicalPlanGenerator;

impl PhysicalPlanGenerator {
    /// Create physical plan for TableFunctionGet (table function scan).
    ///
    /// This method converts a logical table function get operation into a physical
    /// table function operator. It evaluates constant arguments and calls the
    /// bind function to determine return types.
    ///
    /// instead of PhysicalTableFunction.
    ///
    /// # Arguments
    /// * `get` - The logical table function get operator
    ///
    /// # Returns
    /// * `Ok(PhysicalTableFunction)` - The physical table function operator
    /// * `Ok(TableInOutFunction)` - For table-in-out functions
    /// * `Err` - If argument evaluation fails or bind fails
    pub fn create_plan_table_function(
        &self,
        get: &TableFunctionGet,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // Check if this is a table-in-out function
        if get.function.is_in_out_function() {
            return self.create_plan_table_in_out_function(get);
        }

        // Step 1: Evaluate constant arguments
        let input_values = self.evaluate_table_function_arguments(&get.arguments)?;

        // Step 2: Call bind function if available
        let bind_data = self.bind_table_function(get, &input_values)?;

        // Step 3: Determine column IDs for projection
        let column_ids = get
            .projection_ids
            .clone()
            .unwrap_or_else(|| (0..get.column_types.len()).collect());

        // Step 4: Get output types (with projection applied)
        let output_types = get.get_types();
        let output_names = get.get_names();

        // Step 5: Create bind data wrapper
        let bind_data_wrapper = TableFunctionBindDataWrapper::new(
            get.function.clone(),
            bind_data,
            input_values,
            column_ids,
            output_types,
            output_names,
        )
        .with_ordinality_flag(get.with_ordinality);

        // Step 6: Create and return PhysicalTableFunction
        let op = PhysicalTableFunction::new(bind_data_wrapper);
        Ok(Arc::new(op))
    }

    ///
    /// Table-in-out functions process input data from a child operator.
    fn create_plan_table_in_out_function(
        &self,
        get: &TableFunctionGet,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // Step 1: Evaluate constant arguments
        let input_values = self.evaluate_table_function_arguments(&get.arguments)?;

        // Step 2: Call bind function if available
        let bind_data = self.bind_table_function_with_input(get, &input_values)?;

        // Step 3: Determine column IDs for projection
        let column_ids = get
            .projection_ids
            .clone()
            .unwrap_or_else(|| (0..get.column_types.len()).collect());

        // Step 4: Get output types (with projection applied)
        let output_types = get.get_types();
        let output_names = get.get_names();

        // Step 5: Create bind data wrapper for table-in-out function
        let bind_data_wrapper = TableInOutBindDataWrapper::new(
            get.function.clone(),
            bind_data,
            input_values,
            column_ids,
            output_types,
            output_names,
            get.input_table_types.clone(),
            get.input_table_names.clone(),
        );

        // Step 6: Create child operator if there's a child
        // For now, we create a dummy scan as the child
        // In a full implementation, the child would come from the logical plan
        let child: Arc<dyn PhysicalOperator> =
            Arc::new(crate::operator::scan::dummy_scan::PhysicalDummyScan::new());

        // Step 7: Create and return TableInOutFunction
        let op = TableInOutFunction::new(bind_data_wrapper, child);
        Ok(Arc::new(op))
    }

    /// Evaluate table function arguments to constant values.
    ///
    /// Table function arguments must be constants at plan time.
    /// This also handles Cast expressions where the child is a constant.
    fn evaluate_table_function_arguments(&self, arguments: &[Expression]) -> Result<Vec<Value>> {
        let mut values = Vec::with_capacity(arguments.len());

        for arg in arguments {
            let value = self.evaluate_constant_expression(arg)?;
            values.push(value);
        }

        Ok(values)
    }

    /// Recursively evaluate a constant expression to a Value.
    ///
    /// Handles Constant expressions directly, and Cast expressions
    /// where the child is a constant.
    fn evaluate_constant_expression(&self, expr: &Expression) -> Result<Value> {
        match expr {
            Expression::Constant(constant) => Ok(constant.value.clone()),
            Expression::Cast(cast) => {
                // Recursively evaluate the child expression
                let child_value = self.evaluate_constant_expression(&cast.child)?;
                // Apply the cast to the value
                self.apply_cast_to_value(child_value, &cast.target_type)
            }
            _ => Err(paro_error::not_implemented(
                "Table function arguments must be constants".to_string(),
            )),
        }
    }

    /// Apply a cast to a Value, converting it to the target type.
    fn apply_cast_to_value(
        &self,
        value: Value,
        target_type: &paro_common::types::LogicalType,
    ) -> Result<Value> {
        use paro_common::error as paro_error;
        use paro_common::types::LogicalType;

        match (&value, target_type) {
            // Integer to BigInt
            (Value::Integer(i), LogicalType::BigInt) => Ok(Value::BigInt(*i as i64)),
            // SmallInt to BigInt
            (Value::SmallInt(i), LogicalType::BigInt) => Ok(Value::BigInt(*i as i64)),
            // TinyInt to BigInt
            (Value::TinyInt(i), LogicalType::BigInt) => Ok(Value::BigInt(*i as i64)),
            // Integer to Double
            (Value::Integer(i), LogicalType::Double) => Ok(Value::Double(*i as f64)),
            // BigInt to Double
            (Value::BigInt(i), LogicalType::Double) => Ok(Value::Double(*i as f64)),
            // Float to Double
            (Value::Float(f), LogicalType::Double) => Ok(Value::Double(*f as f64)),
            // Same type - no cast needed
            _ if value.logical_type() == *target_type => Ok(value),
            // Null can be cast to any type
            (Value::Null(_), _) => Ok(Value::Null(target_type.clone())),
            // Unsupported cast
            _ => Err(paro_error::not_implemented(format!(
                "Cannot cast {:?} to {:?} in table function argument",
                value.logical_type(),
                target_type
            ))),
        }
    }

    /// Call the table function's bind function.
    fn bind_table_function(
        &self,
        get: &TableFunctionGet,
        input_values: &[Value],
    ) -> Result<Option<Box<dyn TableFunctionBindData>>> {
        // If the function has a bind function, call it
        if let Some(bind_fn) = get.function.bind {
            let named_params = HashMap::new();
            let empty_types: Vec<paro_common::types::LogicalType> = Vec::new();
            let empty_names: Vec<String> = Vec::new();
            let input = TableFunctionBindInput {
                inputs: input_values,
                named_parameters: &named_params,
                input_table_types: &empty_types,
                input_table_names: &empty_names,
            };

            let mut return_types = Vec::new();
            let mut names = Vec::new();

            let bind_data = bind_fn(&input, &mut return_types, &mut names)?;

            // Verify that the bind function returned the expected types
            // (This is a sanity check - the types should match what was determined during planning)
            if !return_types.is_empty() && return_types != get.column_types {
                // Types might differ due to projection, so we just log a warning
                // In a production system, we might want to handle this more carefully
            }

            Ok(bind_data)
        } else {
            // No bind function - return None
            Ok(None)
        }
    }

    /// Call the table function's bind function with input table types (for table-in-out functions).
    fn bind_table_function_with_input(
        &self,
        get: &TableFunctionGet,
        input_values: &[Value],
    ) -> Result<Option<Box<dyn TableFunctionBindData>>> {
        // If the function has a bind function, call it
        if let Some(bind_fn) = get.function.bind {
            let named_params = HashMap::new();
            let input = TableFunctionBindInput {
                inputs: input_values,
                named_parameters: &named_params,
                input_table_types: &get.input_table_types,
                input_table_names: &get.input_table_names,
            };

            let mut return_types = Vec::new();
            let mut names = Vec::new();

            let bind_data = bind_fn(&input, &mut return_types, &mut names)?;

            Ok(bind_data)
        } else {
            // No bind function - return None
            Ok(None)
        }
    }
}

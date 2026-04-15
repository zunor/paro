//! Bind Values Node
//!
//!

use crate::binder::bind::expr;
use crate::binder::ir::BoundValues;
use crate::binder::Binder;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

impl Binder {
    pub fn bind_values_rows(
        &mut self,
        values: Vec<Vec<paro_parser::ast::Expr>>,
    ) -> Result<BoundValues> {
        if values.is_empty() {
            return Err(paro_error::syntax("VALUES must have at least one row"));
        }

        let col_count = values[0].len();
        let mut bound_values = Vec::new();

        for (row_idx, row) in values.into_iter().enumerate() {
            if row.len() != col_count {
                return Err(paro_error::syntax(format!(
                    "VALUES row {} has {} columns, expected {}",
                    row_idx,
                    row.len(),
                    col_count
                )));
            }
            let mut bound_row = Vec::new();
            for expr in row {
                bound_row.push(expr::bind_expression(self, expr)?);
            }
            bound_values.push(bound_row);
        }

        // Determine types by finding the maximum type across all rows for each column
        let mut types = Vec::with_capacity(col_count);
        let mut names = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let mut col_type = LogicalType::Unknown;
            for row in &bound_values {
                let row_col_type = match row[i].return_type() {
                    LogicalType::IntegerLiteral(_) => LogicalType::Integer,
                    LogicalType::StringLiteral => LogicalType::Varchar,
                    t => t,
                };
                col_type = LogicalType::max_logical_type(&col_type, &row_col_type);
            }

            types.push(col_type);
            names.push(format!("col{}", i));
        }

        let projection_index = self.bind_context.generate_table_index();

        Ok(BoundValues {
            projection_index,
            values: bound_values,
            names,
            types,
        })
    }
}

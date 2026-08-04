// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

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

        let mut bound = BoundValues {
            projection_index,
            values: bound_values,
            names,
            types: types.clone(),
        };
        bound.cast_rows_to_types(&types, &self.cast_functions)?;
        Ok(bound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::ir::BoundQuery;
    use crate::binder::test_utils::test_binder;
    use crate::expression::Expression;
    use paro_parser::ast::Statement;

    fn bind_values(sql: &str) -> BoundValues {
        let statement = paro_parser::parse_one(sql).expect("parse VALUES").stmt;
        let Statement::Query(query) = statement else {
            panic!("expected query statement");
        };
        let BoundQuery::Values(values) =
            test_binder().bind_query(*query).expect("bind VALUES query")
        else {
            panic!("expected bound VALUES query");
        };
        values
    }

    #[test]
    fn mixed_values_rows_are_cast_to_their_common_column_type() {
        for (sql, expected_type) in [
            ("VALUES (1), (100000000000)", LogicalType::BigInt),
            (
                "VALUES (1), (2.5)",
                LogicalType::Decimal {
                    precision: 2,
                    scale: 1,
                },
            ),
            ("VALUES (-1), (100000000000)", LogicalType::BigInt),
        ] {
            let values = bind_values(sql);
            assert_eq!(
                values.types.as_slice(),
                std::slice::from_ref(&expected_type)
            );
            assert!(values
                .values
                .iter()
                .all(|row| row[0].return_type() == expected_type));
            assert!(matches!(values.values[0][0], Expression::Cast(_)));
        }
    }

    #[test]
    fn differently_sized_array_rows_use_a_common_list_type() {
        let values = bind_values("VALUES ([1, 2, 3]), ([4, 5]), (NULL), ([])");
        let expected_type = LogicalType::List(Box::new(LogicalType::Integer));

        assert_eq!(
            values.types.as_slice(),
            std::slice::from_ref(&expected_type)
        );
        assert!(values
            .values
            .iter()
            .all(|row| row[0].return_type() == expected_type));
    }
}

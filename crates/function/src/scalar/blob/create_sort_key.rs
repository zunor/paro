use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::sort_key::encode_column;
pub use paro_common::sort_key::OrderModifiers;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::{ExpressionState, ScalarFunction};

/// Create sort key function - returns the function definition.
pub fn get_create_sort_key_function() -> ScalarFunction {
    ScalarFunction::new(
        "create_sort_key".to_string(),
        vec![],
        LogicalType::Blob,
        create_sort_key_impl,
    )
    .with_varargs(LogicalType::Varchar)
}

fn create_sort_key_impl(
    _chunk: &Chunk,
    _state: &dyn ExpressionState,
    _result: &mut Vector,
) -> Result<()> {
    Err(paro_error::not_implemented(
        "create_sort_key should be called through expression binding with proper modifiers",
    ))
}

pub fn encode_sort_key(
    chunk: &Chunk,
    row_idx: usize,
    columns: &[usize],
    modifiers: &[OrderModifiers],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    encode_sort_key_into(chunk, row_idx, columns, modifiers, &mut out)?;
    Ok(out)
}

pub fn encode_sort_key_into(
    chunk: &Chunk,
    row_idx: usize,
    columns: &[usize],
    modifiers: &[OrderModifiers],
    out: &mut Vec<u8>,
) -> Result<()> {
    if columns.len() != modifiers.len() {
        return Err(paro_error::internal(format!(
            "sort key column count mismatch: {} columns, {} modifiers",
            columns.len(),
            modifiers.len()
        )));
    }

    out.clear();
    for (&column_idx, modifiers) in columns.iter().zip(modifiers.iter().copied()) {
        let vector = chunk.column(column_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "sort key column {} out of bounds for chunk with {} columns",
                column_idx,
                chunk.column_count()
            ))
        })?;
        encode_column(vector, row_idx, modifiers, out)?;
    }
    Ok(())
}

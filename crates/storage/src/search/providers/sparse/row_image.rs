// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Sparse vector physical row image helpers.
//!
//! The long-term sparse search path consumes a typed binary row image instead
//! of parsing the user-facing text form in provider hot paths.

use crate::rowset::SparseVector;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

pub(crate) fn validate_sparse_binary_row_image_column(
    logical_type: &LogicalType,
    provider_config: &serde_json::Value,
) -> Result<()> {
    let encoding = provider_config
        .get("physical_encoding")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("binary-v1");
    match encoding {
        "binary-v1" | "typed-binary-v1" => {}
        other => {
            return Err(paro_error::invalid_input(format!(
                "unsupported sparse physical_encoding: {}",
                other
            )))
        }
    }

    if !matches!(logical_type, LogicalType::Blob) {
        return Err(paro_error::not_supported(format!(
            "Sparse index requires Blob binary sparse row image input, got {:?}",
            logical_type
        )));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn encode_sparse_row_image(vector: &SparseVector) -> Result<Vec<u8>> {
    vector.to_row_image_v1()
}

pub(crate) fn decode_sparse_row_image(bytes: &[u8]) -> Result<SparseVector> {
    SparseVector::from_row_image_v1(bytes)
}

pub(crate) fn decode_sparse_row_value(
    bytes: &[u8],
    column_id: u32,
    row_id: u32,
) -> Result<SparseVector> {
    decode_sparse_row_image(bytes).map_err(|err| {
        paro_error::data_corrupted(format!(
            "Sparse binary row image decode failed at column {} row {}: {}",
            column_id, row_id, err
        ))
    })
}

pub(crate) fn decode_sparse_runtime_value(value: Value) -> Result<Option<SparseVector>> {
    match value {
        Value::Null(_) => Ok(None),
        Value::Blob(bytes) => decode_sparse_row_image(&bytes).map(Some),
        other => Err(paro_error::data_corrupted(format!(
            "sparse row image expected Blob binary sparse row image, got {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_row_image_binary_v1_round_trips_sorted_vector() {
        let vector = SparseVector::new(vec![3, 1], vec![0.5, 1.0]).unwrap();
        let bytes = encode_sparse_row_image(&vector).unwrap();
        let restored = decode_sparse_row_image(&bytes).unwrap();
        assert_eq!(restored.dims, vec![1, 3]);
        assert_eq!(restored.weights, vec![1.0, 0.5]);
    }

    #[test]
    fn sparse_row_image_rejects_text_as_binary() {
        let err = decode_sparse_row_image(b"{1:1.0}").expect_err("text is not binary row image");
        assert!(err.to_string().contains("sparse row image"));
    }

    #[test]
    fn sparse_row_image_validation_requires_blob_binary_input() {
        validate_sparse_binary_row_image_column(&LogicalType::Blob, &serde_json::json!({}))
            .unwrap();
        validate_sparse_binary_row_image_column(
            &LogicalType::Blob,
            &serde_json::json!({ "physical_encoding": "typed-binary-v1" }),
        )
        .unwrap();

        let varchar_err =
            validate_sparse_binary_row_image_column(&LogicalType::Varchar, &serde_json::json!({}))
                .expect_err("Varchar text is not a sparse row image");
        assert!(varchar_err.to_string().contains("Blob binary"));

        let legacy_err = validate_sparse_binary_row_image_column(
            &LogicalType::Blob,
            &serde_json::json!({ "physical_encoding": "legacy-text" }),
        )
        .expect_err("legacy text encoding should be rejected");
        assert!(legacy_err.to_string().contains("physical_encoding"));
    }
}

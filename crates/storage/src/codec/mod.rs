// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared encoding/decoding between logical chunks, column batches, and storage column data.

pub(crate) mod batch_encoder;
pub(crate) mod cell_decoder;
pub(crate) mod chunk_encoder;
pub(crate) mod column_decoder;
pub(crate) mod nested_payload_codec;
pub(crate) mod physical_layout;
pub(crate) mod vector_decoder;

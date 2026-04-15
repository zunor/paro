// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::runtime_value::Value;
use paro_session::CollectingSink;

pub fn query_string_col(sink: &CollectingSink, col_idx: usize) -> Vec<String> {
    let mut out = Vec::new();
    let result = sink.assert_single_result();
    for chunk in &result.chunks {
        let col = chunk
            .column(col_idx)
            .expect("missing expected result column");
        for row in 0..chunk.len() {
            match col.get_value(row) {
                Value::Varchar(v) => out.push(v),
                other => panic!("unexpected string value: {:?}", other),
            }
        }
    }
    out
}

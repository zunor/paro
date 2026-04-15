// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::runtime_value::Value;
use paro_session::CollectingSink;

pub fn query_string_pairs(sink: &CollectingSink) -> Vec<(String, String)> {
    let result = sink.assert_single_result();
    let mut out = Vec::new();
    for chunk in &result.chunks {
        let left = chunk.column(0).expect("missing first column");
        let right = chunk.column(1).expect("missing second column");
        for row in 0..chunk.len() {
            let lhs = match left.get_value(row) {
                Value::Varchar(v) => v,
                other => panic!("unexpected left column value: {:?}", other),
            };
            let rhs = match right.get_value(row) {
                Value::Varchar(v) => v,
                other => panic!("unexpected right column value: {:?}", other),
            };
            out.push((lhs, rhs));
        }
    }
    out
}

use paro_common::runtime_value::Value;
use paro_session::CollectingSink;

pub fn query_i64_col(sink: &CollectingSink, col_idx: usize) -> Vec<i64> {
    let mut out = Vec::new();
    let result = sink.assert_single_result();
    for chunk in &result.chunks {
        let col = chunk
            .column(col_idx)
            .expect("missing expected result column");
        for row in 0..chunk.len() {
            match col.get_value(row) {
                Value::TinyInt(v) => out.push(v as i64),
                Value::SmallInt(v) => out.push(v as i64),
                Value::Integer(v) => out.push(v as i64),
                Value::BigInt(v) => out.push(v),
                Value::UBigInt(v) => out.push(v as i64),
                other => panic!("unexpected numeric value: {:?}", other),
            }
        }
    }
    out
}

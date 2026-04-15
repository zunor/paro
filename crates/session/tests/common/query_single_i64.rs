use paro_common::runtime_value::Value;
use paro_session::CollectingSink;

pub fn query_single_i64(sink: &CollectingSink) -> i64 {
    let result = sink.assert_single_result();
    let chunk = result.chunks.first().expect("expected result chunk");
    match chunk.column(0).expect("missing value").get_value(0) {
        Value::BigInt(v) => v,
        Value::Integer(v) => i64::from(v),
        other => panic!("unexpected scalar value: {:?}", other),
    }
}

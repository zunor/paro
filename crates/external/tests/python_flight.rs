// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use paro_common::allocator::DefaultAllocator;
use paro_common::error;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_external::python::{
    client::PythonClient,
    codec,
    definition::{schema_wire, source_digest, Definition},
};

fn definition(
    source: &str,
    mode: &str,
) -> (
    Definition,
    Arc<arrow_schema::Schema>,
    Arc<arrow_schema::Schema>,
) {
    let input = codec::schema(&[("arg0".into(), LogicalType::Integer)]).unwrap();
    let output = codec::schema(&[("result".into(), LogicalType::Integer)]).unwrap();
    (
        Definition {
            routine_id: 123,
            generation: 2,
            owner: "test".into(),
            source: source.into(),
            digest: source_digest(source),
            handler: "main".into(),
            mode: mode.into(),
            strict: false,
            input_schema: schema_wire(&input).unwrap(),
            output_schema: schema_wire(&output).unwrap(),
        },
        input,
        output,
    )
}

#[test]
fn real_flight_scalar_vector_and_cancellation() {
    let client = PythonClient::start().expect("start real Python Arrow worker");
    for (mode, source) in [
        ("scalar", "def main(ctx, value):\n    return None if value is None else value + 1"),
        ("vector", "import pyarrow.compute as pc\ndef main(ctx, values):\n    return pc.add(values, 1).cast(values.type)"),
    ] {
        let (definition, input, output) = definition(source, mode);
        client.validate(definition.clone()).unwrap();
        let batch = RecordBatch::try_new(input, vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(9)])) as ArrayRef]).unwrap();
        let result = client.execute(definition, batch, output, Arc::new(DefaultAllocator::new()), || Ok(())).unwrap();
        assert_eq!(result[0].get_value(0), Value::Integer(2));
        assert_eq!(result[0].get_value(1), Value::Null(LogicalType::Integer));
        assert_eq!(result[0].get_value(2), Value::Integer(10));
    }
    let (definition, input, output) = definition(
        "import time\ndef main(ctx, value):\n    time.sleep(60)\n    return value",
        "scalar",
    );
    let batch =
        RecordBatch::try_new(input, vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef]).unwrap();
    let start = Instant::now();
    let result = client.execute(
        definition,
        batch,
        output,
        Arc::new(DefaultAllocator::new()),
        || {
            if start.elapsed() > Duration::from_millis(250) {
                Err(error::query_canceled())
            } else {
                Ok(())
            }
        },
    );
    assert!(result.unwrap_err().is_query_canceled());
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[test]
fn synchronous_flight_wait_keeps_async_cancellation_runnable() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let client = PythonClient::start().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let cancelled = Arc::new(AtomicBool::new(false));
        let signal = cancelled.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            signal.store(true, Ordering::Release);
        });
        let start = Instant::now();
        let call = tokio::spawn(async move {
            let (definition, input, output) = definition(
                "import time\ndef main(ctx, values):\n    time.sleep(2)\n    return values",
                "vector",
            );
            let batch =
                RecordBatch::try_new(input, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
            client.execute(
                definition,
                batch,
                output,
                Arc::new(DefaultAllocator::new()),
                || {
                    if cancelled.load(Ordering::Acquire) {
                        Err(error::query_canceled())
                    } else {
                        Ok(())
                    }
                },
            )
        });
        assert!(call.await.unwrap().unwrap_err().is_query_canceled());
        assert!(start.elapsed() < Duration::from_secs(2));
        timer.await.unwrap();
    });
}

#[test]
fn concurrent_calls_queue_at_the_owner_limit() {
    concurrent_calls(8, 1);
}

#[test]
fn concurrent_owners_queue_at_the_global_limit() {
    concurrent_calls(16, 4);
}

fn concurrent_calls(calls: usize, owners: usize) {
    let client = Arc::new(PythonClient::start().unwrap());
    let barrier = Arc::new(std::sync::Barrier::new(calls));
    let handles = (0..calls)
        .map(|index| {
            let client = client.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let (mut definition, input, output) = definition(
                    "import time\ndef main(ctx, values):\n    time.sleep(0.1)\n    return values",
                    "vector",
                );
                definition.owner = format!("owner-{}", index % owners);
                let batch = RecordBatch::try_new(
                    input,
                    vec![Arc::new(Int32Array::from(vec![index as i32])) as ArrayRef],
                )
                .unwrap();
                barrier.wait();
                client.validate(definition.clone()).unwrap();
                let result = client
                    .execute(
                        definition,
                        batch,
                        output,
                        Arc::new(DefaultAllocator::new()),
                        || Ok(()),
                    )
                    .unwrap();
                assert_eq!(result[0].get_value(0), Value::Integer(index as i32));
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn cancellation_while_queued_never_starts_a_handler() {
    let directory = tempfile::tempdir().unwrap();
    let release = directory.path().join("release");
    let source = format!(
        "import time\nfrom pathlib import Path\ndef main(ctx, values):\n    Path({} + '/' + str(values[0].as_py())).touch()\n    while not Path({}).exists():\n        time.sleep(0.01)\n    return values",
        serde_json::to_string(&directory.path().to_string_lossy()).unwrap(),
        serde_json::to_string(&release.to_string_lossy()).unwrap(),
    );
    let client = Arc::new(PythonClient::start().unwrap());
    let handles = (0..4)
        .map(|index| {
            let client = client.clone();
            let source = source.clone();
            std::thread::spawn(move || {
                let (definition, input, output) = definition(&source, "vector");
                let batch =
                    RecordBatch::try_new(input, vec![Arc::new(Int32Array::from(vec![index]))])
                        .unwrap();
                client.execute(
                    definition,
                    batch,
                    output,
                    Arc::new(DefaultAllocator::new()),
                    || Ok(()),
                )
            })
        })
        .collect::<Vec<_>>();
    let start = Instant::now();
    while !(0..4).all(|index| directory.path().join(index.to_string()).exists())
        && start.elapsed() < Duration::from_secs(5)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let all_started = (0..4).all(|index| directory.path().join(index.to_string()).exists());
    let (definition, input, output) = definition(&source, "vector");
    let batch = RecordBatch::try_new(input, vec![Arc::new(Int32Array::from(vec![99]))]).unwrap();
    let start = Instant::now();
    let result = client.execute(
        definition,
        batch,
        output,
        Arc::new(DefaultAllocator::new()),
        || {
            if start.elapsed() > Duration::from_millis(100) {
                Err(error::query_canceled())
            } else {
                Ok(())
            }
        },
    );
    // Unblock all owned handlers even when an assertion below fails.
    std::fs::write(release, []).unwrap();
    for handle in handles {
        handle.join().unwrap().unwrap();
    }
    assert!(all_started, "owner capacity was not saturated");
    assert!(result.unwrap_err().is_query_canceled());
    assert!(
        !directory.path().join("99").exists(),
        "queued cancellation ran user code"
    );
    assert!(client.is_running());
}

#[test]
fn decode_failure_cancels_and_reaps_before_capacity_is_reused() {
    use paro_common::allocator::Allocator;
    struct RejectAllocation;
    impl Allocator for RejectAllocation {
        fn allocate(&self, _: usize) -> error::Result<*mut u8> {
            Err(error::out_of_memory("injected allocation failure"))
        }
        fn allocate_zeroed(&self, size: usize) -> error::Result<*mut u8> {
            self.allocate(size)
        }
        fn reallocate(&self, _: *mut u8, _: usize, size: usize) -> error::Result<*mut u8> {
            self.allocate(size)
        }
        fn free(&self, _: *mut u8, _: usize) {
            panic!("no allocation should succeed")
        }
    }
    let client = PythonClient::start().unwrap();
    let (definition, input, output) =
        definition("def main(ctx, values):\n    return values", "vector");
    // More failures than the server's owner limit expose leaked invocation slots.
    for _ in 0..6 {
        let batch =
            RecordBatch::try_new(input.clone(), vec![Arc::new(Int32Array::from(vec![7]))]).unwrap();
        let error = client
            .execute(
                definition.clone(),
                batch,
                output.clone(),
                Arc::new(RejectAllocation),
                || Ok(()),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("injected allocation failure"),
            "{error}"
        );
        assert!(client.is_running());
    }
    let batch = RecordBatch::try_new(input, vec![Arc::new(Int32Array::from(vec![7]))]).unwrap();
    assert_eq!(
        client
            .execute(
                definition,
                batch,
                output,
                Arc::new(DefaultAllocator::new()),
                || Ok(())
            )
            .unwrap()[0]
            .get_value(0),
        Value::Integer(7)
    );
}

#[test]
fn invalid_metadata_and_eager_nested_capacity_fail_without_panicking() {
    use arrow_schema::{DataType, Field, Schema};
    let allocator = Arc::new(DefaultAllocator::new());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Int32,
        true,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    assert!(codec::decode(&batch, &schema, allocator.clone())
        .unwrap_err()
        .to_string()
        .contains("metadata"));

    // One empty list has almost no Arrow data, but the native eager constructor
    // would otherwise reserve a billion fixed-array elements for its child.
    let ty = LogicalType::List(Box::new(LogicalType::Array(
        Box::new(LogicalType::Integer),
        1_000_000_000,
    )));
    let schema = codec::schema(&[("result".into(), ty.clone())]).unwrap();
    let values = codec::array(
        &ty,
        &[Value::List(
            vec![],
            LogicalType::Array(Box::new(LogicalType::Integer), 1_000_000_000),
        )],
    )
    .unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
    assert!(codec::decode(&batch, &schema, allocator)
        .unwrap_err()
        .to_string()
        .contains("budget"));
}

#[test]
fn native_arrow_nested_and_decimal_roundtrip() {
    use LogicalType as L;
    let cases = vec![
        (L::HugeInt, Value::HugeInt(i128::MIN)),
        (L::UHugeInt, Value::UHugeInt(u128::MAX)),
        (
            L::Decimal {
                precision: 38,
                scale: 9,
            },
            Value::Decimal(123456789, 38, 9),
        ),
        (L::Uuid, Value::Uuid(u128::MAX)),
        (L::Interval, Value::Interval(3, 2, i64::MAX)),
        (
            L::List(Box::new(L::Integer)),
            Value::List(vec![Value::Integer(1), Value::Null(L::Integer)], L::Integer),
        ),
        (
            L::Struct(vec![("a".into(), L::Integer)]),
            Value::Struct(vec![Value::Integer(42)], vec![("a".into(), L::Integer)]),
        ),
        (
            L::Array(Box::new(L::Integer), 2),
            Value::Array(
                vec![Value::Integer(7), Value::Null(L::Integer)],
                L::Integer,
                2,
            ),
        ),
    ];
    for (ty, expected) in cases {
        let schema = codec::schema(&[("result".into(), ty.clone())]).unwrap();
        let array = codec::array(
            &ty,
            &[expected.clone(), expected.clone(), Value::Null(ty.clone())],
        )
        .unwrap()
        .slice(1, 2);
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let vectors = codec::decode(&batch, &schema, Arc::new(DefaultAllocator::new())).unwrap();
        assert_eq!(vectors[0].get_value(0), expected);
        assert_eq!(vectors[0].get_value(1), Value::Null(ty));
    }
}

#[test]
fn invalid_native_values_and_expanded_constants_are_rejected() {
    use paro_common::chunk::Chunk;
    use paro_common::vector::Vector;

    let allocator = Arc::new(DefaultAllocator::new());
    for (ty, array) in [
        (
            LogicalType::Decimal {
                precision: 2,
                scale: 0,
            },
            Arc::new(
                arrow_array::Decimal128Array::from(vec![100])
                    .with_precision_and_scale(2, 0)
                    .unwrap(),
            ) as ArrayRef,
        ),
        (
            LogicalType::Json,
            Arc::new(arrow_array::StringArray::from(vec!["not json"])) as ArrayRef,
        ),
        (
            LogicalType::Time,
            Arc::new(arrow_array::Time64MicrosecondArray::from(vec![-1])) as ArrayRef,
        ),
    ] {
        let schema = codec::schema(&[("result".into(), ty)]).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        assert!(codec::decode(&batch, &schema, allocator.clone()).is_err());
    }

    let ty = LogicalType::Varchar;
    let mut value = Vector::try_new(ty.clone(), 1, allocator.clone()).unwrap();
    value
        .try_set_scalar_value(0, &Value::Varchar("x".repeat(4096)))
        .unwrap();
    value.try_set_count(1).unwrap();
    let constant = value.to_constant(65536);
    let chunk = Chunk::from_vectors(vec![constant], allocator);
    let schema = codec::schema(&[("arg0".into(), ty)]).unwrap();
    assert!(codec::encode(&chunk, schema)
        .unwrap_err()
        .to_string()
        .contains("budget"));
}

#[test]
fn null_fixed_arrays_are_bounded_before_padding_is_allocated() {
    use paro_common::{chunk::Chunk, vector::Vector};
    let allocator = Arc::new(DefaultAllocator::new());
    let ty = LogicalType::Array(Box::new(LogicalType::Integer), 1_000_000);
    let vector = Vector::try_constant_null(ty.clone(), 1, allocator.clone()).unwrap();
    let chunk = Chunk::from_vectors(vec![vector], allocator);
    let schema = codec::schema(&[("arg0".into(), ty)]).unwrap();
    assert!(codec::encode(&chunk, schema)
        .unwrap_err()
        .to_string()
        .contains("budget"));
}

#[test]
fn nested_and_wide_values_cross_both_ipc_boundaries() {
    use LogicalType as L;
    let cases = [
        (L::HugeInt, Value::HugeInt(i128::MIN)),
        (L::UHugeInt, Value::UHugeInt(u128::MAX)),
        (
            L::Decimal {
                precision: 38,
                scale: 9,
            },
            Value::Decimal(123456789, 38, 9),
        ),
        (L::Interval, Value::Interval(-1, 2, i64::MAX)),
        (L::Uuid, Value::Uuid(u128::MAX)),
        (
            L::Array(Box::new(L::Integer), 2),
            Value::Array(
                vec![Value::Integer(7), Value::Null(L::Integer)],
                L::Integer,
                2,
            ),
        ),
        (
            L::List(Box::new(L::Integer)),
            Value::List(vec![Value::Integer(1), Value::Null(L::Integer)], L::Integer),
        ),
        (
            L::Struct(vec![("a".into(), L::Integer)]),
            Value::Struct(vec![Value::Integer(42)], vec![("a".into(), L::Integer)]),
        ),
    ];
    let names = (0..cases.len())
        .map(|index| format!("arg{index}"))
        .collect::<Vec<_>>();
    let schema = codec::schema(
        &names
            .iter()
            .zip(&cases)
            .map(|(name, (ty, _))| (name.clone(), ty.clone()))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let columns = cases
        .iter()
        .map(|(ty, value)| codec::array(ty, &[value.clone(), Value::Null(ty.clone())]).unwrap())
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let arguments = names.join(", ");
    let source = format!(
        "import pyarrow as pa\ndef main(ctx, {arguments}):\n    return pa.RecordBatch.from_arrays([{arguments}], names={})",
        serde_json::to_string(&names).unwrap(),
    );
    let definition = Definition {
        routine_id: 124,
        generation: 1,
        owner: "typed-test".into(),
        digest: source_digest(&source),
        source,
        handler: "main".into(),
        mode: "table".into(),
        strict: false,
        input_schema: schema_wire(&schema).unwrap(),
        output_schema: schema_wire(&schema).unwrap(),
    };
    let client = PythonClient::start().unwrap();
    let result = client
        .execute(
            definition,
            batch,
            schema,
            Arc::new(DefaultAllocator::new()),
            || Ok(()),
        )
        .unwrap();
    for (column, (ty, expected)) in result.iter().zip(cases) {
        assert_eq!(column.get_value(0), expected);
        assert_eq!(column.get_value(1), Value::Null(ty));
    }
}

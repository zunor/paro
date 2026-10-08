// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use array_casts::{ArrayBoundCastData, ListBoundCastData};
use struct_casts::StructBoundCastData;

fn kernel(_: &Vector, _: &mut Vector, _: usize, _: &CastExecCtx<'_>) -> Result<bool> {
    Ok(true)
}

#[test]
fn executable_cast_identity_keeps_context_dispatch_and_nested_roles() {
    let plain = BoundCastInfo::fixed(kernel);
    assert!(plain.execution_semantics_equal(&plain.clone()));
    assert!(!plain.execution_semantics_equal(&plain.clone().requiring_runtime_context()));
    assert!(!plain.execution_semantics_equal(&BoundCastInfo::varlen(kernel)));
    let nested = |data: Arc<dyn BoundCastData>| BoundCastInfo::array_with_data(kernel, data);
    let left = nested(Arc::new(ArrayBoundCastData::new(plain.clone())));
    let right = nested(Arc::new(ArrayBoundCastData::new(plain.clone())));
    assert!(left.execution_semantics_equal(&right));
    assert!(
        !left.execution_semantics_equal(&nested(Arc::new(ListBoundCastData {
            child_cast_info: plain.clone()
        })))
    );
    assert!(
        !left.execution_semantics_equal(&nested(Arc::new(ArrayBoundCastData::new(
            plain.requiring_runtime_context()
        ))))
    );
    let registry = CastFunctionSet::new();
    let integer = registry
        .get_cast_function(&LogicalType::Integer, &LogicalType::Integer)
        .unwrap();
    let bigint = registry
        .get_cast_function(&LogicalType::BigInt, &LogicalType::BigInt)
        .unwrap();
    assert!(!integer.execution_semantics_equal(&bigint));
}

#[test]
fn shared_nested_cast_identity_visits_a_linear_dag_not_exponentially_many_paths() {
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(|| {
            let chain = || {
                let mut casts = vec![BoundCastInfo::fixed(kernel)];
                for _ in 0..10_000 {
                    let child = casts.last().unwrap().clone();
                    casts.push(BoundCastInfo::struct_with_data(
                        kernel,
                        Arc::new(StructBoundCastData {
                            field_casts: vec![child.clone(), child],
                        }),
                    ));
                }
                casts
            };
            let mut left = chain();
            let mut right = chain();
            assert!(left
                .last()
                .unwrap()
                .execution_semantics_equal(right.last().unwrap()));
            // This test exercises identity, not the independently recursive cast
            // metadata destructor. Retained ancestors keep teardown bounded too.
            while left.pop().is_some() {}
            while right.pop().is_some() {}
        })
        .unwrap()
        .join()
        .unwrap();
}

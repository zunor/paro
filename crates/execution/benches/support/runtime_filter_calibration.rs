// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Opt-in calibration cells; use the existing structured benchmark timer.
//! Input construction is outside timing. Each build pair differs only in RF.

use super::*;
use paro_common::runtime_value::Value;
use paro_planner::physical::{
    Fingerprint, HashJoinRuntimeFilterSpec, RuntimeFilterResourceContract, RuntimeFilterWaitPolicy,
};
use paro_storage::index::{FixedMembership, FixedMembershipBuildPolicy, Predicate, PredicateTree};
use paro_storage::rowset::page::CompressionType;
use paro_storage::rowset::segment::{
    ColumnData, Segment, SegmentIterator, SegmentOptions, SegmentWriter, SegmentWriterOptions,
};
use paro_storage::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};

pub(super) fn collect(
    selected: &Option<Vec<String>>,
    samples: usize,
    output: &mut Vec<serde_json::Value>,
) {
    if !selected
        .as_ref()
        .is_some_and(|ids| ids.iter().any(|id| id == "runtime_filter_calibration"))
    {
        return;
    }
    for rows in [8192, 65536, 262144] {
        for (domain, step, shuffled, ndv) in [
            ("range", 1, false, rows),
            ("dense", 2, true, rows),
            ("sorted", 1024, true, rows),
            ("duplicates", 2, true, 1024),
        ] {
            for keys in [1, 2] {
                let step = if domain == "sorted" {
                    (1 << 30) / rows as i32
                } else {
                    step
                };
                let inputs = build_inputs(rows, ndv, step, shuffled);
                // Four task-local states, merged through the production sink.
                let spec = HashJoinRuntimeFilterSpec {
                    artifact: Fingerprint(1),
                    wait_policy: RuntimeFilterWaitPolicy::WaitComplete,
                    condition_indices: (0..keys).collect::<Vec<_>>().into_boxed_slice(),
                    resource: RuntimeFilterResourceContract::for_probe_rows(
                        &vec![LogicalType::Integer; keys],
                        4,
                        4_194_304,
                    )
                    .unwrap(),
                };
                for (block, enabled) in [false, true, true, false].into_iter().enumerate() {
                    let mut state = HashJoinBuildFinishBench::with_filter(
                        enabled.then(|| spec.clone()),
                        keys,
                        4,
                    );
                    state.inputs = inputs
                        .iter()
                        .map(Chunk::clone_referencing_vectors)
                        .collect::<Vec<_>>()
                        .into_boxed_slice();
                    let id = format!("rf_build/{domain}/{rows}/{keys}/{block}/{enabled}");
                    let mut result =
                        measure_structured_bench(&id, rows * keys, samples, || state.run_once());
                    result["rows"] = rows.into();
                    result["keys"] = keys.into();
                    result["enabled"] = enabled.into();
                    output.push(result);
                }
            }
        }
    }
    for rows in [65536, 262144] {
        for domain in ["dense", "sorted", "range"] {
            for stride in [1, 10] {
                let state = ScanBench::new(rows, domain, stride);
                let mut reference = HashJoinBuildFinishBench::with_filter(None, 1, 4);
                reference.inputs = build_inputs(rows, rows, 2, true).into_boxed_slice();
                output.push(measure_structured_bench(
                    &format!("rf_apply_reference/{domain}/{rows}/{stride}/before"),
                    rows,
                    samples,
                    || reference.run_once(),
                ));
                for (block, enabled) in [false, true, true, false].into_iter().enumerate() {
                    let id = format!("rf_apply/{domain}/{rows}/{stride}/{block}/{enabled}");
                    let mut result =
                        measure_structured_bench(&id, rows.div_ceil(stride), samples, || {
                            state.run_once(enabled)
                        });
                    result["rows"] = rows.into();
                    result["survivor_stride"] = stride.into();
                    result["enabled"] = enabled.into();
                    output.push(result);
                }
                output.push(measure_structured_bench(
                    &format!("rf_apply_reference/{domain}/{rows}/{stride}/after"),
                    rows,
                    samples,
                    || reference.run_once(),
                ));
            }
        }
    }
}

fn build_inputs(rows: usize, ndv: usize, step: i32, shuffled: bool) -> Vec<Chunk> {
    (0..rows / VECTOR_SIZE)
        .map(|chunk| {
            let values = (0..VECTOR_SIZE)
                .map(|offset| {
                    let row = chunk * VECTOR_SIZE + offset;
                    let value = if shuffled {
                        row.wrapping_mul(104729).wrapping_add(17) % ndv
                    } else {
                        row % ndv
                    };
                    value as i32 * step
                })
                .collect::<Vec<_>>();
            Chunk::from_vectors(
                vec![
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &values,
                        bench_allocator(),
                    ),
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &values,
                        bench_allocator(),
                    ),
                ],
                bench_allocator(),
            )
        })
        .collect()
}

struct ScanBench {
    _directory: tempfile::TempDir,
    segment: Arc<Segment>,
    predicate: PredicateTree,
    baseline: Option<PredicateTree>,
    expected: usize,
    candidates: usize,
    stride: usize,
}

impl ScanBench {
    fn new(rows: usize, domain: &str, stride: usize) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("probe.seg");
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![
                    TabletColumn::new(0, "gate", LogicalType::Integer),
                    TabletColumn::new(1, "key", LogicalType::Integer),
                ],
                KeysType::DuplicateKeys,
            )
            .unwrap(),
        );
        let mut writer = SegmentWriter::create(
            schema.clone(),
            &path,
            SegmentWriterOptions::new(0).with_compression(CompressionType::None),
        )
        .unwrap();
        // Every non-null RF lookup hits. One null per vector prevents zone maps
        // proving the range predicate globally true. The control emits these
        // extra nulls (less than 0.11% of candidates); retain that small output
        // difference rather than timing a different predicate as the control.
        // The gate selects a 1/stride subset for later-stage measurements.
        let keys = (0..rows)
            .map(|row| ((row * 104729 + 17) % rows * 2) as i32)
            .collect::<Vec<_>>();
        let mut nulls = vec![0u8; rows.div_ceil(8)];
        for row in (0..rows).step_by(VECTOR_SIZE) {
            nulls[row / 8] |= 1 << (row % 8);
        }
        writer
            .append_chunk(&[
                ColumnData::new(
                    (0..rows)
                        .flat_map(|row| ((row % stride) as i32).to_le_bytes())
                        .collect::<Vec<_>>(),
                    rows as u32,
                ),
                ColumnData::with_nulls(
                    keys.iter()
                        .flat_map(|key| key.to_le_bytes())
                        .collect::<Vec<_>>(),
                    nulls,
                    rows as u32,
                ),
            ])
            .unwrap();
        writer.finalize().unwrap();
        let segment = Arc::new(
            Segment::open(
                0,
                &path,
                schema,
                SegmentOptions::default().with_verify_checksum(false),
                0,
                0,
                0,
            )
            .unwrap(),
        );
        let membership = if domain == "range" {
            PredicateTree::leaf(Predicate::Range {
                column_id: 1,
                lower: Value::Integer(0),
                upper: Value::Integer((rows * 2) as i32),
            })
        } else {
            PredicateTree::leaf(Predicate::FixedIn {
                column_id: 1,
                values: FixedMembership::i32_with_policy(
                    keys,
                    FixedMembershipBuildPolicy::new(
                        if domain == "dense" { 1 << 26 } else { 0 },
                        rows,
                    ),
                ),
            })
        };
        let gate = (stride > 1).then(|| {
            PredicateTree::leaf(Predicate::Eq {
                column_id: 0,
                value: Value::Integer(0),
            })
        });
        let predicate = match gate.clone() {
            Some(gate) => PredicateTree::and([gate, membership]).unwrap(),
            None => membership,
        };
        let baseline = gate;
        Self {
            _directory: directory,
            segment,
            predicate,
            baseline,
            candidates: rows.div_ceil(stride),
            expected: (0..rows)
                .filter(|row| row % stride == 0 && row % VECTOR_SIZE != 0)
                .count(),
            stride,
        }
    }

    fn run_once(&self, enabled: bool) -> usize {
        let predicate = if enabled {
            Some(self.predicate.clone())
        } else {
            self.baseline.clone()
        };
        let mut iterator =
            SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
                &self.segment,
                vec![0, 1],
                if self.stride > 1 { vec![0, 1] } else { vec![1] },
                None,
                predicate,
                None,
            )
            .unwrap();
        let mut count = 0;
        while iterator.has_next() {
            let (rowids, columns) = iterator.next_batch(VECTOR_SIZE).unwrap();
            count += rowids.len();
            divan::black_box(columns);
            if rowids.is_empty() {
                break;
            }
        }
        assert_eq!(
            count,
            if enabled {
                self.expected
            } else {
                self.candidates
            }
        );
        count
    }
}

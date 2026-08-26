// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::durability::PreparedCommitPlan;
use paro_common::effect::StorageCommitOp;
use paro_common::error as paro_error;
use paro_common::error::Result;
use paro_common::journal::{
    JournalArtifactDescriptor, JournalArtifactKind, JournalRecord, JournalRecordMetadata,
    COMMIT_RECORD_VERSION, JOURNAL_FORMAT_VERSION, JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION,
    JOURNAL_RECORD_METADATA_VERSION, MAINTENANCE_RECORD_VERSION,
};
use serde::Serialize;
use std::fmt;

pub const JOURNAL_FRAME_HEADER_SIZE: usize = 18;
pub const COMMIT_BATCH_BYTES_ESTIMATE_RATIO_EWMA_ALPHA: f64 = 0.125;
pub const COMMIT_BATCH_BYTES_ESTIMATE_RATIO_CLAMP_MIN: f64 = 0.25;
pub const COMMIT_BATCH_BYTES_ESTIMATE_RATIO_CLAMP_MAX: f64 = 4.0;
pub const COMMIT_BATCH_BYTES_ESTIMATE_TYPICAL_MIN_RATIO: f64 = 0.75;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecSizeOverflow {
    pub encoded_size: u64,
    pub max_size: u64,
}

impl CodecSizeOverflow {
    #[inline]
    const fn new(encoded_size: u64) -> Self {
        Self {
            encoded_size,
            max_size: u32::MAX as u64,
        }
    }
}

impl fmt::Display for CodecSizeOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "journal record frame size {} exceeds maximum {}",
            self.encoded_size, self.max_size
        )
    }
}

impl std::error::Error for CodecSizeOverflow {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecSizeCalibrationSample {
    pub estimated_bytes: u32,
    pub actual_bytes: u32,
}

impl CodecSizeCalibrationSample {
    #[inline]
    pub fn actual_to_estimate_ratio(self) -> f64 {
        if self.estimated_bytes == 0 {
            return 1.0;
        }
        self.actual_bytes as f64 / self.estimated_bytes as f64
    }

    #[inline]
    pub fn estimate_to_actual_ratio(self) -> f64 {
        if self.actual_bytes == 0 {
            return 1.0;
        }
        self.estimated_bytes as f64 / self.actual_bytes as f64
    }

    #[inline]
    pub fn clamped_actual_to_estimate_ratio(self) -> f64 {
        self.actual_to_estimate_ratio().clamp(
            COMMIT_BATCH_BYTES_ESTIMATE_RATIO_CLAMP_MIN,
            COMMIT_BATCH_BYTES_ESTIMATE_RATIO_CLAMP_MAX,
        )
    }
}

/// Binary frame header prepended to each durable journal payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalFrameHeader {
    pub format_version: u16,
    pub payload_len: u32,
    pub lsn: u64,
    pub checksum: u32,
}

impl JournalFrameHeader {
    pub fn new(payload_len: u32, lsn: u64, checksum: u32) -> Self {
        Self {
            format_version: JOURNAL_FORMAT_VERSION,
            payload_len,
            lsn,
            checksum,
        }
    }

    pub fn encode(self) -> [u8; JOURNAL_FRAME_HEADER_SIZE] {
        let mut bytes = [0u8; JOURNAL_FRAME_HEADER_SIZE];
        bytes[0..2].copy_from_slice(&self.format_version.to_le_bytes());
        bytes[2..6].copy_from_slice(&self.payload_len.to_le_bytes());
        bytes[6..14].copy_from_slice(&self.lsn.to_le_bytes());
        bytes[14..18].copy_from_slice(&self.checksum.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < JOURNAL_FRAME_HEADER_SIZE {
            return Err(paro_error::serialization_error(format!(
                "journal frame too short: expected at least {} bytes, got {}",
                JOURNAL_FRAME_HEADER_SIZE,
                bytes.len()
            )));
        }

        Ok(Self {
            format_version: u16::from_le_bytes(bytes[0..2].try_into().unwrap()),
            payload_len: u32::from_le_bytes(bytes[2..6].try_into().unwrap()),
            lsn: u64::from_le_bytes(bytes[6..14].try_into().unwrap()),
            checksum: u32::from_le_bytes(bytes[14..18].try_into().unwrap()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedJournalFrame {
    pub header: JournalFrameHeader,
    pub record: JournalRecord,
}

pub fn encode_record(record: &JournalRecord, lsn: u64) -> Result<Vec<u8>> {
    let payload = bincode::serialize(record)
        .map_err(|err| paro_error::serialization_error(format!("journal encode: {err}")))?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| paro_error::serialization_error("journal payload exceeds u32 length"))?;
    let frame_len = JOURNAL_FRAME_HEADER_SIZE
        .checked_add(payload.len())
        .ok_or_else(|| paro_error::serialization_error("journal frame length overflow"))?;
    u32::try_from(frame_len)
        .map_err(|_| paro_error::serialization_error("journal frame exceeds u32 length"))?;
    let checksum = crc32c::crc32c(&payload);
    let header = JournalFrameHeader::new(payload_len, lsn, checksum);
    let mut frame = Vec::with_capacity(frame_len);
    frame.extend_from_slice(&header.encode());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Builds an estimate/actual calibration sample for diagnostics and benchmarks.
///
/// This function serializes the record to measure actual frame bytes and must
/// stay off the commit prepare hot path.
pub fn codec_size_calibration_sample_for_record(
    record: &JournalRecord,
    lsn: u64,
) -> Result<CodecSizeCalibrationSample> {
    let estimated_bytes = encoded_journal_record_size_upper_bound(record)
        .map_err(|err| paro_error::serialization_error(err.to_string()))?;
    let actual_bytes = u32::try_from(encode_record(record, lsn)?.len())
        .map_err(|_| paro_error::serialization_error("journal frame exceeds u32 length"))?;
    Ok(CodecSizeCalibrationSample {
        estimated_bytes,
        actual_bytes,
    })
}

/// Builds a prepared-plan estimate/actual calibration sample.
///
/// Unlike `encoded_size_upper_bound_for_plan`, this consumes a cloned plan to
/// materialize the durable record for calibration. Do not call it from
/// production commit admission or sequencing.
pub fn codec_size_calibration_sample_for_plan(
    plan: &PreparedCommitPlan,
    commit_id: u64,
    lsn: u64,
) -> Result<CodecSizeCalibrationSample> {
    let estimated_bytes = encoded_size_upper_bound_for_plan(plan)
        .map_err(|err| paro_error::serialization_error(err.to_string()))?;
    let record = JournalRecord::Commit(plan.clone().into_record(commit_id));
    let actual_bytes = u32::try_from(encode_record(&record, lsn)?.len())
        .map_err(|_| paro_error::serialization_error("journal frame exceeds u32 length"))?;
    Ok(CodecSizeCalibrationSample {
        estimated_bytes,
        actual_bytes,
    })
}

/// Returns an upper bound for the encoded journal frame length, including header bytes.
pub fn encoded_journal_record_size_upper_bound(
    record: &JournalRecord,
) -> std::result::Result<u32, CodecSizeOverflow> {
    encoded_frame_size_upper_bound(serialized_size_upper_bound(record)?)
}

/// Returns an upper bound for the durable commit record frame a prepared plan will produce.
///
/// The estimator walks borrowed plan payloads with bincode's size estimator instead of
/// serializing into a temporary buffer. The unknown commit id is represented as `u64::MAX`
/// so variable-width integer encodings remain bounded by the worst case.
pub fn encoded_size_upper_bound_for_plan(
    plan: &PreparedCommitPlan,
) -> std::result::Result<u32, CodecSizeOverflow> {
    let metadata = JournalRecordMetadata::default();
    let metadata_base_size = serialized_size_upper_bound(&metadata)?;
    let metadata_bound = metadata_size_upper_bound(plan, metadata_base_size)?;
    let record = BorrowedJournalRecord::Commit(BorrowedCommitRecord {
        record_version: COMMIT_RECORD_VERSION,
        metadata,
        txn_id: plan.txn_id,
        start_time: plan.start_time,
        commit_id: u64::MAX,
        catalog_ops: &plan.catalog_ops,
        storage_ops: &plan.storage_ops,
        apply_descriptors: &plan.apply_descriptors,
        deferred_tasks: &plan.deferred_tasks,
    });
    let record_size_with_empty_metadata = serialized_size_upper_bound(&record)?;
    let record_size_bound = record_size_with_empty_metadata
        .checked_add(metadata_bound.saturating_sub(metadata_base_size))
        .ok_or_else(|| CodecSizeOverflow::new(u64::MAX))?;
    encoded_frame_size_upper_bound(record_size_bound)
}

fn serialized_size_upper_bound(
    value: &impl Serialize,
) -> std::result::Result<u64, CodecSizeOverflow> {
    bincode::serialized_size(value).map_err(|_| CodecSizeOverflow::new(u64::MAX))
}

fn encoded_frame_size_upper_bound(
    payload_size: u64,
) -> std::result::Result<u32, CodecSizeOverflow> {
    let encoded_size = payload_size
        .checked_add(JOURNAL_FRAME_HEADER_SIZE as u64)
        .ok_or_else(|| CodecSizeOverflow::new(u64::MAX))?;
    u32::try_from(encoded_size).map_err(|_| CodecSizeOverflow::new(encoded_size))
}

fn metadata_size_upper_bound(
    plan: &PreparedCommitPlan,
    metadata_base_size: u64,
) -> std::result::Result<u64, CodecSizeOverflow> {
    // Catalog/apply inputs can project variable-sized object keys, schema
    // dependency names, and artifact descriptors into metadata. Use their
    // borrowed encoded size as conservative slack without building metadata or
    // re-serializing storage payloads. Deferred tasks only change scalar replay
    // counts/checksums, so they do not need variable-size metadata slack here.
    let catalog_metadata_slack = serialized_size_upper_bound(&plan.catalog_ops)?;
    let apply_metadata_slack = serialized_size_upper_bound(&plan.apply_descriptors)?;
    let tablet_id_size = serialized_size_upper_bound(&0u64)?;
    let artifact_descriptor_size = serialized_size_upper_bound(&JournalArtifactDescriptor {
        tablet_id: Some(u64::MAX),
        artifact_id: u64::MAX,
        kind: JournalArtifactKind::ApplyDescriptor,
        descriptor_checksum_crc32c: u32::MAX,
    })?;
    let storage_op_count = plan.storage_ops.len() as u64;
    let storage_mutation_count = storage_mutation_count(&plan.storage_ops) as u64;

    checked_sum(&[
        metadata_base_size,
        catalog_metadata_slack,
        apply_metadata_slack,
        checked_product(tablet_id_size, storage_op_count)?,
        checked_product(artifact_descriptor_size, storage_mutation_count)?,
    ])
}

fn storage_mutation_count(storage_ops: &[StorageCommitOp]) -> usize {
    storage_ops
        .iter()
        .map(|op| match op {
            StorageCommitOp::Tablet(tablet) => tablet.mutations.len(),
        })
        .sum()
}

fn checked_sum(values: &[u64]) -> std::result::Result<u64, CodecSizeOverflow> {
    values.iter().try_fold(0u64, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| CodecSizeOverflow::new(u64::MAX))
    })
}

fn checked_product(left: u64, right: u64) -> std::result::Result<u64, CodecSizeOverflow> {
    left.checked_mul(right)
        .ok_or_else(|| CodecSizeOverflow::new(u64::MAX))
}

// `JournalRecord::Commit` is the first variant, and bincode encodes enum tags
// with a fixed-width discriminant, so this borrowed mirror keeps the same frame
// tag size without owning the durable payload.
#[derive(Serialize)]
enum BorrowedJournalRecord<'a> {
    Commit(BorrowedCommitRecord<'a>),
}

#[derive(Serialize)]
struct BorrowedCommitRecord<'a> {
    record_version: u16,
    metadata: JournalRecordMetadata,
    txn_id: u64,
    start_time: u64,
    commit_id: u64,
    catalog_ops: &'a [paro_common::effect::CatalogTxnOp],
    storage_ops: &'a [paro_common::effect::StorageCommitOp],
    apply_descriptors: &'a [paro_common::effect::ApplyDescriptor],
    deferred_tasks: &'a [paro_common::effect::DeferredTask],
}

pub fn decode_frame(frame: &[u8]) -> Result<DecodedJournalFrame> {
    let header = JournalFrameHeader::decode(frame)?;
    if header.format_version != JOURNAL_FORMAT_VERSION {
        return Err(paro_error::serialization_error(format!(
            "unsupported journal frame version {}, expected {}",
            header.format_version, JOURNAL_FORMAT_VERSION
        )));
    }

    let payload_len = header.payload_len as usize;
    let expected_len = JOURNAL_FRAME_HEADER_SIZE
        .checked_add(payload_len)
        .ok_or_else(|| paro_error::serialization_error("journal frame length overflow"))?;
    if frame.len() != expected_len {
        return Err(paro_error::serialization_error(format!(
            "journal frame length mismatch: header payload_len={} but frame has {} payload bytes",
            payload_len,
            frame.len().saturating_sub(JOURNAL_FRAME_HEADER_SIZE)
        )));
    }

    let payload = &frame[JOURNAL_FRAME_HEADER_SIZE..];
    let computed_checksum = crc32c::crc32c(payload);
    if computed_checksum != header.checksum {
        return Err(paro_error::serialization_error(format!(
            "journal checksum mismatch: stored={}, computed={}",
            header.checksum, computed_checksum
        )));
    }

    let record: JournalRecord = bincode::deserialize(payload)
        .map_err(|err| paro_error::serialization_error(format!("journal decode: {err}")))?;
    validate_record_metadata(&record)?;
    Ok(DecodedJournalFrame { header, record })
}

fn validate_record_metadata(record: &JournalRecord) -> Result<()> {
    match record {
        JournalRecord::Commit(record) => {
            if record.record_version != COMMIT_RECORD_VERSION {
                return Err(paro_error::serialization_error(format!(
                    "unsupported journal commit record version {}, expected {}",
                    record.record_version, COMMIT_RECORD_VERSION
                )));
            }
            if record.metadata.metadata_version != JOURNAL_RECORD_METADATA_VERSION {
                return Err(paro_error::serialization_error(format!(
                    "unsupported journal commit metadata version {}, expected {}",
                    record.metadata.metadata_version, JOURNAL_RECORD_METADATA_VERSION
                )));
            }
            if record.metadata.participant_descriptor_version
                != JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION
            {
                return Err(paro_error::serialization_error(format!(
                    "unsupported journal participant descriptor version {}, expected {}",
                    record.metadata.participant_descriptor_version,
                    JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION
                )));
            }
            if !record.metadata_matches_payload() {
                return Err(paro_error::serialization_error(
                    "journal commit metadata does not match record payload",
                ));
            }
        }
        JournalRecord::Maintenance(record) => {
            if record.record_version != MAINTENANCE_RECORD_VERSION {
                return Err(paro_error::serialization_error(format!(
                    "unsupported journal maintenance record version {}, expected {}",
                    record.record_version, MAINTENANCE_RECORD_VERSION
                )));
            }
            if record.metadata.metadata_version != JOURNAL_RECORD_METADATA_VERSION {
                return Err(paro_error::serialization_error(format!(
                    "unsupported journal maintenance metadata version {}, expected {}",
                    record.metadata.metadata_version, JOURNAL_RECORD_METADATA_VERSION
                )));
            }
            if record.metadata.participant_descriptor_version
                != JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION
            {
                return Err(paro_error::serialization_error(format!(
                    "unsupported journal participant descriptor version {}, expected {}",
                    record.metadata.participant_descriptor_version,
                    JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION
                )));
            }
            if !record.metadata_matches_payload() {
                return Err(paro_error::serialization_error(
                    "journal maintenance metadata does not match record payload",
                ));
            }
        }
        JournalRecord::CheckpointFence(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::ddl::{
        CreateSchemaPayload, DdlChange, DdlChangeRecord, DdlObjectKey, DdlObjectKind,
    };
    use paro_common::durability::{PreparedCommitPlan, PreparedMaintenancePlan};
    use paro_common::effect::{
        ApplyDescriptor, ArtifactNamespace, ArtifactRef, CleanupDescriptor,
        CompactionCumulativePointAction, DeferredTask, DeletePatchEncoding, DeletePatchGroup,
        DeletePatchInline, DeletePatchRef, DeletePatchSegment, RetiredRowsetInput,
        RuntimeTransitionDescriptor, SearchGenerationHeadMeta, StorageCommitOp, TabletApplyOp,
        TabletMutation, VersionSpan,
    };
    use paro_common::journal::{
        CheckpointFence, CommitRecord, JournalRecord, JournalRecordMetadata, MaintenanceKind,
        MaintenanceRecord, COMMIT_RECORD_VERSION, MAINTENANCE_RECORD_VERSION,
    };
    use proptest::prelude::*;

    #[test]
    fn journal_frame_roundtrip() {
        let record = JournalRecord::CheckpointFence(CheckpointFence {
            checkpoint_marker: 42,
        });
        let frame = encode_record(&record, 17).unwrap();
        let decoded = decode_frame(&frame).unwrap();
        assert_eq!(decoded.header.lsn, 17);
        assert_eq!(decoded.record, record);
    }

    #[test]
    fn journal_record_size_bound_covers_encoded_frame() {
        let record = JournalRecord::CheckpointFence(CheckpointFence {
            checkpoint_marker: 42,
        });
        let bound = encoded_journal_record_size_upper_bound(&record).unwrap();
        let frame = encode_record(&record, 17).unwrap();
        assert!(bound as usize >= frame.len());
    }

    #[test]
    fn prepared_plan_size_bound_covers_commit_record_frame() {
        let plan = PreparedCommitPlan {
            txn_id: 7,
            start_time: 11,
            catalog_ops: Vec::new(),
            storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id: 99,
                mutations: vec![TabletMutation::PublishRowset {
                    rowset_id: 123,
                    version_span: VersionSpan { start: 1, end: 9 },
                    rowset_ref: ArtifactRef {
                        namespace: ArtifactNamespace::CanonicalRowset,
                        locator: vec!["rowset_123".to_string()],
                    },
                }],
            })],
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
            tablets: Vec::new(),
        };

        let bound = encoded_size_upper_bound_for_plan(&plan).unwrap();
        let frame = encode_record(&JournalRecord::Commit(plan.into_record(42)), 17).unwrap();

        assert!(bound as usize >= frame.len());
    }

    #[test]
    fn typical_prepared_plan_calibration_ratio_stays_inside_gate() {
        let samples = [
            PreparedCommitPlan {
                txn_id: 1,
                start_time: 1,
                catalog_ops: Vec::new(),
                storage_ops: Vec::new(),
                apply_descriptors: Vec::new(),
                deferred_tasks: Vec::new(),
                tablets: Vec::new(),
            },
            PreparedCommitPlan {
                txn_id: 2,
                start_time: 2,
                catalog_ops: Vec::new(),
                storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                    tablet_id: 11,
                    mutations: vec![TabletMutation::PublishRowset {
                        rowset_id: 101,
                        version_span: VersionSpan { start: 0, end: 0 },
                        rowset_ref: ArtifactRef {
                            namespace: ArtifactNamespace::CanonicalRowset,
                            locator: vec!["tablet_11".to_string(), "rowset_101".to_string()],
                        },
                    }],
                })],
                apply_descriptors: Vec::new(),
                deferred_tasks: Vec::new(),
                tablets: Vec::new(),
            },
            PreparedCommitPlan {
                txn_id: 3,
                start_time: 3,
                catalog_ops: Vec::new(),
                storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                    tablet_id: 12,
                    mutations: vec![
                        TabletMutation::ApplyPrimaryDelete {
                            keys: vec![b"pk-1".to_vec(), b"pk-2".to_vec()],
                        },
                        TabletMutation::ApplyDeletePatch {
                            patch: DeletePatchRef::Inline(DeletePatchInline {
                                encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
                                row_count: 2,
                                groups: vec![DeletePatchGroup {
                                    rowset_id: 101,
                                    segments: vec![DeletePatchSegment {
                                        segment_id: 0,
                                        row_offsets_delta: vec![7, 3],
                                    }],
                                }],
                            }),
                            deleted_row_count: 2,
                        },
                    ],
                })],
                apply_descriptors: Vec::new(),
                deferred_tasks: Vec::new(),
                tablets: Vec::new(),
            },
        ];

        for (offset, plan) in samples.iter().enumerate() {
            let sample =
                codec_size_calibration_sample_for_plan(plan, 100 + offset as u64, 17).unwrap();
            assert!(
                sample.actual_to_estimate_ratio() >= COMMIT_BATCH_BYTES_ESTIMATE_TYPICAL_MIN_RATIO,
                "sample {offset} ratio too low: {sample:?}, slack={:.3}",
                sample.estimate_to_actual_ratio()
            );
            assert_eq!(
                sample.clamped_actual_to_estimate_ratio(),
                sample.actual_to_estimate_ratio()
            );
        }
    }

    #[test]
    fn journal_frame_rejects_checksum_mismatch() {
        let record = JournalRecord::CheckpointFence(CheckpointFence {
            checkpoint_marker: 42,
        });
        let mut frame = encode_record(&record, 3).unwrap();
        *frame.last_mut().unwrap() ^= 0x5a;
        let err = decode_frame(&frame).expect_err("checksum mismatch should fail");
        assert!(err.to_string().contains("journal checksum mismatch"));
    }

    #[test]
    fn maintenance_record_kind_roundtrips_through_journal_frame_codec() {
        let record = JournalRecord::Maintenance(MaintenanceRecord {
            record_version: MAINTENANCE_RECORD_VERSION,
            metadata: JournalRecordMetadata::maintenance(&[], &[], &[], &[]),
            maintenance_id: 9,
            kind: MaintenanceKind::MaterializedViewRefresh,
            catalog_ops: Vec::new(),
            storage_ops: Vec::new(),
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
        });
        let frame = encode_record(&record, 17).unwrap();
        let decoded = decode_frame(&frame).unwrap();
        assert_eq!(decoded.header.lsn, 17);
        assert_eq!(decoded.record, record);
    }

    #[test]
    fn journal_frame_rejects_stale_record_metadata() {
        let record = JournalRecord::Commit(CommitRecord {
            record_version: COMMIT_RECORD_VERSION,
            metadata: JournalRecordMetadata::transaction(&[], &[], &[], &[]),
            txn_id: 1,
            start_time: 1,
            commit_id: 2,
            catalog_ops: Vec::new(),
            storage_ops: vec![StorageCommitOp::Tablet(TabletApplyOp {
                tablet_id: 41,
                mutations: vec![TabletMutation::PublishRowset {
                    rowset_id: 7,
                    version_span: VersionSpan { start: 2, end: 2 },
                    rowset_ref: ArtifactRef {
                        namespace: ArtifactNamespace::CanonicalRowset,
                        locator: vec!["rowset_7".to_string()],
                    },
                }],
            })],
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
        });
        let frame = encode_record(&record, 17).unwrap();
        let err = decode_frame(&frame).expect_err("metadata drift should fail");
        assert!(err
            .to_string()
            .contains("journal commit metadata does not match record payload"));
    }

    proptest! {
        #[test]
        fn journal_record_size_bound_covers_all_record_variants(record in arb_journal_record()) {
            let sample = codec_size_calibration_sample_for_record(&record, 17).unwrap();
            prop_assert!(sample.estimated_bytes >= sample.actual_bytes);
            prop_assert!(sample.actual_to_estimate_ratio() > 0.0);
            prop_assert_eq!(
                sample.clamped_actual_to_estimate_ratio(),
                sample.actual_to_estimate_ratio().clamp(
                    COMMIT_BATCH_BYTES_ESTIMATE_RATIO_CLAMP_MIN,
                    COMMIT_BATCH_BYTES_ESTIMATE_RATIO_CLAMP_MAX,
                )
            );
        }

        #[test]
        fn prepared_plan_size_bound_covers_generated_commit_records(
            plan in arb_prepared_commit_plan(),
            commit_id in 1u64..1_000_000,
        ) {
            let sample = codec_size_calibration_sample_for_plan(&plan, commit_id, 19).unwrap();
            prop_assert!(sample.estimated_bytes >= sample.actual_bytes);
            prop_assert!(sample.actual_to_estimate_ratio() > 0.0);
        }
    }

    fn arb_journal_record() -> impl Strategy<Value = JournalRecord> {
        prop_oneof![
            arb_prepared_commit_plan()
                .prop_flat_map(|plan| (Just(plan), 1u64..1_000_000))
                .prop_map(|(plan, commit_id)| JournalRecord::Commit(plan.into_record(commit_id))),
            arb_prepared_maintenance_plan()
                .prop_flat_map(|plan| (Just(plan), 1u64..1_000_000))
                .prop_map(|(plan, maintenance_id)| {
                    JournalRecord::Maintenance(plan.into_record(maintenance_id))
                }),
            any::<u64>().prop_map(|checkpoint_marker| {
                JournalRecord::CheckpointFence(CheckpointFence { checkpoint_marker })
            }),
        ]
    }

    fn arb_prepared_commit_plan() -> impl Strategy<Value = PreparedCommitPlan> {
        (
            1u64..1_000_000,
            any::<u64>(),
            prop::collection::vec(arb_catalog_op(), 0..3),
            prop::collection::vec(arb_storage_op(), 0..4),
            prop::collection::vec(arb_apply_descriptor(), 0..3),
            prop::collection::vec(arb_deferred_task(), 0..3),
        )
            .prop_map(
                |(
                    txn_id,
                    start_time,
                    catalog_ops,
                    storage_ops,
                    apply_descriptors,
                    deferred_tasks,
                )| PreparedCommitPlan {
                    txn_id,
                    start_time,
                    catalog_ops,
                    storage_ops,
                    apply_descriptors,
                    deferred_tasks,
                    tablets: Vec::new(),
                },
            )
    }

    fn arb_prepared_maintenance_plan() -> impl Strategy<Value = PreparedMaintenancePlan> {
        (
            arb_maintenance_kind(),
            prop::collection::vec(arb_catalog_op(), 0..3),
            prop::collection::vec(arb_storage_op(), 0..4),
            prop::collection::vec(arb_apply_descriptor(), 0..3),
            prop::collection::vec(arb_deferred_task(), 0..3),
        )
            .prop_map(
                |(kind, catalog_ops, storage_ops, apply_descriptors, deferred_tasks)| {
                    PreparedMaintenancePlan {
                        kind,
                        catalog_ops,
                        storage_ops,
                        apply_descriptors,
                        deferred_tasks,
                        tablets: Vec::new(),
                    }
                },
            )
    }

    fn arb_maintenance_kind() -> impl Strategy<Value = MaintenanceKind> {
        prop_oneof![
            Just(MaintenanceKind::Compaction),
            Just(MaintenanceKind::IndexBackfill),
            Just(MaintenanceKind::MaterializedViewRefresh),
        ]
    }

    fn arb_catalog_op() -> impl Strategy<Value = paro_common::effect::CatalogTxnOp> {
        (arb_object_key(), any::<u64>(), any::<bool>()).prop_map(
            |(key, object_id, if_not_exists)| paro_common::effect::CatalogTxnOp {
                change: DdlChangeRecord {
                    key,
                    change: DdlChange::CreateSchema(CreateSchemaPayload {
                        object_id,
                        if_not_exists,
                    }),
                },
            },
        )
    }

    fn arb_storage_op() -> impl Strategy<Value = StorageCommitOp> {
        (
            1u64..1_000_000,
            prop::collection::vec(arb_tablet_mutation(), 0..4),
        )
            .prop_map(|(tablet_id, mutations)| {
                StorageCommitOp::Tablet(TabletApplyOp {
                    tablet_id,
                    mutations,
                })
            })
    }

    fn arb_tablet_mutation() -> impl Strategy<Value = TabletMutation> {
        prop_oneof![
            (1u64..1_000_000, arb_version_span(), arb_artifact_ref()).prop_map(
                |(rowset_id, version_span, rowset_ref)| TabletMutation::PublishRowset {
                    rowset_id,
                    version_span,
                    rowset_ref,
                }
            ),
            prop::collection::vec(prop::collection::vec(any::<u8>(), 0..24), 0..8)
                .prop_map(|keys| TabletMutation::ApplyPrimaryDelete { keys }),
            (arb_delete_patch_ref(), 0u32..1_000).prop_map(|(patch, deleted_row_count)| {
                TabletMutation::ApplyDeletePatch {
                    patch,
                    deleted_row_count,
                }
            }),
            (
                1u64..1_000_000,
                1u64..1_000_000,
                1u64..1_000_000,
                arb_version_span(),
                arb_artifact_ref(),
                arb_artifact_ref(),
                prop::collection::vec(1u64..1_000_000, 0..4),
                prop::collection::vec(arb_retired_rowset_input(), 0..4),
                prop_oneof![
                    Just(CompactionCumulativePointAction::Preserve),
                    Just(CompactionCumulativePointAction::AdvanceToOutputEndExclusive),
                ],
            )
                .prop_map(
                    |(
                        plan_id,
                        job_id,
                        output_rowset_id,
                        output_version,
                        staged_ref,
                        output_ref,
                        replaced_inputs,
                        retired_inputs,
                        cumulative_point_action,
                    )| TabletMutation::PublishCompaction {
                        plan_id,
                        job_id,
                        output_rowset_id,
                        output_version,
                        staged_ref,
                        output_ref,
                        replaced_inputs,
                        retired_inputs,
                        cumulative_point_action,
                    },
                ),
            (
                arb_artifact_ref(),
                arb_artifact_ref(),
                1u64..1_000_000,
                1u64..1_000_000,
                1u64..1_000_000,
                any::<u64>(),
            )
                .prop_map(
                    |(
                        staged_ref,
                        generation_ref,
                        definition_id,
                        generation_id,
                        root_version,
                        config_fingerprint,
                    )| TabletMutation::PublishSearchGeneration {
                        staged_ref,
                        generation_ref,
                        head: SearchGenerationHeadMeta {
                            definition_id,
                            generation_id,
                            root_version,
                            config_fingerprint,
                            root_file_name: format!(
                                "manifest_g{generation_id}_v{root_version}.json"
                            ),
                        },
                    },
                ),
        ]
    }

    fn arb_version_span() -> impl Strategy<Value = VersionSpan> {
        (0i64..1_000_000, 0i64..1_000_000).prop_map(|(start, delta)| VersionSpan {
            start,
            end: start.saturating_add(delta),
        })
    }

    fn arb_retired_rowset_input() -> impl Strategy<Value = RetiredRowsetInput> {
        (
            1u64..1_000_000,
            0i64..1_000_000,
            0i64..1_000_000,
            prop::collection::vec(any::<u32>(), 0..6),
        )
            .prop_map(
                |(rowset_id, start_version, end_delta, rssids)| RetiredRowsetInput {
                    rowset_id,
                    start_version,
                    end_version: start_version.saturating_add(end_delta),
                    rssids,
                },
            )
    }

    fn arb_artifact_ref() -> impl Strategy<Value = ArtifactRef> {
        (
            prop_oneof![
                Just(ArtifactNamespace::CanonicalRowset),
                Just(ArtifactNamespace::Staged),
                Just(ArtifactNamespace::DeletePatch),
                Just(ArtifactNamespace::SearchGeneration),
            ],
            small_components(0..4),
        )
            .prop_map(|(namespace, locator)| ArtifactRef { namespace, locator })
    }

    fn arb_delete_patch_ref() -> impl Strategy<Value = DeletePatchRef> {
        prop_oneof![
            arb_delete_patch_inline().prop_map(DeletePatchRef::Inline),
            arb_artifact_ref().prop_map(DeletePatchRef::Artifact),
        ]
    }

    fn arb_delete_patch_inline() -> impl Strategy<Value = DeletePatchInline> {
        prop::collection::vec(
            (
                1u64..1_000_000,
                prop::collection::vec(
                    (any::<u32>(), prop::collection::vec(0u32..1024, 0..6)),
                    0..4,
                ),
            ),
            0..4,
        )
        .prop_map(|groups| {
            let mut row_count = 0u32;
            let groups = groups
                .into_iter()
                .map(|(rowset_id, segments)| {
                    let segments = segments
                        .into_iter()
                        .map(|(segment_id, row_offsets_delta)| {
                            row_count = row_count.saturating_add(row_offsets_delta.len() as u32);
                            DeletePatchSegment {
                                segment_id,
                                row_offsets_delta,
                            }
                        })
                        .collect();
                    DeletePatchGroup {
                        rowset_id,
                        segments,
                    }
                })
                .collect();
            DeletePatchInline {
                encoding: DeletePatchEncoding::GroupedRowOffsetDeltaV1,
                row_count,
                groups,
            }
        })
    }

    fn arb_apply_descriptor() -> impl Strategy<Value = ApplyDescriptor> {
        prop_oneof![
            arb_object_key().prop_map(|graph| {
                ApplyDescriptor::RuntimeTransition(
                    RuntimeTransitionDescriptor::RegisterGraphRuntime { graph },
                )
            }),
            (small_components(0..4), any::<bool>()).prop_map(|(path_components, recursive)| {
                ApplyDescriptor::Cleanup(CleanupDescriptor::RemoveDirectory {
                    path_components,
                    recursive,
                })
            }),
        ]
    }

    fn arb_deferred_task() -> impl Strategy<Value = DeferredTask> {
        (
            arb_object_key(),
            small_component(),
            small_component(),
            prop::collection::vec(any::<u32>(), 0..6),
            prop::option::of(small_component()),
        )
            .prop_map(
                |(index, table_name, index_type, column_ids, fulltext_config)| {
                    DeferredTask::FinalizeIndexState {
                        index,
                        table_name,
                        index_type,
                        column_ids,
                        fulltext_config,
                    }
                },
            )
    }

    fn arb_object_key() -> impl Strategy<Value = DdlObjectKey> {
        (
            small_component(),
            prop::option::of(small_component()),
            small_component(),
            prop_oneof![
                Just(DdlObjectKind::Schema),
                Just(DdlObjectKind::Table),
                Just(DdlObjectKind::View),
                Just(DdlObjectKind::Index),
                Just(DdlObjectKind::PropertyGraph),
            ],
        )
            .prop_map(|(database, schema, name, kind)| DdlObjectKey {
                database,
                schema,
                name,
                kind,
            })
    }

    fn small_components(size: std::ops::Range<usize>) -> impl Strategy<Value = Vec<String>> {
        prop::collection::vec(small_component(), size)
    }

    fn small_component() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_]{0,12}".prop_map(|value| value)
    }
}

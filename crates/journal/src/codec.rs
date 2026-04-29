// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error as paro_error;
use paro_common::error::Result;
use paro_common::journal::{
    JournalRecord, COMMIT_RECORD_VERSION, JOURNAL_FORMAT_VERSION,
    JOURNAL_PARTICIPANT_DESCRIPTOR_VERSION, JOURNAL_RECORD_METADATA_VERSION,
    MAINTENANCE_RECORD_VERSION,
};

pub const JOURNAL_FRAME_HEADER_SIZE: usize = 18;

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
    let checksum = crc32c::crc32c(&payload);
    let header = JournalFrameHeader::new(payload_len, lsn, checksum);
    let mut frame = Vec::with_capacity(JOURNAL_FRAME_HEADER_SIZE + payload.len());
    frame.extend_from_slice(&header.encode());
    frame.extend_from_slice(&payload);
    Ok(frame)
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
    use paro_common::effect::{StorageCommitOp, TabletApplyOp, TabletMutation, VersionSpan};
    use paro_common::journal::{
        CheckpointFence, CommitRecord, JournalRecord, JournalRecordMetadata, MaintenanceKind,
        MaintenanceRecord, COMMIT_RECORD_VERSION, MAINTENANCE_RECORD_VERSION,
    };

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
                    rowset_ref: paro_common::effect::ArtifactRef {
                        namespace: paro_common::effect::ArtifactNamespace::CanonicalRowset,
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
}

use super::tablet_runtime::{Tablet, Version};
use crate::compaction::plan::types::CumulativePointAction;
use crate::compaction::publish::record::CompactionPublishRecord;
use crate::wal::recovery::{ReplayHandler, WalRecovery};
use paro_common::error::Result;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TabletWalReplayReport {
    pub replayed_missing_rowset_commit: bool,
    pub replayed_compaction_publish: bool,
}

struct TabletWalReplayHandler<'a> {
    tablet: &'a Tablet,
    report: TabletWalReplayReport,
}

impl ReplayHandler for TabletWalReplayHandler<'_> {
    fn replay_primary_delete(&mut self, keys: &[Vec<u8>]) -> Result<()> {
        self.tablet.replay_primary_delete_idempotent(keys.to_vec())
    }

    fn replay_row_id_delete(&mut self, locations: &[(u64, u32, u32)]) -> Result<()> {
        self.tablet.apply_row_id_delete_locations(locations)
    }

    fn replay_rowset_commit(
        &mut self,
        tablet_id: u64,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: &str,
    ) -> Result<()> {
        if tablet_id != self.tablet.tablet_id() {
            return Ok(());
        }
        let version = Version::new(start_version, end_version);
        if self.tablet.find_rowset_by_id(rowset_id).is_none()
            && should_replay_rowset_commit(self.tablet, &version)
        {
            self.report.replayed_missing_rowset_commit = true;
        }
        self.tablet
            .replay_rowset_commit(rowset_id, start_version, end_version, rowset_path)
    }

    fn replay_compaction_publish(
        &mut self,
        tablet_id: u64,
        plan_id: u64,
        job_id: u64,
        output_rowset_id: u64,
        output_start_version: i64,
        output_end_version: i64,
        cumulative_point_action: CumulativePointAction,
        output_rowset_path: &str,
        replaced_inputs: &[u64],
    ) -> Result<()> {
        if tablet_id != self.tablet.tablet_id() {
            return Ok(());
        }
        self.report.replayed_compaction_publish = true;
        self.tablet
            .replay_compaction_publish(&CompactionPublishRecord {
                plan_id: crate::compaction::plan::types::CompactionPlanId(plan_id),
                job_id: crate::compaction::plan::types::CompactionJobId(job_id),
                tablet_id,
                output_rowset_id,
                output_version: Version::new(output_start_version, output_end_version),
                cumulative_point_action,
                output_rowset_path: output_rowset_path.to_string(),
                replaced_inputs: replaced_inputs.to_vec(),
            })
    }

    fn on_checkpoint(&mut self, _: u64) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn should_replay_rowset_commit(tablet: &Tablet, version: &Version) -> bool {
    let rs_map = tablet.rs_version_map.read().unwrap();
    !rs_map
        .keys()
        .any(|existing_version| existing_version.contains_range(version))
}

pub(crate) fn replay_primary_wal(tablet: &Tablet) -> Result<TabletWalReplayReport> {
    let wal_path = tablet.data_dir().join("tablet.wal");
    let recovery = WalRecovery::new(&wal_path);
    let mut handler = TabletWalReplayHandler {
        tablet,
        report: TabletWalReplayReport::default(),
    };
    let _ = recovery.recover(&mut handler)?;
    Ok(handler.report)
}

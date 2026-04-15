use super::segment::Segment;
use crate::primary_key::DeleteVector;
use paro_common::error::Result;
#[cfg(test)]
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;

const LIVE_DELETE_VECTOR_EPOCH: u64 = u64::MAX;

#[derive(Debug, Clone)]
pub(super) struct CachedDeleteVector {
    pub(super) epoch: u64,
    pub(super) delete_vector: Option<DeleteVector>,
}

impl Segment {
    pub(super) fn load_delete_vector_from_disk(&self) -> Result<Option<DeleteVector>> {
        DeleteVector::load_from_dir(self.rowset_path()?, self.segment_id)
    }

    pub(super) fn load_delete_vector_from_disk_at_version(
        &self,
        version: i64,
    ) -> Result<Option<DeleteVector>> {
        DeleteVector::load_from_dir_at_version(self.rowset_path()?, self.segment_id, version)
    }

    /// Invalidate cached delete vector after rowset reload / delvec persistence.
    pub(crate) fn invalidate_delete_vector_cache(&self) {
        self.delete_vector_cache.store(None);
    }

    /// Load delete vector for this segment at the provided snapshot epoch.
    pub(crate) fn load_delete_vector_with_epoch(&self, epoch: u64) -> Result<Option<DeleteVector>> {
        #[cfg(test)]
        self.delete_vector_load_requests
            .fetch_add(1, AtomicOrdering::Relaxed);

        if let Some(cached) = self.delete_vector_cache.load_full() {
            if cached.epoch == epoch {
                return Ok(cached.delete_vector.clone());
            }
        }

        let delete_vector = if epoch == LIVE_DELETE_VECTOR_EPOCH {
            self.load_delete_vector_from_disk()?
        } else {
            self.load_delete_vector_from_disk_at_version(epoch as i64)?
        };
        self.delete_vector_cache
            .store(Some(Arc::new(CachedDeleteVector {
                epoch,
                delete_vector: delete_vector.clone(),
            })));
        Ok(delete_vector)
    }

    /// Load delete vector for this segment.
    pub fn load_delete_vector(&self) -> Result<Option<DeleteVector>> {
        self.load_delete_vector_with_epoch(LIVE_DELETE_VECTOR_EPOCH)
    }

    pub fn load_delete_vector_at_version(&self, version: i64) -> Result<Option<DeleteVector>> {
        self.load_delete_vector_from_disk_at_version(version)
    }

    #[cfg(test)]
    pub(crate) fn reset_delete_vector_load_requests_for_test(&self) {
        self.delete_vector_load_requests
            .store(0, AtomicOrdering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn delete_vector_load_requests_for_test(&self) -> u64 {
        self.delete_vector_load_requests
            .load(AtomicOrdering::Relaxed)
    }
}

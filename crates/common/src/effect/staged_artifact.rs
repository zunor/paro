use crate::ddl::DdlObjectKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StagingArtifactId {
    pub txn_id: u64,
    pub path_components: Vec<String>,
}

impl StagingArtifactId {
    pub fn new(txn_id: u64, path_components: Vec<String>) -> Self {
        Self {
            txn_id,
            path_components,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StagedArtifactDescriptor {
    PropertyGraphBuild {
        object: DdlObjectKey,
        staging: StagingArtifactId,
        schema_fingerprint: String,
    },
}

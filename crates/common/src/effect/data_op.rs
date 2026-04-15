use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowsetLocator {
    pub tablet_id: u64,
    pub rowset_id: u64,
    pub path_components: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreparedDataOp {
    RowsetCommit {
        locator: RowsetLocator,
        start_version: i64,
        end_version: i64,
    },
    PrimaryDelete {
        tablet_id: u64,
        keys: Vec<Vec<u8>>,
    },
    RowIdDelete {
        tablet_id: u64,
        locations: Vec<(u64, u32, u32)>,
    },
}

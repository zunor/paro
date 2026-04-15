use paro_storage::wal::wal_entry::{WalHeaderMetadata, WAL_DB_IDENTIFIER_LEN};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const DATABASE_STORAGE_IDENTITY_FORMAT_VERSION: u16 = 1;
pub const DATABASE_STORAGE_IDENTITY_KEY: &str = "config/storage_identity";

static STORAGE_IDENTITY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseStorageIdentity {
    pub format_version: u16,
    pub database_id: u64,
    pub db_identifier: [u8; WAL_DB_IDENTIFIER_LEN],
    pub created_at_ms: i64,
}

impl DatabaseStorageIdentity {
    pub fn new(database_id: u64) -> Self {
        Self {
            format_version: DATABASE_STORAGE_IDENTITY_FORMAT_VERSION,
            database_id,
            db_identifier: generate_db_identifier(database_id),
            created_at_ms: current_timestamp_ms(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.format_version != DATABASE_STORAGE_IDENTITY_FORMAT_VERSION {
            anyhow::bail!(
                "unsupported storage identity format version {}",
                self.format_version
            );
        }
        if self.database_id == 0 {
            anyhow::bail!("storage identity database_id 0 is reserved");
        }
        if self.db_identifier == [0; WAL_DB_IDENTIFIER_LEN] {
            anyhow::bail!("storage identity db_identifier cannot be all zeros");
        }
        Ok(())
    }

    pub fn wal_header_metadata(&self) -> anyhow::Result<WalHeaderMetadata> {
        self.validate()?;
        Ok(WalHeaderMetadata::new(self.db_identifier, 0))
    }
}

fn generate_db_identifier(database_id: u64) -> [u8; WAL_DB_IDENTIFIER_LEN] {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let sequence = STORAGE_IDENTITY_SEQUENCE.fetch_add(1, Ordering::Relaxed) as u128;
    let pid = std::process::id() as u128;
    let mixed = now_nanos ^ (sequence << 64) ^ (pid << 32) ^ database_id as u128;
    mixed.to_le_bytes()
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

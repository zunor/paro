// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! TableStorageDescriptor - persistent storage handle for catalog entries.
//!
//! The descriptor intentionally stores only stable metadata required to
//! reconstruct/open runtime table storage.

use crate::tablet::KeysType;
use crate::tablet::TabletIdentity;
use paro_common::error::{self as paro_error, Result};
use std::io::{Cursor, Read, Write};

const DESCRIPTOR_MAGIC: [u8; 4] = *b"PTSD";
const MAX_DATA_DIR_LEN: usize = 16 * 1024;

/// Stable storage descriptor persisted by catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStorageDescriptor {
    pub format_version: u16,
    pub tablet_id: u64,
    pub table_id: u64,
    pub partition_id: u64,
    pub schema_id: u64,
    pub schema_version: u32,
    pub schema_hash: u32,
    pub data_dir: String,
    pub keys_type: u8,
}

impl TableStorageDescriptor {
    pub const CURRENT_FORMAT_VERSION: u16 = 2;

    /// Build a descriptor from raw fields.
    pub fn new(
        tablet_id: u64,
        table_id: u64,
        partition_id: u64,
        schema_id: u64,
        schema_version: u32,
        schema_hash: u32,
        data_dir: String,
        keys_type: u8,
    ) -> Result<Self> {
        let descriptor = Self {
            format_version: Self::CURRENT_FORMAT_VERSION,
            tablet_id,
            table_id,
            partition_id,
            schema_id,
            schema_version,
            schema_hash,
            data_dir,
            keys_type,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// Build a descriptor from `KeysType`.
    pub fn from_keys_type(
        tablet_id: u64,
        table_id: u64,
        partition_id: u64,
        schema_id: u64,
        schema_version: u32,
        schema_hash: u32,
        data_dir: String,
        keys_type: KeysType,
    ) -> Result<Self> {
        Self::new(
            tablet_id,
            table_id,
            partition_id,
            schema_id,
            schema_version,
            schema_hash,
            data_dir,
            Self::encode_keys_type(keys_type)?,
        )
    }

    pub fn identity(&self) -> TabletIdentity {
        TabletIdentity {
            table_id: self.table_id,
            partition_id: self.partition_id,
            tablet_id: self.tablet_id,
            schema_id: self.schema_id,
            schema_version: self.schema_version,
        }
    }

    pub fn keys_type_enum(&self) -> Result<KeysType> {
        Self::decode_keys_type(self.keys_type)
    }

    pub fn encode_keys_type(keys_type: KeysType) -> Result<u8> {
        match keys_type {
            KeysType::PrimaryKeys => Ok(KeysType::PrimaryKeys as u8),
            KeysType::DuplicateKeys => Ok(KeysType::DuplicateKeys as u8),
            _ => Err(paro_error::invalid_input(format!(
                "unsupported keys_type for TableStorageDescriptor: {keys_type:?}"
            ))),
        }
    }

    pub fn decode_keys_type(code: u8) -> Result<KeysType> {
        match code {
            x if x == KeysType::PrimaryKeys as u8 => Ok(KeysType::PrimaryKeys),
            x if x == KeysType::DuplicateKeys as u8 => Ok(KeysType::DuplicateKeys),
            _ => Err(paro_error::invalid_input(format!(
                "unsupported keys_type code for TableStorageDescriptor: {code}"
            ))),
        }
    }

    /// Validate version and field constraints.
    pub fn validate(&self) -> Result<()> {
        if self.format_version != Self::CURRENT_FORMAT_VERSION {
            return Err(paro_error::invalid_input(format!(
                "unsupported table storage descriptor format version {}, expected {}",
                self.format_version,
                Self::CURRENT_FORMAT_VERSION
            )));
        }

        if self.tablet_id == 0 {
            return Err(paro_error::invalid_input(
                "table storage descriptor tablet_id must be > 0",
            ));
        }
        if self.table_id == 0 {
            return Err(paro_error::invalid_input(
                "table storage descriptor table_id must be > 0",
            ));
        }
        if self.schema_id == 0 {
            return Err(paro_error::invalid_input(
                "table storage descriptor schema_id must be > 0",
            ));
        }
        if self.schema_version == 0 {
            return Err(paro_error::invalid_input(
                "table storage descriptor schema_version must be > 0",
            ));
        }

        if self.data_dir.is_empty() {
            return Err(paro_error::invalid_input(
                "table storage descriptor data_dir must not be empty",
            ));
        }
        if self.data_dir.len() > MAX_DATA_DIR_LEN {
            return Err(paro_error::invalid_input(format!(
                "table storage descriptor data_dir too long: {}",
                self.data_dir.len()
            )));
        }

        Self::decode_keys_type(self.keys_type)?;
        Ok(())
    }

    /// Serialize descriptor into a compact binary payload.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        self.validate()?;

        let mut buffer = Vec::new();
        buffer.write_all(&DESCRIPTOR_MAGIC)?;
        buffer.write_all(&self.format_version.to_le_bytes())?;
        buffer.write_all(&self.tablet_id.to_le_bytes())?;
        buffer.write_all(&self.table_id.to_le_bytes())?;
        buffer.write_all(&self.partition_id.to_le_bytes())?;
        buffer.write_all(&self.schema_id.to_le_bytes())?;
        buffer.write_all(&self.schema_version.to_le_bytes())?;
        buffer.write_all(&self.schema_hash.to_le_bytes())?;

        let data_dir = self.data_dir.as_bytes();
        buffer.write_all(&(data_dir.len() as u32).to_le_bytes())?;
        buffer.write_all(data_dir)?;
        buffer.write_all(&[self.keys_type])?;
        Ok(buffer)
    }

    /// Deserialize a descriptor from binary payload.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        let mut magic = [0u8; 4];
        cursor.read_exact(&mut magic)?;
        if magic != DESCRIPTOR_MAGIC {
            return Err(paro_error::invalid_input(
                "invalid table storage descriptor magic",
            ));
        }

        let mut u16_buf = [0u8; 2];
        cursor.read_exact(&mut u16_buf)?;
        let format_version = u16::from_le_bytes(u16_buf);

        let mut u64_buf = [0u8; 8];
        cursor.read_exact(&mut u64_buf)?;
        let tablet_id = u64::from_le_bytes(u64_buf);

        cursor.read_exact(&mut u64_buf)?;
        let table_id = u64::from_le_bytes(u64_buf);

        cursor.read_exact(&mut u64_buf)?;
        let partition_id = u64::from_le_bytes(u64_buf);

        cursor.read_exact(&mut u64_buf)?;
        let schema_id = u64::from_le_bytes(u64_buf);

        let mut u32_buf = [0u8; 4];
        cursor.read_exact(&mut u32_buf)?;
        let schema_version = u32::from_le_bytes(u32_buf);

        cursor.read_exact(&mut u32_buf)?;
        let schema_hash = u32::from_le_bytes(u32_buf);

        cursor.read_exact(&mut u32_buf)?;
        let data_dir_len = u32::from_le_bytes(u32_buf) as usize;
        if data_dir_len > MAX_DATA_DIR_LEN {
            return Err(paro_error::invalid_input(format!(
                "table storage descriptor data_dir too long: {data_dir_len}"
            )));
        }

        let mut data_dir_bytes = vec![0u8; data_dir_len];
        cursor.read_exact(&mut data_dir_bytes)?;
        let data_dir = String::from_utf8(data_dir_bytes).map_err(|e| {
            paro_error::invalid_input(format!(
                "invalid UTF-8 in table storage descriptor data_dir: {e}"
            ))
        })?;

        let mut keys_type_buf = [0u8; 1];
        cursor.read_exact(&mut keys_type_buf)?;
        let keys_type = keys_type_buf[0];

        if cursor.position() as usize != bytes.len() {
            return Err(paro_error::invalid_input(
                "table storage descriptor contains trailing bytes",
            ));
        }

        let descriptor = Self {
            format_version,
            tablet_id,
            table_id,
            partition_id,
            schema_id,
            schema_version,
            schema_hash,
            data_dir,
            keys_type,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_roundtrip() {
        let descriptor = TableStorageDescriptor::from_keys_type(
            1,
            2,
            0,
            11,
            7,
            100,
            "/tmp/paro/tablet-1".to_string(),
            KeysType::PrimaryKeys,
        )
        .unwrap();

        let bytes = descriptor.serialize().unwrap();
        let restored = TableStorageDescriptor::deserialize(&bytes).unwrap();
        assert_eq!(restored, descriptor);
        assert_eq!(restored.keys_type_enum().unwrap(), KeysType::PrimaryKeys);
    }

    #[test]
    fn descriptor_rejects_invalid_keys_type() {
        let mut descriptor = TableStorageDescriptor::new(
            1,
            2,
            0,
            11,
            7,
            100,
            "/tmp/paro/tablet-1".to_string(),
            KeysType::DuplicateKeys as u8,
        )
        .unwrap();
        descriptor.keys_type = KeysType::UniqueKeys as u8;
        assert!(descriptor.validate().is_err());
    }

    #[test]
    fn descriptor_rejects_unknown_version() {
        let mut descriptor = TableStorageDescriptor::from_keys_type(
            1,
            2,
            0,
            11,
            7,
            100,
            "/tmp/paro/tablet-1".to_string(),
            KeysType::DuplicateKeys,
        )
        .unwrap();
        descriptor.format_version = TableStorageDescriptor::CURRENT_FORMAT_VERSION + 1;
        let err = descriptor.serialize().unwrap_err().to_string();
        assert!(err.contains("unsupported table storage descriptor format version"));
    }

    #[test]
    fn descriptor_exposes_tablet_identity() {
        let descriptor = TableStorageDescriptor::from_keys_type(
            5,
            6,
            7,
            8,
            9,
            100,
            "/tmp/paro/tablet-5".to_string(),
            KeysType::DuplicateKeys,
        )
        .unwrap();

        assert_eq!(
            descriptor.identity(),
            TabletIdentity {
                tablet_id: 5,
                table_id: 6,
                partition_id: 7,
                schema_id: 8,
                schema_version: 9,
            }
        );
    }
}

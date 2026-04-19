// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const SEGMENT_CATALOG_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentLayout {
    seed_path: PathBuf,
    root_dir: PathBuf,
    catalog_path: PathBuf,
    segments_dir: PathBuf,
}

impl SegmentLayout {
    pub fn from_seed_path(seed_path: impl AsRef<Path>) -> Self {
        let seed_path = seed_path.as_ref().to_path_buf();
        let root_dir = if seed_path.is_dir() {
            seed_path.clone()
        } else {
            seed_path.with_extension("journal")
        };
        let catalog_path = root_dir.join("catalog.json");
        let segments_dir = root_dir.join("segments");
        Self {
            seed_path,
            root_dir,
            catalog_path,
            segments_dir,
        }
    }

    pub fn seed_path(&self) -> &Path {
        &self.seed_path
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn catalog_path(&self) -> &Path {
        &self.catalog_path
    }

    pub fn segments_dir(&self) -> &Path {
        &self.segments_dir
    }

    pub fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.segments_dir.join(format!("{segment_id:020}.wal"))
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        fs::create_dir_all(&self.root_dir).map_err(|err| {
            paro_error::io_error(format!(
                "create journal root {}: {}",
                self.root_dir.display(),
                err
            ))
        })?;
        fs::create_dir_all(&self.segments_dir).map_err(|err| {
            paro_error::io_error(format!(
                "create journal segments root {}: {}",
                self.segments_dir.display(),
                err
            ))
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentCatalogEntry {
    pub segment_id: u64,
    pub locator: String,
    pub start_lsn: u64,
    pub sealed_end_lsn: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentCatalog {
    pub format_version: u32,
    pub active_segment_id: u64,
    pub next_segment_id: u64,
    pub segments: Vec<SegmentCatalogEntry>,
}

impl SegmentCatalog {
    pub fn new(initial_start_lsn: u64) -> Self {
        Self {
            format_version: SEGMENT_CATALOG_FORMAT_VERSION,
            active_segment_id: 1,
            next_segment_id: 2,
            segments: vec![SegmentCatalogEntry {
                segment_id: 1,
                locator: format!("{:020}.wal", 1),
                start_lsn: initial_start_lsn.max(1),
                sealed_end_lsn: None,
            }],
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.format_version != SEGMENT_CATALOG_FORMAT_VERSION {
            return Err(paro_error::invalid_input(format!(
                "unsupported segment catalog format version {}, expected {}",
                self.format_version, SEGMENT_CATALOG_FORMAT_VERSION
            )));
        }
        if self.segments.is_empty() {
            return Err(paro_error::invalid_input(
                "segment catalog must contain at least one segment",
            ));
        }
        if self
            .segments
            .iter()
            .all(|segment| segment.segment_id != self.active_segment_id)
        {
            return Err(paro_error::invalid_input(format!(
                "segment catalog active segment {} is missing",
                self.active_segment_id
            )));
        }
        Ok(())
    }

    pub fn active_segment(&self) -> Option<&SegmentCatalogEntry> {
        self.segments
            .iter()
            .find(|segment| segment.segment_id == self.active_segment_id)
    }

    pub fn active_segment_mut(&mut self) -> Option<&mut SegmentCatalogEntry> {
        self.segments
            .iter_mut()
            .find(|segment| segment.segment_id == self.active_segment_id)
    }

    pub fn append_rotated_segment(&mut self, start_lsn: u64) -> SegmentCatalogEntry {
        let segment_id = self.next_segment_id;
        self.next_segment_id = self.next_segment_id.saturating_add(1);
        self.active_segment_id = segment_id;
        let entry = SegmentCatalogEntry {
            segment_id,
            locator: format!("{segment_id:020}.wal"),
            start_lsn,
            sealed_end_lsn: None,
        };
        self.segments.push(entry.clone());
        self.segments.sort_by_key(|segment| segment.segment_id);
        entry
    }

    pub fn segment_for_replay_lsn(&self, replay_from_lsn: u64) -> Option<&SegmentCatalogEntry> {
        let mut segments = self.segments.iter().collect::<Vec<_>>();
        segments.sort_by_key(|segment| segment.segment_id);

        if replay_from_lsn == 0 {
            return segments.first().copied();
        }

        let mut candidate = segments.first().copied();
        for segment in segments {
            if segment.start_lsn > replay_from_lsn {
                break;
            }
            candidate = Some(segment);
            if segment
                .sealed_end_lsn
                .is_some_and(|sealed_end_lsn| replay_from_lsn <= sealed_end_lsn)
            {
                return Some(segment);
            }
        }
        candidate
    }
}

#[derive(Debug, Clone)]
pub struct SegmentCatalogStore {
    layout: SegmentLayout,
}

impl SegmentCatalogStore {
    pub fn from_seed_path(seed_path: impl AsRef<Path>) -> Self {
        Self {
            layout: SegmentLayout::from_seed_path(seed_path),
        }
    }

    pub fn layout(&self) -> &SegmentLayout {
        &self.layout
    }

    pub fn exists(&self) -> bool {
        self.layout.catalog_path().exists()
    }

    pub fn load(&self) -> Result<Option<SegmentCatalog>> {
        if !self.layout.catalog_path().exists() {
            return Ok(None);
        }
        let bytes = fs::read(self.layout.catalog_path()).map_err(|err| {
            paro_error::io_error(format!(
                "read segment catalog {}: {}",
                self.layout.catalog_path().display(),
                err
            ))
        })?;
        let catalog: SegmentCatalog = serde_json::from_slice(&bytes).map_err(|err| {
            paro_error::invalid_input(format!(
                "deserialize segment catalog {}: {}",
                self.layout.catalog_path().display(),
                err
            ))
        })?;
        catalog.validate()?;
        Ok(Some(catalog))
    }

    pub fn load_or_create(&self, initial_start_lsn: u64) -> Result<SegmentCatalog> {
        if let Some(catalog) = self.load()? {
            return Ok(catalog);
        }
        let catalog = SegmentCatalog::new(initial_start_lsn);
        self.save(&catalog)?;
        Ok(catalog)
    }

    pub fn save(&self, catalog: &SegmentCatalog) -> Result<()> {
        catalog.validate()?;
        self.layout.ensure_dirs()?;

        let bytes = serde_json::to_vec_pretty(catalog)
            .map_err(|err| paro_error::internal(format!("serialize segment catalog: {}", err)))?;

        let tmp_path = self.layout.catalog_path().with_extension("json.tmp");
        let mut file = File::create(&tmp_path).map_err(|err| {
            paro_error::io_error(format!(
                "create segment catalog tmp {}: {}",
                tmp_path.display(),
                err
            ))
        })?;
        file.write_all(&bytes).map_err(|err| {
            paro_error::io_error(format!(
                "write segment catalog tmp {}: {}",
                tmp_path.display(),
                err
            ))
        })?;
        file.sync_all().map_err(|err| {
            paro_error::io_error(format!(
                "sync segment catalog tmp {}: {}",
                tmp_path.display(),
                err
            ))
        })?;
        drop(file);

        fs::rename(&tmp_path, self.layout.catalog_path()).map_err(|err| {
            paro_error::io_error(format!(
                "rename segment catalog tmp {} -> {}: {}",
                tmp_path.display(),
                self.layout.catalog_path().display(),
                err
            ))
        })?;

        let parent = self
            .layout
            .catalog_path()
            .parent()
            .ok_or_else(|| paro_error::internal("segment catalog has no parent directory"))?;
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|err| {
                paro_error::io_error(format!(
                    "sync segment catalog parent {}: {}",
                    parent.display(),
                    err
                ))
            })?;
        Ok(())
    }
}

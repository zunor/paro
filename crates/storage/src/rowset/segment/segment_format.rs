// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::index::short_key::ShortKeyFooter;
use crate::rowset::encoding::FieldType;
use crate::rowset::page::{CompressionType, EncodingType, PagePointer};
use crate::statistics::{ColumnStatistics, HnswIndexStatistics};
use crate::tablet::ColumnId;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

/// Column metadata within a segment footer.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    /// Column ID
    pub column_id: ColumnId,
    /// Number of rows
    pub num_rows: u64,
    /// Encoding type
    pub encoding: EncodingType,
    /// Compression type
    pub compression: CompressionType,
    /// Data page pointer
    pub data_page_pointer: PagePointer,
    /// Ordinal index pointer
    pub ordinal_index_pointer: PagePointer,
    /// ZoneMap index pointer
    pub zonemap_index_pointer: PagePointer,
    /// Dictionary page pointer (optional)
    pub dict_page_pointer: Option<PagePointer>,
    /// Bloom filter index pointer (optional)
    pub bloom_filter_pointer: Option<PagePointer>,
    /// Bitmap index pointer (optional)
    pub bitmap_index_pointer: Option<PagePointer>,
    /// HNSW index pointer (optional)
    pub hnsw_index_pointer: Option<PagePointer>,
    /// Small durable HNSW summary used without materializing the graph.
    pub hnsw_index_statistics: Option<HnswIndexStatistics>,
    /// Sparse index pointer (optional)
    pub sparse_index_pointer: Option<PagePointer>,
    /// Full-text index pointer (optional)
    pub fulltext_index_pointer: Option<PagePointer>,
    /// Field type
    pub field_type: FieldType,
    /// Whether nullable
    pub is_nullable: bool,
    /// Total memory footprint (data + index) in bytes
    pub total_mem_footprint: u64,
    /// Column statistics (optional)
    pub column_stats: Option<ColumnStatistics>,
    /// NULL count (optional)
    pub null_count: Option<u64>,
}

impl ColumnMeta {
    /// Create new column metadata.
    pub fn new(column_id: ColumnId, field_type: FieldType) -> Self {
        Self {
            column_id,
            num_rows: 0,
            encoding: EncodingType::Plain,
            compression: CompressionType::Lz4,
            data_page_pointer: PagePointer::new(0, 0),
            ordinal_index_pointer: PagePointer::new(0, 0),
            zonemap_index_pointer: PagePointer::new(0, 0),
            dict_page_pointer: None,
            bloom_filter_pointer: None,
            bitmap_index_pointer: None,
            hnsw_index_pointer: None,
            hnsw_index_statistics: None,
            sparse_index_pointer: None,
            fulltext_index_pointer: None,
            field_type,
            is_nullable: true,
            total_mem_footprint: 0,
            column_stats: None,
            null_count: None,
        }
    }
    /// Serialize to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);

        buf.extend_from_slice(&self.column_id.to_le_bytes());
        buf.extend_from_slice(&self.num_rows.to_le_bytes());
        buf.push(self.encoding as u8);
        buf.push(self.compression as u8);
        buf.extend_from_slice(&self.data_page_pointer.offset.to_le_bytes());
        buf.extend_from_slice(&self.data_page_pointer.size.to_le_bytes());
        buf.extend_from_slice(&self.ordinal_index_pointer.offset.to_le_bytes());
        buf.extend_from_slice(&self.ordinal_index_pointer.size.to_le_bytes());
        buf.extend_from_slice(&self.zonemap_index_pointer.offset.to_le_bytes());
        buf.extend_from_slice(&self.zonemap_index_pointer.size.to_le_bytes());

        macro_rules! write_opt_ptr {
            ($ptr:expr) => {
                if let Some(ptr) = $ptr {
                    buf.push(1);
                    buf.extend_from_slice(&ptr.offset.to_le_bytes());
                    buf.extend_from_slice(&ptr.size.to_le_bytes());
                } else {
                    buf.push(0);
                }
            };
        }

        write_opt_ptr!(self.dict_page_pointer);
        write_opt_ptr!(self.bloom_filter_pointer);
        write_opt_ptr!(self.bitmap_index_pointer);
        write_opt_ptr!(self.hnsw_index_pointer);
        write_opt_ptr!(self.sparse_index_pointer);
        write_opt_ptr!(self.fulltext_index_pointer);

        buf.push(self.field_type as u8);
        buf.push(u8::from(self.is_nullable));
        buf.extend_from_slice(&self.total_mem_footprint.to_le_bytes());

        if let (Some(stats), Some(null_count)) = (&self.column_stats, self.null_count) {
            buf.push(1);
            buf.extend_from_slice(&null_count.to_le_bytes());

            let mut type_buf = Vec::new();
            stats
                .statistics()
                .get_type()
                .serialize(&mut type_buf)
                .expect("serialize logical type");
            buf.extend_from_slice(&(type_buf.len() as u32).to_le_bytes());
            buf.extend_from_slice(&type_buf);

            let mut stats_buf = Vec::new();
            stats
                .serialize(&mut stats_buf)
                .expect("serialize column stats");
            buf.extend_from_slice(&(stats_buf.len() as u32).to_le_bytes());
            buf.extend_from_slice(&stats_buf);
        } else {
            buf.push(0);
        }

        if let Some(stats) = &self.hnsw_index_statistics {
            buf.push(1);
            buf.extend_from_slice(&stats.to_bytes());
        } else {
            buf.push(0);
        }

        buf
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 61 {
            return Err(paro_error::data_corrupted("ColumnMeta: data too short"));
        }

        let mut offset = 0usize;

        let column_id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let num_rows = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let encoding = EncodingType::from_u8(data[offset]).ok_or_else(|| {
            paro_error::data_corrupted(format!("ColumnMeta: invalid encoding {}", data[offset]))
        })?;
        offset += 1;

        let compression = CompressionType::from_u8(data[offset]).ok_or_else(|| {
            paro_error::data_corrupted(format!("ColumnMeta: invalid compression {}", data[offset]))
        })?;
        offset += 1;

        let data_page_pointer = PagePointer::new(
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()),
            u32::from_le_bytes(data[offset + 8..offset + 12].try_into().unwrap()),
        );
        offset += 12;

        let ordinal_index_pointer = PagePointer::new(
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()),
            u32::from_le_bytes(data[offset + 8..offset + 12].try_into().unwrap()),
        );
        offset += 12;

        let zonemap_index_pointer = PagePointer::new(
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()),
            u32::from_le_bytes(data[offset + 8..offset + 12].try_into().unwrap()),
        );
        offset += 12;

        fn read_opt_ptr(data: &[u8], offset: &mut usize) -> Result<Option<PagePointer>> {
            if *offset >= data.len() {
                return Err(paro_error::data_corrupted(
                    "ColumnMeta: missing optional pointer flag",
                ));
            }
            let has_ptr = data[*offset] != 0;
            *offset += 1;
            if !has_ptr {
                return Ok(None);
            }
            if *offset + 12 > data.len() {
                return Err(paro_error::data_corrupted(
                    "ColumnMeta: truncated optional pointer",
                ));
            }
            let ptr = PagePointer::new(
                u64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap()),
                u32::from_le_bytes(data[*offset + 8..*offset + 12].try_into().unwrap()),
            );
            *offset += 12;
            Ok(Some(ptr))
        }

        let dict_page_pointer = read_opt_ptr(data, &mut offset)?;
        let bloom_filter_pointer = read_opt_ptr(data, &mut offset)?;
        let bitmap_index_pointer = read_opt_ptr(data, &mut offset)?;
        let hnsw_index_pointer = read_opt_ptr(data, &mut offset)?;
        let sparse_index_pointer = read_opt_ptr(data, &mut offset)?;
        let fulltext_index_pointer = read_opt_ptr(data, &mut offset)?;

        if offset + 2 + 8 > data.len() {
            return Err(paro_error::data_corrupted(
                "ColumnMeta: truncated field/nullability metadata",
            ));
        }

        let field_type = FieldType::from_u8(data[offset]).ok_or_else(|| {
            paro_error::data_corrupted(format!("ColumnMeta: invalid field type {}", data[offset]))
        })?;
        offset += 1;

        let is_nullable = data[offset] != 0;
        offset += 1;

        let total_mem_footprint = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        if offset >= data.len() {
            return Err(paro_error::data_corrupted(
                "ColumnMeta: missing column statistics flag",
            ));
        }
        let (column_stats, null_count) = if data[offset] != 0 {
            offset += 1;

            if offset + 8 > data.len() {
                return Err(paro_error::data_corrupted(
                    "ColumnMeta: null_count truncated",
                ));
            }
            let null_count = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            if offset + 4 > data.len() {
                return Err(paro_error::data_corrupted(
                    "ColumnMeta: stats type length missing",
                ));
            }
            let type_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + type_len > data.len() {
                return Err(paro_error::data_corrupted(
                    "ColumnMeta: stats type truncated",
                ));
            }
            let mut type_cursor = std::io::Cursor::new(&data[offset..offset + type_len]);
            let logical_type = LogicalType::deserialize(&mut type_cursor)
                .map_err(|e| paro_error::data_corrupted(e.to_string()))?;
            offset += type_len;

            if offset + 4 > data.len() {
                return Err(paro_error::data_corrupted(
                    "ColumnMeta: stats length truncated",
                ));
            }
            let stats_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + stats_len > data.len() {
                return Err(paro_error::data_corrupted(
                    "ColumnMeta: stats bytes truncated",
                ));
            }
            let mut stats_cursor = std::io::Cursor::new(&data[offset..offset + stats_len]);
            let stats = ColumnStatistics::deserialize(&mut stats_cursor, logical_type)?;
            offset += stats_len;
            (Some(stats), Some(null_count))
        } else {
            offset += 1;
            (None, None)
        };

        // HNSW graph summaries live in the footer so metadata/planning never
        // has to read or decompress the graph page. Absence is accepted only
        // for a pre-summary footer, which remains useful for rebuilding an
        // unsupported search artifact without making base rows unreadable.
        let hnsw_index_statistics = if offset == data.len() {
            None
        } else {
            let has_stats = data[offset] != 0;
            offset += 1;
            if has_stats {
                let end = offset
                    .checked_add(HnswIndexStatistics::BYTE_LEN)
                    .ok_or_else(|| {
                        paro_error::data_corrupted("ColumnMeta: HNSW statistics overflow")
                    })?;
                let bytes = data.get(offset..end).ok_or_else(|| {
                    paro_error::data_corrupted("ColumnMeta: HNSW statistics truncated")
                })?;
                offset = end;
                Some(HnswIndexStatistics::from_bytes(bytes)?)
            } else {
                None
            }
        };
        if offset != data.len() {
            return Err(paro_error::data_corrupted(format!(
                "ColumnMeta: {} unexpected trailing bytes",
                data.len() - offset
            )));
        }

        Ok(Self {
            column_id,
            num_rows,
            encoding,
            compression,
            data_page_pointer,
            ordinal_index_pointer,
            zonemap_index_pointer,
            dict_page_pointer,
            bloom_filter_pointer,
            bitmap_index_pointer,
            hnsw_index_pointer,
            hnsw_index_statistics,
            sparse_index_pointer,
            fulltext_index_pointer,
            field_type,
            is_nullable,
            total_mem_footprint,
            column_stats,
            null_count,
        })
    }
}

/// Segment footer containing metadata.
#[derive(Debug, Clone)]
pub struct SegmentFooter {
    /// Segment version
    pub version: u32,
    /// Number of rows in segment
    pub num_rows: u64,
    /// Column metadata array
    pub column_metas: Vec<ColumnMeta>,
    /// Short key index pointer
    pub short_key_index_pointer: Option<PagePointer>,
    /// Short key footer for canonical short-key decoding
    pub short_key_index_footer: Option<ShortKeyFooter>,
    /// Footer checksum
    pub checksum: u32,
}

/// Magic number for segment footer.
const SEGMENT_FOOTER_MAGIC: u32 = 0x53454746; // "SEGF"

/// Current segment version.
const SEGMENT_VERSION: u32 = 1;

impl SegmentFooter {
    /// Create a new segment footer.
    pub fn new(num_rows: u64, column_metas: Vec<ColumnMeta>) -> Self {
        Self {
            version: SEGMENT_VERSION,
            num_rows,
            column_metas,
            short_key_index_pointer: None,
            short_key_index_footer: None,
            checksum: 0,
        }
    }

    /// Serialize footer to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);

        buf.extend_from_slice(&SEGMENT_FOOTER_MAGIC.to_le_bytes());
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&self.num_rows.to_le_bytes());
        buf.extend_from_slice(&(self.column_metas.len() as u32).to_le_bytes());

        for col_meta in &self.column_metas {
            let col_bytes = col_meta.serialize();
            buf.extend_from_slice(&(col_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&col_bytes);
        }

        if let Some(ptr) = self.short_key_index_pointer {
            buf.push(1);
            buf.extend_from_slice(&ptr.offset.to_le_bytes());
            buf.extend_from_slice(&ptr.size.to_le_bytes());
            if let Some(footer) = &self.short_key_index_footer {
                buf.push(1);
                buf.extend_from_slice(&footer.to_bytes());
            } else {
                buf.push(0);
            }
        } else {
            buf.push(0);
        }

        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        let footer_size = buf.len() as u32 + 4;
        buf.extend_from_slice(&footer_size.to_le_bytes());

        buf
    }

    /// Deserialize footer from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 24 {
            return Err(paro_error::data_corrupted("SegmentFooter: data too short"));
        }

        let mut offset = 0usize;

        let magic = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        if magic != SEGMENT_FOOTER_MAGIC {
            return Err(paro_error::data_corrupted(format!(
                "Invalid segment footer magic: expected {:08X}, got {:08X}",
                SEGMENT_FOOTER_MAGIC, magic
            )));
        }
        offset += 4;

        let version = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let num_rows = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let num_columns = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut column_metas = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            let col_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let col_meta = ColumnMeta::deserialize(&data[offset..offset + col_len])?;
            column_metas.push(col_meta);
            offset += col_len;
        }

        let has_short_key = data[offset] != 0;
        offset += 1;
        let (short_key_index_pointer, short_key_index_footer) = if has_short_key {
            let ptr_offset = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let ptr_size = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let ptr = Some(PagePointer::new(ptr_offset, ptr_size));

            let footer = if offset < data.len() - 4 && data[offset] != 0 {
                offset += 1;
                let end = offset + 24;
                if end > data.len() {
                    return Err(paro_error::data_corrupted(
                        "SegmentFooter: short key footer truncated",
                    ));
                }
                let footer = ShortKeyFooter::from_bytes(&data[offset..end])?;
                offset = end;
                Some(footer)
            } else {
                if offset < data.len() - 4 {
                    offset += 1;
                }
                None
            };
            (ptr, footer)
        } else {
            (None, None)
        };

        let checksum = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());

        Ok(Self {
            version,
            num_rows,
            column_metas,
            short_key_index_pointer,
            short_key_index_footer,
            checksum,
        })
    }
}

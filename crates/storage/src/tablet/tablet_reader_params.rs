use crate::buffer::Prefetcher;
use crate::index::PredicateTree;
use crate::rowset::segment::SegmentOptions;
use crate::tablet::{ColumnId, TabletRef};
use paro_common::allocator::Allocator;
use paro_common::error::Result;
use std::collections::HashMap;
use std::sync::Arc;

use super::tablet_reader::TabletReader;

#[derive(Debug, Clone)]
pub struct TabletReaderParams {
    pub version: i64,
    pub columns: Option<Vec<usize>>,
    pub projection: Option<ColumnProjection>,
    pub batch_size: usize,
    pub use_direct_io: bool,
    pub predicate_tree: Option<PredicateTree>,
    pub late_materialize: bool,
    pub predicate_columns: Option<Vec<ColumnId>>,
    pub segment_id: Option<u32>,
    pub segment_options: Option<SegmentOptions>,
    pub prefetcher: Option<Arc<Prefetcher>>,
    pub emit_row_id: bool,
}

#[derive(Debug, Clone)]
pub struct ColumnProjection {
    output_columns: Vec<usize>,
    read_columns: Vec<usize>,
    output_to_read: Vec<usize>,
}

impl ColumnProjection {
    pub fn new(output_columns: Vec<usize>) -> Self {
        let mut read_columns = Vec::new();
        let mut output_to_read = Vec::with_capacity(output_columns.len());
        let mut seen = HashMap::new();

        for &col in &output_columns {
            let read_idx = *seen.entry(col).or_insert_with(|| {
                let idx = read_columns.len();
                read_columns.push(col);
                idx
            });
            output_to_read.push(read_idx);
        }

        Self {
            output_columns,
            read_columns,
            output_to_read,
        }
    }

    pub fn output_columns(&self) -> &[usize] {
        &self.output_columns
    }

    pub fn read_columns(&self) -> &[usize] {
        &self.read_columns
    }

    pub fn output_to_read(&self) -> &[usize] {
        &self.output_to_read
    }
}

impl Default for TabletReaderParams {
    fn default() -> Self {
        Self {
            version: i64::MAX,
            columns: None,
            projection: None,
            batch_size: 4096,
            use_direct_io: false,
            predicate_tree: None,
            late_materialize: false,
            predicate_columns: None,
            segment_id: None,
            segment_options: None,
            prefetcher: None,
            emit_row_id: false,
        }
    }
}

impl TabletReaderParams {
    pub fn with_version(version: i64) -> Self {
        Self {
            version,
            ..Default::default()
        }
    }

    pub fn with_columns(mut self, columns: Vec<usize>) -> Self {
        self.columns = Some(columns);
        self.projection = None;
        self
    }

    pub fn with_projection(mut self, projection: ColumnProjection) -> Self {
        self.projection = Some(projection);
        self.columns = None;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_predicates(mut self, predicate_tree: PredicateTree) -> Self {
        self.predicate_tree = Some(predicate_tree);
        self
    }

    pub fn with_late_materialize(mut self, predicate_columns: Vec<ColumnId>) -> Self {
        self.late_materialize = true;
        self.predicate_columns = Some(predicate_columns);
        self
    }

    pub fn with_segment(mut self, segment_id: u32) -> Self {
        self.segment_id = Some(segment_id);
        self
    }

    pub fn with_segment_options(mut self, options: SegmentOptions) -> Self {
        self.segment_options = Some(options);
        self
    }

    pub fn with_prefetcher(mut self, prefetcher: Arc<Prefetcher>) -> Self {
        self.prefetcher = Some(prefetcher);
        self
    }

    pub fn with_emit_row_id(mut self, emit_row_id: bool) -> Self {
        self.emit_row_id = emit_row_id;
        self
    }
}

pub struct TabletReaderBuilder {
    tablet: TabletRef,
    params: TabletReaderParams,
    allocator: Option<Arc<dyn Allocator>>,
}

impl TabletReaderBuilder {
    pub fn new(tablet: TabletRef) -> Self {
        Self {
            tablet,
            params: TabletReaderParams::default(),
            allocator: None,
        }
    }

    pub fn version(mut self, version: i64) -> Self {
        self.params.version = version;
        self
    }

    pub fn columns(mut self, columns: Vec<usize>) -> Self {
        self.params.columns = Some(columns);
        self.params.projection = None;
        self
    }

    pub fn projection(mut self, projection: ColumnProjection) -> Self {
        self.params.projection = Some(projection);
        self.params.columns = None;
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.params.batch_size = batch_size;
        self
    }

    pub fn direct_io(mut self, enable: bool) -> Self {
        self.params.use_direct_io = enable;
        self
    }

    pub fn emit_row_id(mut self, emit_row_id: bool) -> Self {
        self.params.emit_row_id = emit_row_id;
        self
    }

    pub fn allocator(mut self, allocator: Arc<dyn Allocator>) -> Self {
        self.allocator = Some(allocator);
        self
    }

    pub fn build(self) -> Result<TabletReader> {
        if let Some(allocator) = self.allocator {
            TabletReader::new_with_allocator(self.tablet, self.params, allocator)
        } else {
            TabletReader::new(self.tablet, self.params)
        }
    }
}

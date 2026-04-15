/// Maximum number of row blocks addressable inside one row region.
pub const MAX_BLOCKS_PER_REGION: usize = 1 << 16;
/// Maximum number of rows addressable inside one row region by dense ordinal metadata.
pub const MAX_ROWS_PER_REGION: u64 = u32::MAX as u64;

/// Storage backing policy for a sealed row/heap block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockBacking {
    /// A plain in-memory block.
    InMemory,
    /// A block backed by the BufferPool and therefore eligible for eviction/reload.
    BufferPoolBacked,
}

/// Metadata for one sealed row block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowBlock {
    index: u16,
    row_count: u32,
    backing: BlockBacking,
}

impl RowBlock {
    pub(crate) fn new(index: u16, row_count: u32, backing: BlockBacking) -> Self {
        Self {
            index,
            row_count,
            backing,
        }
    }

    #[inline]
    pub fn index(&self) -> u16 {
        self.index
    }

    #[inline]
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    #[inline]
    pub fn backing(&self) -> BlockBacking {
        self.backing
    }
}

/// Metadata for one sealed heap block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapBlock {
    index: u32,
    backing: BlockBacking,
}

impl HeapBlock {
    pub(crate) fn new(index: u32, backing: BlockBacking) -> Self {
        Self { index, backing }
    }

    #[inline]
    pub fn index(&self) -> u32 {
        self.index
    }

    #[inline]
    pub fn backing(&self) -> BlockBacking {
        self.backing
    }
}

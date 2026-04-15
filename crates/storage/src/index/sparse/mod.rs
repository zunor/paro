//! # Sparse Vector Index Components
//!
//! Building blocks for sparse vector indexing (posting lists, inverted index, etc.).

mod builder;
mod inverted_index;
mod persistence;
mod posting_list;
mod search;
mod sparse_index;

pub use builder::SparseIndexBuilder;
pub use inverted_index::InvertedIndex;
pub use posting_list::{DocId, PostingElement, PostingList, Weight};
pub use search::{SparseSearchConfig, SparseSearchContext};
pub use sparse_index::{IndicesTracker, SparseVectorIndex};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::sparse_vector::SparseVector;
    use roaring::RoaringBitmap;

    #[test]
    fn test_sparse_index_build() {
        let mut builder = SparseIndexBuilder::new();
        let v0 = SparseVector::new(vec![3, 1], vec![0.5, 1.0]).unwrap();
        let v1 = SparseVector::new(vec![2, 1], vec![1.0, 0.2]).unwrap();
        builder.add(0, &v0).unwrap();
        builder.add(1, &v1).unwrap();
        let index = builder.build();

        let list = index.get_posting_list(1).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list.elements()[0].doc_id, 0);
        assert_eq!(list.elements()[1].doc_id, 1);

        let bytes = index.serialize().unwrap();
        let restored = SparseVectorIndex::deserialize(&bytes).unwrap();
        assert_eq!(restored.num_vectors(), 2);
        assert_eq!(restored.get_posting_list(1).unwrap().len(), 2);
    }

    #[test]
    fn test_sparse_search() {
        let mut builder = SparseIndexBuilder::new();
        let v0 = SparseVector::new(vec![1, 3], vec![1.0, 0.5]).unwrap();
        let v1 = SparseVector::new(vec![1, 2], vec![0.2, 1.0]).unwrap();
        let v2 = SparseVector::new(vec![2, 3], vec![0.4, 0.4]).unwrap();
        builder.add(0, &v0).unwrap();
        builder.add(1, &v1).unwrap();
        builder.add(2, &v2).unwrap();
        let index = builder.build();

        let query = SparseVector::new(vec![1, 2], vec![1.0, 1.0]).unwrap();
        let results = index.search(&query, 2, None).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].idx, 1);
        assert_eq!(results[1].idx, 0);
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn test_sparse_with_filter_bitmap() {
        let mut builder = SparseIndexBuilder::with_config(SparseSearchConfig::new(0.9));
        let v0 = SparseVector::new(vec![1], vec![1.0]).unwrap();
        let v1 = SparseVector::new(vec![1], vec![2.0]).unwrap();
        let v2 = SparseVector::new(vec![2], vec![3.0]).unwrap();
        builder.add(0, &v0).unwrap();
        builder.add(1, &v1).unwrap();
        builder.add(2, &v2).unwrap();
        let index = builder.build();

        let query = SparseVector::new(vec![1], vec![1.0]).unwrap();
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0);
        let results = index.search(&query, 10, Some(&bitmap)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].idx, 0);
    }

    #[test]
    fn test_sparse_filter_strategy_switch() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        let num_vectors = 10;
        assert!(super::search::should_plain_search(
            SparseSearchConfig::new(0.5),
            num_vectors,
            &bitmap
        ));
        assert!(!super::search::should_plain_search(
            SparseSearchConfig::new(0.05),
            num_vectors,
            &bitmap
        ));
    }

    #[test]
    fn test_sparse_high_dimension() {
        let mut builder = SparseIndexBuilder::new();
        let v0 = SparseVector::new(vec![30000], vec![1.0]).unwrap();
        builder.add(0, &v0).unwrap();
        let index = builder.build();

        let query = SparseVector::new(vec![30000], vec![1.0]).unwrap();
        let results = index.search(&query, 1, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].idx, 0);
    }
}

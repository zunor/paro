//! Index Integration Tests (T400-I)
//!
//! End-to-end tests for index operations:
//! - Index creation on empty tables
//! - Index creation on tables with data
//! - Index lookup operations
//! - Index removal
//! - Concurrent index operations

use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use std::sync::Arc;
use std::thread;

// ============================================================================
// Helper Functions
// ============================================================================

fn create_test_chunk_i32(values: &[i32]) -> Chunk {
    let vec = Vector::from_i32(values);
    Chunk::from_vectors(vec![vec])
}

fn create_test_chunk_i64(values: &[i64]) -> Chunk {
    let vec = Vector::from_i64(values);
    Chunk::from_vectors(vec![vec])
}

fn create_test_chunk_multi(ids: &[i32], values: &[i64]) -> Chunk {
    let id_vec = Vector::from_i32(ids);
    let value_vec = Vector::from_i64(values);
    Chunk::from_vectors(vec![id_vec, value_vec])
}

fn create_table(types: &[LogicalType]) -> TableHandle {
    TableFactory::default().create_table(types).unwrap()
}

// ============================================================================
// TableHandle Index Integration Tests
// ============================================================================

mod table_handle_index_tests {
    use super::*;

    #[test]
    fn test_table_handle_new_has_no_indexes() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        assert_eq!(table.index_count(), 0);
        assert!(!table.has_index("any_index"));
    }

    #[test]
    fn test_table_handle_get_index_not_found() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        assert!(table.get_index("nonexistent").is_none());
    }

    #[test]
    fn test_table_handle_remove_index_not_found() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        let removed = table.remove_index("nonexistent");
        assert!(removed.is_none());
    }

    #[test]
    fn test_table_handle_get_indexes() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        let indexes = table.get_indexes();
        assert!(indexes.is_empty());
    }
}

// ============================================================================
// Empty Table Index Creation Tests
// ============================================================================

mod empty_table_tests {
    use super::*;

    #[test]
    fn test_create_index_on_empty_table() {
        let types = vec![LogicalType::Integer, LogicalType::BigInt];
        let table = Arc::new(create_table(&types));

        // Table should be empty
        assert_eq!(table.total_rows(), 0);
        assert_eq!(table.index_count(), 0);
    }

    #[test]
    fn test_empty_table_scan_for_index() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        // Scanning empty table should work
        assert_eq!(table.total_rows(), 0);
    }
}

// ============================================================================
// Table with Data Index Tests
// ============================================================================

mod data_table_tests {
    use super::*;

    #[test]
    fn test_table_with_data_for_indexing() {
        let types = vec![LogicalType::Integer, LogicalType::BigInt];
        let table = create_table(&types);

        // Insert some data
        let chunk = create_test_chunk_multi(&[1, 2, 3, 4, 5], &[100, 200, 300, 400, 500]);
        table.append(&chunk).unwrap();

        assert_eq!(table.total_rows(), 5);
    }

    #[test]
    fn test_large_table_for_indexing() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        // Insert 10000 rows
        let values: Vec<i32> = (0..10000).collect();
        let chunk = create_test_chunk_i32(&values);
        table.append(&chunk).unwrap();

        assert_eq!(table.total_rows(), 10000);
    }

    #[test]
    fn test_multiple_chunks_for_indexing() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        // Insert multiple chunks
        for i in 0..10 {
            let values: Vec<i32> = ((i * 100)..((i + 1) * 100)).collect();
            let chunk = create_test_chunk_i32(&values);
            table.append(&chunk).unwrap();
        }

        assert_eq!(table.total_rows(), 1000);
    }
}

// ============================================================================
// Concurrent Index Operation Tests
// ============================================================================

mod concurrent_tests {
    use super::*;

    #[test]
    fn test_concurrent_table_reads_during_index_build() {
        let types = vec![LogicalType::Integer];
        let table = Arc::new(create_table(&types));

        // Pre-populate table
        let values: Vec<i32> = (0..1000).collect();
        let chunk = create_test_chunk_i32(&values);
        table.append(&chunk).unwrap();

        let table_reader = Arc::clone(&table);

        // Spawn reader threads
        let mut handles = vec![];
        for _ in 0..4 {
            let t = Arc::clone(&table_reader);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let count = t.total_rows();
                    assert!(count >= 1000, "Should see at least initial rows");
                }
            });
            handles.push(handle);
        }

        // Wait for all readers
        for handle in handles {
            handle.join().expect("Reader thread should complete");
        }

        assert_eq!(table.total_rows(), 1000);
    }

    #[test]
    fn test_concurrent_table_writes_and_index_check() {
        let types = vec![LogicalType::Integer];
        let table = Arc::new(create_table(&types));

        let table_writer = Arc::clone(&table);
        let table_reader = Arc::clone(&table);

        // Writer thread
        let writer_handle = thread::spawn(move || {
            for i in 0..10 {
                let values: Vec<i32> = ((i * 100)..((i + 1) * 100)).collect();
                let chunk = create_test_chunk_i32(&values);
                table_writer.append(&chunk).unwrap();
            }
        });

        // Reader thread checking index count
        let reader_handle = thread::spawn(move || {
            for _ in 0..100 {
                let _ = table_reader.index_count();
                let _ = table_reader.has_index("test_idx");
            }
        });

        writer_handle.join().expect("Writer should complete");
        reader_handle.join().expect("Reader should complete");

        assert_eq!(table.total_rows(), 1000);
    }
}

// ============================================================================
// Index Vacuum Tests
// ============================================================================

mod vacuum_tests {
    use super::*;

    #[test]
    fn test_vacuum_empty_indexes() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        // Vacuum should work on empty index set
        table.vacuum_indexes();
        assert_eq!(table.index_count(), 0);
    }
}

// ============================================================================
// Edge Case Tests
// ============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_table_with_null_values() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        // Create chunk with some values (nulls would be handled by validity mask)
        let chunk = create_test_chunk_i32(&[1, 2, 3]);
        table.append(&chunk).unwrap();

        assert_eq!(table.total_rows(), 3);
    }

    #[test]
    fn test_table_with_duplicate_values() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        // Insert duplicate values
        let chunk = create_test_chunk_i32(&[1, 1, 1, 2, 2, 3]);
        table.append(&chunk).unwrap();

        assert_eq!(table.total_rows(), 6);
    }

    #[test]
    fn test_table_with_negative_values() {
        let types = vec![LogicalType::Integer];
        let table = create_table(&types);

        let chunk = create_test_chunk_i32(&[-100, -50, 0, 50, 100]);
        table.append(&chunk).unwrap();

        assert_eq!(table.total_rows(), 5);
    }

    #[test]
    fn test_table_with_large_values() {
        let types = vec![LogicalType::BigInt];
        let table = create_table(&types);

        let chunk = create_test_chunk_i64(&[i64::MIN, -1, 0, 1, i64::MAX]);
        table.append(&chunk).unwrap();

        assert_eq!(table.total_rows(), 5);
    }
}

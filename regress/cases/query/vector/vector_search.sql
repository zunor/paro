-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Create table with vector column
CREATE TABLE items (id INT PRIMARY KEY, emb VECTOR(3));

-- Insert some data
INSERT INTO items VALUES (1, '[1.0, 1.0, 1.0]');
INSERT INTO items VALUES (2, '[2.0, 2.0, 2.0]');
INSERT INTO items VALUES (3, '[1.0, 1.0, 2.0]');
INSERT INTO items VALUES (4, NULL);

-- Check with filter
SELECT id FROM items WHERE id > 1 ORDER BY emb <-> '[1.0, 1.0, 1.0]', id LIMIT 2;

-- Check with filter that excludes everything
SELECT id FROM items WHERE id > 10 ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;

-- Check distance with NULL
SELECT id, emb <-> '[1.0, 1.0, 1.0]' as dist FROM items ORDER BY id;

-- Basic vector search (exact nearest neighbor)
EXPLAIN SELECT id FROM items ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;
SELECT id FROM items ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;

-- Exercise the indexed path on one inline-built rowset. Persist the complete,
-- versioned HNSW contract instead of relying on provider-local defaults.
CREATE TABLE indexed_items (id INT, emb VECTOR(3));
CREATE VECTOR INDEX idx_indexed_items_emb ON indexed_items (emb)
    distance = l2
    m = 8
    ef_construct = 32
    ef_search = 24
    build_seed = 7
    plain_scan_threshold = 10000
    filtered_plain_scan_threshold = 0
    inline_max_vector_count = 4096
    inline_max_graph_memory_bytes = 1048576
    inline_max_dimension = 3;
INSERT INTO indexed_items
SELECT i, '[1.0,1.0,1.0]'::VECTOR(3)
FROM generate_series(1, 2048) AS generated(i);
SELECT index_name, index_type FROM paro_indexes()
WHERE index_name = 'idx_indexed_items_emb';
-- @normalize explain_search_ids
EXPLAIN SELECT id FROM indexed_items
ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;

-- Prepared Top-K must preserve PostgreSQL's reusable $n parameter identity.
PREPARE vector_topk(VECTOR(3)) AS
    SELECT id FROM indexed_items ORDER BY emb <-> $1 LIMIT 2;
-- @query
EXECUTE vector_topk('[1.0, 1.0, 1.0]');
-- @query
EXECUTE vector_topk('[2.0, 2.0, 2.0]');
DEALLOCATE vector_topk;

-- The hint must reach the physical vector-search request.
-- @normalize explain_search_ids
EXPLAIN SELECT /*+ HNSW_EF(37) */ id
FROM indexed_items
ORDER BY emb <-> '[1.0, 1.0, 1.0]'
LIMIT 2;

-- Vector search with different target
SELECT id FROM items ORDER BY emb <-> '[2.0, 2.0, 2.0]' LIMIT 2;

-- Check distance calculation
SELECT id, emb <-> '[1.0, 1.0, 1.0]' as dist FROM items ORDER BY dist, id;

-- Drop table
DROP TABLE indexed_items;
DROP TABLE items;

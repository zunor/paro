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
CREATE TABLE indexed_items (id INT, bucket SMALLINT, emb VECTOR(3));
CREATE INDEX idx_indexed_items_id ON indexed_items (id);
CREATE INDEX idx_indexed_items_bucket ON indexed_items (bucket);
CREATE VECTOR INDEX idx_indexed_items_emb ON indexed_items (emb)
    distance = l2
    m = 8
    ef_construct = 32
    ef_search = 24
    build_seed = 7
    plain_scan_threshold = 0
    filtered_plain_scan_threshold = 8
    filter_columns = 'id,bucket'
    inline_max_vector_count = 4096
    inline_max_graph_memory_bytes = 1048576
    inline_max_dimension = 3;
INSERT INTO indexed_items
SELECT i, i % 10, '[1.0,1.0,1.0]'::VECTOR(3)
FROM generate_series(1, 2048) AS generated(i);
SELECT index_name, index_type FROM paro_indexes()
WHERE index_name = 'idx_indexed_items_emb';
-- @normalize explain_search_ids
EXPLAIN SELECT id FROM indexed_items
ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;

-- A scalar predicate must become part of VECTOR_SEARCH, not remain as a
-- relational FILTER above it. EXPLAIN also exposes the exact-vs-graph policy.
-- @normalize explain_search_ids
EXPLAIN SELECT id FROM indexed_items
WHERE bucket = 3
ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;

-- Keep SQL-level coverage for all adaptive stages. A singleton is an exact
-- bitmap scan, a nine-row range predicts two-hop refinement, and the bucket
-- predicate above predicts masked admission without refinement.
-- @normalize explain_search_ids
EXPLAIN SELECT id FROM indexed_items
WHERE id = 1
ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;
-- @normalize explain_search_ids
EXPLAIN SELECT id FROM indexed_items
WHERE id <= 9
ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;
SELECT id FROM indexed_items
WHERE id <= 9
ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;

-- A cosine operator must not consume an L2 artifact. Metric mismatch is a
-- capability miss and falls back to the exact relational plan.
-- @normalize explain_search_ids
EXPLAIN SELECT id FROM indexed_items
ORDER BY emb <=> '[1.0, 0.0, 0.0]' LIMIT 2;
SELECT id FROM indexed_items
ORDER BY emb <=> '[1.0, 0.0, 0.0]', id LIMIT 2;

-- Prepared Top-K must preserve PostgreSQL's reusable $n parameter identity.
PREPARE vector_topk(VECTOR(3)) AS
    SELECT emb <-> $1 AS dist
    FROM indexed_items
    ORDER BY emb <-> $1
    LIMIT 2;
-- @query
EXECUTE vector_topk('[1.0, 1.0, 1.0]');
-- @query
EXECUTE vector_topk('[2.0, 2.0, 2.0]');
DEALLOCATE vector_topk;

-- Reusable plans retain the scalar parameter and choose the filtered search
-- strategy from the exact bitmap cardinality when the source opens.
PREPARE filtered_vector_topk(INT, VECTOR(3)) AS
    SELECT bucket FROM indexed_items
    WHERE bucket = $1
    ORDER BY emb <-> $2 LIMIT 2;
-- Reuse the same prepared plan with two bindings. The returned column makes a
-- stale, plan-cached bitmap observable without depending on HNSW tie order.
-- @query
EXECUTE filtered_vector_topk(3, '[1.0, 1.0, 1.0]');
-- @query
EXECUTE filtered_vector_topk(7, '[1.0, 1.0, 1.0]');
-- An out-of-domain comparison is an empty predicate, not a narrowing-cast
-- error. This also protects parameter binding from changing global cast rules.
-- @query
EXECUTE filtered_vector_topk(100000, '[1.0, 1.0, 1.0]');
-- @normalize explain_search_ids
EXPLAIN EXECUTE filtered_vector_topk(3, '[1.0, 1.0, 1.0]');
DEALLOCATE filtered_vector_topk;

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

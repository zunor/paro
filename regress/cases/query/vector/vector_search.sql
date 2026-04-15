# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

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

-- Verify index creation
SELECT index_name, index_type FROM paro_indexes();

-- Basic vector search (exact nearest neighbor)
EXPLAIN SELECT id FROM items ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;
SELECT id FROM items ORDER BY emb <-> '[1.0, 1.0, 1.0]' LIMIT 2;

-- Vector search with different target
SELECT id FROM items ORDER BY emb <-> '[2.0, 2.0, 2.0]' LIMIT 2;

-- Check distance calculation
SELECT id, emb <-> '[1.0, 1.0, 1.0]' as dist FROM items ORDER BY dist, id;

-- Drop table
DROP TABLE items;

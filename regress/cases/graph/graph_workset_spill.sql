-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Graph workset spill regression tests (I2.7)
-- Covers:
-- 1. force_external correctness for multi-hop expand
-- 2. force_external correctness for shortest path single-source / bound target
-- 3. emit_path_info path reconstruction via path_length/vertices/edges
-- 4. large result streaming through HaveMoreOutput
-- 5. force_external uses the database-owned default temp directory after RESET

-- ============================================================
-- Part 1: Small graph for force_external + path reconstruction
-- ============================================================

CREATE TABLE gw_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE gw_follows (src_id BIGINT, dst_id BIGINT);

INSERT INTO gw_person VALUES
    (1, 'A'),
    (2, 'B'),
    (3, 'C'),
    (4, 'D'),
    (5, 'E');

INSERT INTO gw_follows VALUES
    (1, 2),
    (2, 3),
    (3, 4),
    (4, 5),
    (1, 3),
    (3, 5);

CREATE PROPERTY GRAPH gw_graph
VERTEX TABLES (
    gw_person LABEL Node
)
EDGE TABLES (
    gw_follows
        SOURCE KEY (src_id) REFERENCES gw_person (id)
        DESTINATION KEY (dst_id) REFERENCES gw_person (id)
        LABEL Follows
);

SET temp_directory = '/tmp/paro_regress_graph_workset_spill';
SET force_external = true;

-- WS1: force_external + multi-hop expand should preserve path reconstruction.
SELECT dst, hops, array_length(verts, 1) AS vertex_count, array_length(rels, 1) AS edge_count
FROM GRAPH_TABLE(gw_graph
    MATCH p = (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (b.name AS dst, path_length(p) AS hops, vertices(p) AS verts, edges(p) AS rels)
) gt
ORDER BY dst;

-- WS2: force_external + shortest path single-source should preserve path reconstruction.
SELECT dst, hops, array_length(verts, 1) AS vertex_count, array_length(rels, 1) AS edge_count
FROM GRAPH_TABLE(gw_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (b.name AS dst, path_length(p) AS hops, vertices(p) AS verts, edges(p) AS rels)
) gt
ORDER BY dst;

-- WS3: force_external + bound target shortest path should preserve reconstruction.
SELECT src, dst, hops, array_length(verts, 1) AS vertex_count, array_length(rels, 1) AS edge_count
FROM GRAPH_TABLE(gw_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node WHERE b.name = 'E')
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops, vertices(p) AS verts, edges(p) AS rels)
) gt;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;

SET force_external = true;
SELECT * FROM GRAPH_TABLE(gw_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (b.name AS dst)
) gt;
SET force_external = DEFAULT;

DROP PROPERTY GRAPH gw_graph;
DROP TABLE gw_follows;
DROP TABLE gw_person;

-- ============================================================
-- Part 2: Wide graph for large-result HaveMoreOutput coverage
-- ============================================================

CREATE TABLE gw_batch_node (id BIGINT PRIMARY KEY, kind VARCHAR);
CREATE TABLE gw_batch_edge (src_id BIGINT, dst_id BIGINT);

INSERT INTO gw_batch_node VALUES (1, 'root');
INSERT INTO gw_batch_node
SELECT i, 'mid'
FROM generate_series(2, 51) AS t(i);
INSERT INTO gw_batch_node
SELECT i, 'leaf'
FROM generate_series(52, 2551) AS t(i);

INSERT INTO gw_batch_edge
SELECT 1, i
FROM generate_series(2, 51) AS t(i);
INSERT INTO gw_batch_edge
SELECT 1 + ((i - 52) / 50) + 1, i
FROM generate_series(52, 2551) AS t(i);

CREATE PROPERTY GRAPH gw_batch_graph
VERTEX TABLES (
    gw_batch_node LABEL Node
)
EDGE TABLES (
    gw_batch_edge
        SOURCE KEY (src_id) REFERENCES gw_batch_node (id)
        DESTINATION KEY (dst_id) REFERENCES gw_batch_node (id)
        LABEL Link
);

SET temp_directory = '/tmp/paro_regress_graph_workset_spill';
SET force_external = true;

-- WS4: large multi-hop expand results should stream without losing rows.
SELECT count(*) AS total_rows, min(hops) AS min_hops, max(hops) AS max_hops
FROM GRAPH_TABLE(gw_batch_graph
    MATCH p = (a:Node WHERE a.id = 1)-[e:Link]->{1,2}(b:Node)
    COLUMNS (b.id AS dst, path_length(p) AS hops)
) gt;

-- WS5: large ANY SHORTEST results should stream through bounded output.
SELECT count(*) AS total_rows, min(hops) AS min_hops, max(hops) AS max_hops
FROM GRAPH_TABLE(gw_batch_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.id = 1)-[e:Link]->{1,2}(b:Node)
    COLUMNS (b.id AS dst, path_length(p) AS hops)
) gt;

-- WS6: large ALL SHORTEST results should stream through bounded output.
SELECT count(*) AS total_rows, min(dst) AS min_dst, max(dst) AS max_dst
FROM GRAPH_TABLE(gw_batch_graph
    MATCH ALL SHORTEST (a:Node WHERE a.id = 1)-[e:Link]->{1,2}(b:Node)
    COLUMNS (b.id AS dst)
) gt;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;

DROP PROPERTY GRAPH gw_batch_graph;
DROP TABLE gw_batch_edge;
DROP TABLE gw_batch_node;

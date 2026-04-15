-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- PhysicalGraphShortestPath regression tests (Task 4.2)
-- Tests: ANY SHORTEST and ALL SHORTEST path modes with BFS shortest path operator.
--
-- Graph topology:
--   A -> B -> C -> D -> E  (linear chain)
--   A -> C                 (shortcut)
--   B -> D                 (shortcut)
--
-- This creates multiple paths of different lengths between vertices,
-- allowing us to verify that shortest path semantics are correct.

CREATE TABLE sp_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE sp_follows (src_id BIGINT, dst_id BIGINT, weight INT);

INSERT INTO sp_person VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');
INSERT INTO sp_follows VALUES
    (1, 2, 10),
    (2, 3, 20),
    (3, 4, 30),
    (4, 5, 40),
    (1, 3, 50),
    (2, 4, 60);

CREATE PROPERTY GRAPH sp_graph
VERTEX TABLES (
    sp_person LABEL Node
)
EDGE TABLES (
    sp_follows
        SOURCE KEY (src_id) REFERENCES sp_person (id)
        DESTINATION KEY (dst_id) REFERENCES sp_person (id)
        LABEL Follows
);

-- SP1: ANY SHORTEST single hop — same as regular single hop
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,1}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- SP2: ANY SHORTEST multi-hop from A — should return each reachable vertex
-- at its shortest distance only (one row per destination)
-- A->B (1 hop), A->C (1 hop via shortcut), A->D (2 hops via A->C->D or A->B->D),
-- A->E (3 hops via A->C->D->E or A->B->D->E)
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- SP3: ANY SHORTEST from B — B->C (1 hop), B->D (1 hop via shortcut), B->E (2 hops)
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'B')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- SP4: ANY SHORTEST from E (leaf) — no outgoing edges, empty result
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'E')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- SP5: ALL SHORTEST from A with {1,4} — returns all shortest paths per destination
-- A->B (1 hop, 1 path), A->C (1 hop, 1 path),
-- A->D (2 hops, 2 paths: A->C->D and A->B->D),
-- A->E (3 hops, 2 paths: A->C->D->E and A->B->D->E)
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH ALL SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- SP6: ANY SHORTEST with + quantifier from A
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->+(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- SP7: ANY SHORTEST with path variable
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,2}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- SP8: ALL SHORTEST single hop from A — should return both direct neighbors
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH ALL SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,1}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- SP9: ANY SHORTEST with min_hops=2 — skip 1-hop results
-- From A: 2-hop destinations are D (via A->C->D or A->B->D)
-- 3-hop destination is E
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{2,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- SP10: ANY SHORTEST with multiple sources in one batch
-- Sources: A, B. Ensure lane-parallel BFS returns per-source shortest results.
SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name IN ('A', 'B'))-[e:Follows]->{1,2}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

-- SP10A: force_external should preserve bound-target shortest path reconstruction.
SET temp_directory = '/tmp/paro_regress_graph_spill';
SET force_external = true;

SELECT * FROM GRAPH_TABLE(sp_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node WHERE b.name = 'E')
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;

-- SP11: Sparse/dense switch with path_length (frontier crosses threshold)
-- Use 64 vertices so threshold = 1. Frontier expands from 1 -> 2 (dense).
CREATE TABLE sp_dense_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE sp_dense_follows (src_id BIGINT, dst_id BIGINT);

INSERT INTO sp_dense_person VALUES
    (1, 'N1'), (2, 'N2'), (3, 'N3'), (4, 'N4'), (5, 'N5'), (6, 'N6'), (7, 'N7'), (8, 'N8'),
    (9, 'N9'), (10, 'N10'), (11, 'N11'), (12, 'N12'), (13, 'N13'), (14, 'N14'), (15, 'N15'),
    (16, 'N16'), (17, 'N17'), (18, 'N18'), (19, 'N19'), (20, 'N20'), (21, 'N21'), (22, 'N22'),
    (23, 'N23'), (24, 'N24'), (25, 'N25'), (26, 'N26'), (27, 'N27'), (28, 'N28'), (29, 'N29'),
    (30, 'N30'), (31, 'N31'), (32, 'N32'), (33, 'N33'), (34, 'N34'), (35, 'N35'), (36, 'N36'),
    (37, 'N37'), (38, 'N38'), (39, 'N39'), (40, 'N40'), (41, 'N41'), (42, 'N42'), (43, 'N43'),
    (44, 'N44'), (45, 'N45'), (46, 'N46'), (47, 'N47'), (48, 'N48'), (49, 'N49'), (50, 'N50'),
    (51, 'N51'), (52, 'N52'), (53, 'N53'), (54, 'N54'), (55, 'N55'), (56, 'N56'), (57, 'N57'),
    (58, 'N58'), (59, 'N59'), (60, 'N60'), (61, 'N61'), (62, 'N62'), (63, 'N63'), (64, 'N64');

INSERT INTO sp_dense_follows VALUES
    (1, 2),
    (1, 3),
    (2, 4);

CREATE PROPERTY GRAPH sp_dense_graph
VERTEX TABLES (
    sp_dense_person LABEL Node
)
EDGE TABLES (
    sp_dense_follows
        SOURCE KEY (src_id) REFERENCES sp_dense_person (id)
        DESTINATION KEY (dst_id) REFERENCES sp_dense_person (id)
        LABEL Follows
);

SELECT * FROM GRAPH_TABLE(sp_dense_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.id = 1)-[e:Follows]->{1,2}(b:Node)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

DROP PROPERTY GRAPH sp_dense_graph;
DROP TABLE sp_dense_follows;
DROP TABLE sp_dense_person;

-- SP12: result set larger than VECTOR_SIZE should stream through HaveMoreOutput
-- without losing rows in lane mode.
CREATE TABLE sp_batch_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE sp_batch_follows (src_id BIGINT, dst_id BIGINT);

INSERT INTO sp_batch_person
SELECT i, 'B' || i::VARCHAR
FROM generate_series(1, 2051) AS t(i);

INSERT INTO sp_batch_follows
SELECT 1, i
FROM generate_series(2, 2051) AS t(i);

CREATE PROPERTY GRAPH sp_batch_graph
VERTEX TABLES (
    sp_batch_person LABEL Node
)
EDGE TABLES (
    sp_batch_follows
        SOURCE KEY (src_id) REFERENCES sp_batch_person (id)
        DESTINATION KEY (dst_id) REFERENCES sp_batch_person (id)
        LABEL Follows
);

SELECT count(*) AS total_rows, min(dst) AS min_dst, max(dst) AS max_dst
FROM GRAPH_TABLE(sp_batch_graph
    MATCH ANY SHORTEST (a:Node WHERE a.id = 1)-[e:Follows]->{1,1}(b:Node)
    COLUMNS (b.id AS dst)
) gt;

-- SP13: path rows must stay aligned with scalar rows across HaveMoreOutput boundaries.
SELECT count(*) AS total_rows, min(hops) AS min_hops, max(hops) AS max_hops
FROM GRAPH_TABLE(sp_batch_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.id = 1)-[e:Follows]->{1,1}(b:Node)
    COLUMNS (b.id AS dst, path_length(p) AS hops)
) gt;

-- SP14: ALL SHORTEST should also stream through the same bounded output path.
SELECT count(*) AS total_rows, min(dst) AS min_dst, max(dst) AS max_dst
FROM GRAPH_TABLE(sp_batch_graph
    MATCH ALL SHORTEST (a:Node WHERE a.id = 1)-[e:Follows]->{1,1}(b:Node)
    COLUMNS (b.id AS dst)
) gt;

-- SP15: force_external should preserve single-source path output and batching.
SET temp_directory = '/tmp/paro_regress_graph_spill';
SET force_external = true;

SELECT count(*) AS total_rows, min(hops) AS min_hops, max(hops) AS max_hops
FROM GRAPH_TABLE(sp_batch_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.id = 1)-[e:Follows]->{1,1}(b:Node)
    COLUMNS (b.id AS dst, path_length(p) AS hops)
) gt;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;

DROP PROPERTY GRAPH sp_batch_graph;
DROP TABLE sp_batch_follows;
DROP TABLE sp_batch_person;

-- Cleanup
DROP PROPERTY GRAPH sp_graph;
DROP TABLE sp_follows;
DROP TABLE sp_person;

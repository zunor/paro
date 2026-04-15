-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Path Functions regression tests (Task 4.3)
-- Tests: path_length(p) scalar function with path variables in GRAPH_TABLE.
--
-- Graph topology (same as graph_shortest_path.sql):
--   A -> B -> C -> D -> E  (linear chain)
--   A -> C                 (shortcut)
--   B -> D                 (shortcut)

CREATE TABLE pf_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE pf_follows (src_id BIGINT, dst_id BIGINT, weight INT);

INSERT INTO pf_person VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');
INSERT INTO pf_follows VALUES
    (1, 2, 10),
    (2, 3, 20),
    (3, 4, 30),
    (4, 5, 40),
    (1, 3, 50),
    (2, 4, 60);

CREATE PROPERTY GRAPH pf_graph
VERTEX TABLES (
    pf_person LABEL Node
)
EDGE TABLES (
    pf_follows
        SOURCE KEY (src_id) REFERENCES pf_person (id)
        DESTINATION KEY (dst_id) REFERENCES pf_person (id)
        LABEL Follows
);

-- PF1: path_length with ANY SHORTEST multi-hop
-- A->B (1 hop), A->C (1 hop), A->D (2 hops), A->E (3 hops)
SELECT * FROM GRAPH_TABLE(pf_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

-- PF2: path_length with regular multi-hop {1,3} from B
-- B->C (1 hop), B->D (1 hop via shortcut), B->E (2 hops)
SELECT * FROM GRAPH_TABLE(pf_graph
    MATCH p = (a:Node WHERE a.name = 'B')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

-- PF3: path_length with + quantifier from A
SELECT * FROM GRAPH_TABLE(pf_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->+(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

-- PF4: path_length single hop {1,1} — should always return 1
SELECT * FROM GRAPH_TABLE(pf_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,1}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

-- PF5: path_length from leaf node E — empty result
SELECT * FROM GRAPH_TABLE(pf_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'E')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

-- PF6: path_length with ALL SHORTEST — multiple shortest paths for D
-- A->D has 2 shortest paths (both 2 hops): A->C->D and A->B->D
SELECT * FROM GRAPH_TABLE(pf_graph
    MATCH p = ALL SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst, hops;

-- PF7: Combination of path_length with regular columns only (no path variable name conflict)
SELECT * FROM GRAPH_TABLE(pf_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,2}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY hops, dst;

-- PF8: path_length with min_hops=2 — skip 1-hop results
SELECT * FROM GRAPH_TABLE(pf_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{2,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

-- PF9: vertices(p) should return a path-preserving list of graph element refs
SELECT dst, array_length(verts, 1) AS vertex_count, verts IS NULL AS is_null
FROM GRAPH_TABLE(pf_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,2}(b:Node)
    COLUMNS (b.name AS dst, vertices(p) AS verts)
) gt;

-- PF10: edges(p) should return a path-preserving list of graph element refs
SELECT dst, array_length(rels, 1) AS edge_count, rels IS NULL AS is_null
FROM GRAPH_TABLE(pf_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,2}(b:Node)
    COLUMNS (b.name AS dst, edges(p) AS rels)
) gt;

-- PF11: element_id(p) should fail explicitly on path variables
SELECT * FROM GRAPH_TABLE(pf_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,2}(b:Node)
    COLUMNS (element_id(p) AS eid)
) gt;

-- Cleanup
DROP PROPERTY GRAPH pf_graph;
DROP TABLE pf_follows;
DROP TABLE pf_person;

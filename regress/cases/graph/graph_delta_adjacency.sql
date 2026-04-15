# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- DeltaAdjacency regression tests (Task 5.1)
-- Tests: incremental graph index updates after INSERT/DELETE on edge tables.
--
-- Graph topology (initial):
--   A -> B -> C  (linear chain)
--   A -> C       (shortcut)
--
-- After INSERT: A -> D (new edge)
-- After DELETE: A -> C shortcut removed

CREATE TABLE da_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE da_follows (id BIGINT PRIMARY KEY, src_id BIGINT, dst_id BIGINT, weight INT);

INSERT INTO da_person VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D');
INSERT INTO da_follows VALUES (1, 1, 2, 10), (2, 2, 3, 20), (3, 1, 3, 30);

CREATE PROPERTY GRAPH da_graph
VERTEX TABLES (
    da_person LABEL Node
)
EDGE TABLES (
    da_follows
        SOURCE KEY (src_id) REFERENCES da_person (id)
        DESTINATION KEY (dst_id) REFERENCES da_person (id)
        LABEL Follows
);

-- DA1: Baseline query — A can reach B and C directly
SELECT * FROM GRAPH_TABLE(da_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- DA2: Baseline multi-hop — A can reach B (1 hop), C (1 hop), C again via B (2 hops)
SELECT * FROM GRAPH_TABLE(da_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- DA3: Rebuild graph after INSERT — add edge A -> D
INSERT INTO da_follows VALUES (4, 1, 4, 40);

-- Rebuild the graph to pick up the new edge
DROP PROPERTY GRAPH da_graph;
CREATE PROPERTY GRAPH da_graph
VERTEX TABLES (
    da_person LABEL Node
)
EDGE TABLES (
    da_follows
        SOURCE KEY (src_id) REFERENCES da_person (id)
        DESTINATION KEY (dst_id) REFERENCES da_person (id)
        LABEL Follows
);

-- DA4: After rebuild — A can now reach B, C, and D directly
SELECT * FROM GRAPH_TABLE(da_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- DA5: Rebuild graph after DELETE — remove A -> C shortcut (rowid 3)
DELETE FROM da_follows WHERE id = 3;

DROP PROPERTY GRAPH da_graph;
CREATE PROPERTY GRAPH da_graph
VERTEX TABLES (
    da_person LABEL Node
)
EDGE TABLES (
    da_follows
        SOURCE KEY (src_id) REFERENCES da_person (id)
        DESTINATION KEY (dst_id) REFERENCES da_person (id)
        LABEL Follows
);

-- DA6: After delete rebuild — A reaches B and D directly, C only via B
SELECT * FROM GRAPH_TABLE(da_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- DA7: Multi-hop after delete — A reaches B (1), D (1), C (2 via B)
SELECT * FROM GRAPH_TABLE(da_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- DA8: Verify compaction scenario — multiple INSERT + DELETE then rebuild
INSERT INTO da_follows VALUES (5, 3, 1, 50);
INSERT INTO da_follows VALUES (6, 4, 2, 60);
DELETE FROM da_follows WHERE id = 2;

DROP PROPERTY GRAPH da_graph;
CREATE PROPERTY GRAPH da_graph
VERTEX TABLES (
    da_person LABEL Node
)
EDGE TABLES (
    da_follows
        SOURCE KEY (src_id) REFERENCES da_person (id)
        DESTINATION KEY (dst_id) REFERENCES da_person (id)
        LABEL Follows
);

-- DA9: After multiple mutations — edges: A->B(1), A->D(4), C->A(5), D->B(6)
-- B->C(2) was deleted
SELECT * FROM GRAPH_TABLE(da_graph
    MATCH (a:Node)-[e:Follows]->(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

-- DA10: Multi-hop reachability after compaction
SELECT * FROM GRAPH_TABLE(da_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- Cleanup
DROP PROPERTY GRAPH da_graph;
DROP TABLE da_follows;
DROP TABLE da_person;

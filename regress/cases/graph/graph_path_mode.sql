-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Path mode parser + quantifier extensions regression tests
-- Tests: + quantifier, * quantifier, path variable, path mode parsing
-- Note: path_mode and path_variable are parsed but not yet used by the execution layer (Phase 4).
-- This test verifies that the parser correctly accepts the new syntax and that
-- + and * quantifiers execute correctly end-to-end.

-- Setup: reuse the chain graph pattern
-- Graph: A -> B -> C -> D -> E (linear chain)
--         A -> C (shortcut)
CREATE TABLE pm_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE pm_follows (src_id BIGINT, dst_id BIGINT, weight INT);

INSERT INTO pm_person VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');
INSERT INTO pm_follows VALUES
    (1, 2, 10),
    (2, 3, 20),
    (3, 4, 30),
    (4, 5, 40),
    (1, 3, 50);

CREATE PROPERTY GRAPH pm_graph
VERTEX TABLES (
    pm_person LABEL Node
)
EDGE TABLES (
    pm_follows
        SOURCE KEY (src_id) REFERENCES pm_person (id)
        DESTINATION KEY (dst_id) REFERENCES pm_person (id)
        LABEL Follows
);

-- PM1: + quantifier (one or more hops) from A — should reach B,C,D,E
SELECT * FROM GRAPH_TABLE(pm_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->+(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PM2: + quantifier from E (leaf node) — no outgoing edges, empty result
SELECT * FROM GRAPH_TABLE(pm_graph
    MATCH (a:Node WHERE a.name = 'E')-[e:Follows]->+(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PM3: + quantifier from C — should reach D, E
SELECT * FROM GRAPH_TABLE(pm_graph
    MATCH (a:Node WHERE a.name = 'C')-[e:Follows]->+(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PM4: {1,4} from A — should match + result (B,C,D,E) since graph diameter is 4
SELECT * FROM GRAPH_TABLE(pm_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PM5: path variable syntax (parsed but not yet used by execution)
-- p = ... is accepted by parser; execution ignores path_variable
SELECT * FROM GRAPH_TABLE(pm_graph
    MATCH p = (a:Node WHERE a.name = 'A')-[e:Follows]->{1,2}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PM6: path variable + path mode syntax (parsed, execution ignores both)
SELECT * FROM GRAPH_TABLE(pm_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,2}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PM7: ANY path mode without path variable
SELECT * FROM GRAPH_TABLE(pm_graph
    MATCH ANY (a:Node WHERE a.name = 'B')-[e:Follows]->{1,2}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PM8: ALL SHORTEST path mode
SELECT * FROM GRAPH_TABLE(pm_graph
    MATCH ALL SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,1}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- Cleanup
DROP PROPERTY GRAPH pm_graph;
DROP TABLE pm_follows;
DROP TABLE pm_person;

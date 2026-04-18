-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- ExpandInto and bound-target shortest-path regression tests

CREATE TABLE ei_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE ei_knows (src_id BIGINT, dst_id BIGINT);
CREATE TABLE ei_seed (src_id BIGINT, dst_id BIGINT);
CREATE TABLE ei_route (src_id BIGINT, dst_id BIGINT);

INSERT INTO ei_person VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Carol'),
    (4, 'Dave'),
    (5, 'Eve');

INSERT INTO ei_knows VALUES
    (1, 2),
    (1, 3),
    (2, 1),
    (3, 1),
    (2, 4);

INSERT INTO ei_seed VALUES
    (2, 1),
    (3, 1);

INSERT INTO ei_route VALUES
    (2, 4),
    (4, 1),
    (3, 5),
    (5, 4);

CREATE PROPERTY GRAPH ei_graph
VERTEX TABLES (
    ei_person LABEL Node
)
EDGE TABLES (
    ei_knows
        SOURCE KEY (src_id) REFERENCES ei_person (id)
        DESTINATION KEY (dst_id) REFERENCES ei_person (id)
        LABEL Knows,
    ei_seed
        SOURCE KEY (src_id) REFERENCES ei_person (id)
        DESTINATION KEY (dst_id) REFERENCES ei_person (id)
        LABEL Seed,
    ei_route
        SOURCE KEY (src_id) REFERENCES ei_person (id)
        DESTINATION KEY (dst_id) REFERENCES ei_person (id)
        LABEL Route
);

-- EI1: singleton target filter should trigger ExpandInto-style fast path
SELECT * FROM GRAPH_TABLE(ei_graph
    MATCH (a:Node WHERE a.name = 'Alice')-[k:Knows]->(b:Node WHERE b.name = 'Bob')
    COLUMNS (a.name AS anchor, b.name AS neighbor)
) gt
ORDER BY neighbor;

-- EI2: EXPLAIN should surface the bound-target ExpandInto mode
EXPLAIN SELECT * FROM GRAPH_TABLE(ei_graph
    MATCH (a:Node WHERE a.name = 'Alice')-[k:Knows]->(b:Node WHERE b.name = 'Bob')
    COLUMNS (a.name AS anchor, b.name AS neighbor)
) gt;

-- EI3: singleton destination filter should trigger bound-target bidirectional BFS
SELECT * FROM GRAPH_TABLE(ei_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'Carol')-[r:Route]->{1,3}(b:Node WHERE b.name = 'Alice')
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

-- EI4: EXPLAIN should show the bidirectional bound-target shortest-path fast path
EXPLAIN SELECT * FROM GRAPH_TABLE(ei_graph
    MATCH ANY SHORTEST (a:Node WHERE a.name = 'Carol')-[r:Route]->{1,3}(b:Node WHERE b.name = 'Alice')
    COLUMNS (a.name AS src, b.name AS dst)
) gt;

DROP PROPERTY GRAPH ei_graph;
DROP TABLE ei_route;
DROP TABLE ei_seed;
DROP TABLE ei_knows;
DROP TABLE ei_person;

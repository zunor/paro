# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

DROP PROPERTY GRAPH IF EXISTS refresh_graph;
DROP TABLE IF EXISTS refresh_knows;
DROP TABLE IF EXISTS refresh_person;

CREATE TABLE refresh_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE refresh_knows (src_id BIGINT, dst_id BIGINT);

INSERT INTO refresh_person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
INSERT INTO refresh_knows VALUES (1, 2);

CREATE PROPERTY GRAPH refresh_graph
VERTEX TABLES (
    refresh_person LABEL Person
)
EDGE TABLES (
    refresh_knows
        SOURCE KEY (src_id) REFERENCES refresh_person (id)
        DESTINATION KEY (dst_id) REFERENCES refresh_person (id)
        LABEL Knows
);

SELECT src, dst FROM GRAPH_TABLE(refresh_graph
    MATCH (a:Person)-[e:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

INSERT INTO refresh_knows VALUES (2, 3);
REFRESH PROPERTY GRAPH refresh_graph;

SELECT src, dst FROM GRAPH_TABLE(refresh_graph
    MATCH (a:Person)-[e:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

DELETE FROM refresh_knows WHERE src_id = 1 AND dst_id = 2;
REFRESH PROPERTY GRAPH refresh_graph;

SELECT src, dst FROM GRAPH_TABLE(refresh_graph
    MATCH (a:Person)-[e:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

INSERT INTO refresh_person VALUES (4, 'Dave');
INSERT INTO refresh_knows VALUES (3, 4), (1, 3), (2, 4);
REFRESH PROPERTY GRAPH refresh_graph;

SELECT src, dst FROM GRAPH_TABLE(refresh_graph
    MATCH (a:Person)-[e:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

DROP PROPERTY GRAPH refresh_graph;
DROP TABLE refresh_knows;
DROP TABLE refresh_person;

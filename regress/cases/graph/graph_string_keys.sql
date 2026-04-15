-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE sk_person (id VARCHAR PRIMARY KEY, name VARCHAR);
CREATE TABLE sk_knows (src_id VARCHAR, dst_id VARCHAR, since INT);

INSERT INTO sk_person VALUES
    ('alice', 'Alice'),
    ('bob', 'Bob'),
    ('carol', 'Carol');

INSERT INTO sk_knows VALUES
    ('alice', 'bob', 2020),
    ('bob', 'carol', 2021);

CREATE PROPERTY GRAPH sk_graph
VERTEX TABLES (
    sk_person LABEL Person
)
EDGE TABLES (
    sk_knows
        SOURCE KEY (src_id) REFERENCES sk_person (id)
        DESTINATION KEY (dst_id) REFERENCES sk_person (id)
        LABEL Knows
);

SELECT * FROM GRAPH_TABLE(sk_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, k.since AS since)
) gt
ORDER BY src, dst;

DROP PROPERTY GRAPH sk_graph;
DROP TABLE sk_knows;
DROP TABLE sk_person;

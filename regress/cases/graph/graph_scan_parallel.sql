# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- GraphScan parallel source regression (T5.4.5.1)
-- Ensures parallel scan partitions do not drop or duplicate vertices.

CREATE TABLE ps_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE ps_self (src_id BIGINT, dst_id BIGINT);

INSERT INTO ps_person
SELECT i, 'P' || i::VARCHAR
FROM generate_series(1, 5000) AS t(i);

INSERT INTO ps_self
SELECT i, i
FROM generate_series(1, 5000) AS t(i);

CREATE PROPERTY GRAPH ps_graph
VERTEX TABLES (
    ps_person LABEL Person
)
EDGE TABLES (
    ps_self
        SOURCE KEY (src_id) REFERENCES ps_person (id)
        DESTINATION KEY (dst_id) REFERENCES ps_person (id)
        LABEL Self
);

SELECT count(*) AS total,
       count(DISTINCT src) AS distinct_src,
       min(src) AS min_id,
       max(src) AS max_id
FROM GRAPH_TABLE(ps_graph
    MATCH (a:Person)-[e:Self]->(b:Person)
    COLUMNS (a.id AS src, b.id AS dst)
) gt;

-- Cleanup
DROP PROPERTY GRAPH ps_graph;
DROP TABLE ps_self;
DROP TABLE ps_person;

-- Graph query single/multi-thread consistency (T5.4.5.1)
-- Verifies GraphScan parallelization yields identical results under threads=1 vs threads=4.

DROP PROPERTY GRAPH IF EXISTS gt_graph;
DROP TABLE IF EXISTS gt_edge;
DROP TABLE IF EXISTS gt_person;

CREATE TABLE gt_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE gt_edge (src_id BIGINT, dst_id BIGINT);

INSERT INTO gt_person
SELECT i, 'P' || i::VARCHAR
FROM generate_series(1, 5000) AS t(i);

-- Self edges + star edges from 1 to all
INSERT INTO gt_edge
SELECT i, i FROM generate_series(1, 5000) AS t(i);
INSERT INTO gt_edge
SELECT 1, i FROM generate_series(1, 5000) AS t(i);

CREATE PROPERTY GRAPH gt_graph
VERTEX TABLES (
    gt_person LABEL Person
)
EDGE TABLES (
    gt_edge
        SOURCE KEY (src_id) REFERENCES gt_person (id)
        DESTINATION KEY (dst_id) REFERENCES gt_person (id)
        LABEL Link
);

SET threads = 1;
SELECT count(*) AS total,
       count(DISTINCT src) AS distinct_src,
       count(DISTINCT dst) AS distinct_dst,
       min(src) AS min_src,
       max(src) AS max_src,
       min(dst) AS min_dst,
       max(dst) AS max_dst
FROM GRAPH_TABLE(gt_graph
    MATCH (a:Person)-[e:Link]->(b:Person)
    COLUMNS (a.id AS src, b.id AS dst)
) gt;

SET threads = 4;
SELECT count(*) AS total,
       count(DISTINCT src) AS distinct_src,
       count(DISTINCT dst) AS distinct_dst,
       min(src) AS min_src,
       max(src) AS max_src,
       min(dst) AS min_dst,
       max(dst) AS max_dst
FROM GRAPH_TABLE(gt_graph
    MATCH (a:Person)-[e:Link]->(b:Person)
    COLUMNS (a.id AS src, b.id AS dst)
) gt;

DROP PROPERTY GRAPH gt_graph;
DROP TABLE gt_edge;
DROP TABLE gt_person;

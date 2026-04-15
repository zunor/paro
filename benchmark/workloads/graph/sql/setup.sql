DROP PROPERTY GRAPH IF EXISTS bench_graph;
DROP TABLE IF EXISTS bench_edge;
DROP TABLE IF EXISTS bench_person;

CREATE TABLE bench_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE bench_edge (src_id BIGINT, dst_id BIGINT);

INSERT INTO bench_person
SELECT i, 'P' || i::VARCHAR
FROM generate_series(1, ${vertices}) AS t(i);

INSERT INTO bench_edge
SELECT i, i + 1
FROM generate_series(1, ${vertices_minus_one}) AS t(i);

INSERT INTO bench_edge
SELECT
    ((i * 17) % ${vertices}) + 1,
    ((i * 31) % ${vertices}) + 1
FROM generate_series(1, ${remaining_edges}) AS t(i);

CREATE PROPERTY GRAPH bench_graph
VERTEX TABLES (
    bench_person LABEL Person
)
EDGE TABLES (
    bench_edge
        SOURCE KEY (src_id) REFERENCES bench_person (id)
        DESTINATION KEY (dst_id) REFERENCES bench_person (id)
        LABEL Link
);

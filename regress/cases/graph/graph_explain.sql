-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Setup: create tables and insert data
CREATE TABLE person (id BIGINT PRIMARY KEY, name VARCHAR, age INT);
CREATE TABLE company (id BIGINT PRIMARY KEY, name VARCHAR, city VARCHAR);
CREATE TABLE knows (person1_id BIGINT, person2_id BIGINT, since DATE);
CREATE TABLE works_at (person_id BIGINT, company_id BIGINT, role VARCHAR);

INSERT INTO person VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);
INSERT INTO company VALUES (100, 'Acme', 'NYC'), (200, 'Beta', 'SF');
INSERT INTO knows VALUES (1, 2, '2020-01-01'), (2, 3, '2021-06-15'), (1, 3, '2019-03-20');
INSERT INTO works_at VALUES (1, 100, 'Engineer'), (2, 200, 'Manager'), (3, 100, 'Designer');

CREATE PROPERTY GRAPH social_network
VERTEX TABLES (
    person LABEL Person,
    company LABEL Company
)
EDGE TABLES (
    knows
        SOURCE KEY (person1_id) REFERENCES person (id)
        DESTINATION KEY (person2_id) REFERENCES person (id)
        LABEL Knows,
    works_at
        SOURCE KEY (person_id) REFERENCES person (id)
        DESTINATION KEY (company_id) REFERENCES company (id)
        LABEL WorksAt
);

-- EX1: EXPLAIN 一跳模式
EXPLAIN SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt;

-- EX2: EXPLAIN 两跳模式
EXPLAIN SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k1:Knows]->(b:Person)-[k2:Knows]->(c:Person)
    COLUMNS (a.name AS a_name, b.name AS b_name, c.name AS c_name)
) gt;

-- EX3: EXPLAIN 反向边
EXPLAIN SELECT * FROM GRAPH_TABLE(social_network
    MATCH (b:Person)<-[k:Knows]-(a:Person)
    COLUMNS (a.name AS src_name, b.name AS dst_name)
) gt;

-- EX4: EXPLAIN 跨 label (Person -> WorksAt -> Company)
EXPLAIN SELECT * FROM GRAPH_TABLE(social_network
    MATCH (p:Person)-[w:WorksAt]->(c:Company)
    COLUMNS (p.name AS person_name, c.name AS company_name)
) gt;

-- EX5: EXPLAIN multi-hop {1,3}
EXPLAIN SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt;

-- EX6: EXPLAIN ANY SHORTEST path
EXPLAIN SELECT * FROM GRAPH_TABLE(social_network
    MATCH ANY SHORTEST (a:Person)-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt;

-- EX7: EXPLAIN ALL SHORTEST path
EXPLAIN SELECT * FROM GRAPH_TABLE(social_network
    MATCH ALL SHORTEST (a:Person)-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt;

-- EX8: EXPLAIN ANALYZE 一跳模式
-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt;

-- EX9: EXPLAIN ANALYZE ANY SHORTEST path
-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE SELECT * FROM GRAPH_TABLE(social_network
    MATCH ANY SHORTEST (a:Person)-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt;

-- EX10: force_external should expose graph expand external status in EXPLAIN ANALYZE
SET temp_directory = '/tmp/paro_regress_graph_spill';
SET force_external = true;

-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt;

-- EX11: force_external should expose graph shortest path external status in EXPLAIN ANALYZE
-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE SELECT * FROM GRAPH_TABLE(social_network
    MATCH p = ANY SHORTEST (a:Person)-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name, path_length(p) AS hops)
) gt;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;

SET force_external = true;
-- The database-owned default temp directory remains available after RESET.
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt;
SET force_external = DEFAULT;

-- Cleanup
DROP PROPERTY GRAPH social_network;
DROP TABLE works_at;
DROP TABLE knows;
DROP TABLE company;
DROP TABLE person;

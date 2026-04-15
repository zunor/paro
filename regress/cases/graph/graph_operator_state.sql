-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- T5.4.0 Operator State Redesign Regression Tests
-- Verifies that GraphScan and GraphExpand operators work correctly
-- after state redesign (cached index handles, new state structs).

-- Setup: create tables and insert data
CREATE TABLE person (id BIGINT PRIMARY KEY, name VARCHAR, age INT);
CREATE TABLE company (id BIGINT PRIMARY KEY, name VARCHAR, city VARCHAR);
CREATE TABLE knows (person1_id BIGINT, person2_id BIGINT, since DATE);
CREATE TABLE works_at (person_id BIGINT, company_id BIGINT, role VARCHAR);

INSERT INTO person VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35), (4, 'Diana', 28);
INSERT INTO company VALUES (100, 'Acme', 'NYC'), (200, 'Beta', 'SF');
INSERT INTO knows VALUES (1, 2, '2020-01-01'), (2, 3, '2021-06-15'), (1, 3, '2019-03-20'), (3, 4, '2022-01-01'), (1, 4, '2023-05-10');
INSERT INTO works_at VALUES (1, 100, 'Engineer'), (2, 200, 'Manager'), (3, 100, 'Designer'), (4, 200, 'Analyst');

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

-- OS1: Basic scan + expand (verifies cached index in GraphScanLocalState)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY from_name, to_name;

-- OS2: Source vertex filter (verifies GraphScan state with filter)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

-- OS3: Target vertex filter (verifies GraphExpand state with target filter)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]->(b:Person WHERE b.age > 30)
    COLUMNS (a.name AS from_name, b.name AS to_name, b.age AS to_age)
) gt
ORDER BY from_name, to_name;

-- OS4: Cross-label expand (verifies cached index for different vertex labels)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (p:Person)-[w:WorksAt]->(c:Company)
    COLUMNS (p.name AS person_name, c.name AS company_name, w.role AS role_name)
) gt
ORDER BY person_name;

-- OS5: Multi-hop expansion (verifies GraphExpandOperatorState multi-hop fields)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,2}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

-- OS6: Backward edge (verifies cached CSR references for backward direction)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (b:Person)<-[k:Knows]-(a:Person)
    COLUMNS (a.name AS src_name, b.name AS dst_name)
) gt
ORDER BY src_name, dst_name;

-- OS7: Bidirectional edge (verifies both forward and backward CSR cached)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Bob')-[k:Knows]-(b:Person)
    COLUMNS (a.name AS left_name, b.name AS right_name)
) gt
ORDER BY right_name;

-- OS8: Multiple queries in sequence (verifies state reuse across queries)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Charlie')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Diana')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

-- OS9: Multi-hop {1,3} from Alice (verifies deep multi-hop state)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

-- OS10: Aggregation over graph query (verifies state works with downstream operators)
SELECT from_name, count(*) AS friend_count
FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name)
) gt
GROUP BY from_name
ORDER BY friend_count DESC, from_name;

-- Cleanup
DROP PROPERTY GRAPH social_network;
DROP TABLE works_at;
DROP TABLE knows;
DROP TABLE company;
DROP TABLE person;

-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- T5.4.0.3 / T5.4.0.4 Operator State & Index Cache Regression Tests
-- Verifies that:
-- 1. GraphShortestPath works correctly with BFSState (cached index handle)
-- 2. All graph operators (Scan, Expand, ShortestPath, Project) use cached
--    Arc<GraphProjectionIndex> from state, eliminating hot-path RwLock.
-- 3. Sequential queries reuse cached state correctly.

-- Setup: create tables and insert data
CREATE TABLE ic_person (id BIGINT PRIMARY KEY, name VARCHAR, age INT);
CREATE TABLE ic_company (id BIGINT PRIMARY KEY, name VARCHAR, city VARCHAR);
CREATE TABLE ic_knows (person1_id BIGINT, person2_id BIGINT, since DATE);
CREATE TABLE ic_works_at (person_id BIGINT, company_id BIGINT, role VARCHAR);

INSERT INTO ic_person VALUES
    (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35),
    (4, 'Diana', 28), (5, 'Eve', 40);
INSERT INTO ic_company VALUES (100, 'Acme', 'NYC'), (200, 'Beta', 'SF');
INSERT INTO ic_knows VALUES
    (1, 2, '2020-01-01'), (2, 3, '2021-06-15'), (1, 3, '2019-03-20'),
    (3, 4, '2022-01-01'), (4, 5, '2023-05-10'), (1, 5, '2023-08-01');
INSERT INTO ic_works_at VALUES
    (1, 100, 'Engineer'), (2, 200, 'Manager'),
    (3, 100, 'Designer'), (4, 200, 'Analyst'), (5, 100, 'CTO');

CREATE PROPERTY GRAPH ic_graph
VERTEX TABLES (
    ic_person LABEL Person,
    ic_company LABEL Company
)
EDGE TABLES (
    ic_knows
        SOURCE KEY (person1_id) REFERENCES ic_person (id)
        DESTINATION KEY (person2_id) REFERENCES ic_person (id)
        LABEL Knows,
    ic_works_at
        SOURCE KEY (person_id) REFERENCES ic_person (id)
        DESTINATION KEY (company_id) REFERENCES ic_company (id)
        LABEL WorksAt
);

-- IC1: ANY SHORTEST path (verifies GraphShortestPathState cached index)
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH ANY SHORTEST (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- IC2: ALL SHORTEST path (verifies BFS state with multiple paths)
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH ALL SHORTEST (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,2}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- IC3: Shortest path from non-root vertex (verifies state reset between queries)
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH ANY SHORTEST (a:Person WHERE a.name = 'Bob')-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- IC4: Shortest path from leaf vertex (empty result, verifies state handles no-output)
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH ANY SHORTEST (a:Person WHERE a.name = 'Eve')-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- IC5: Regular expand after shortest path (verifies Expand cached index is independent)
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

-- IC6: Cross-label expand (verifies cached index works for different edge/vertex labels)
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH (p:Person)-[w:WorksAt]->(c:Company)
    COLUMNS (p.name AS person_name, c.name AS company_name, w.role AS role_name)
) gt
ORDER BY person_name;

-- IC7: Multi-hop expand (verifies GraphExpandOperatorState cached index with multi-hop)
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,2}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

-- IC8: Backward edge with cached index
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH (b:Person)<-[k:Knows]-(a:Person WHERE a.name = 'Charlie')
    COLUMNS (a.name AS src_name, b.name AS dst_name)
) gt
ORDER BY dst_name;

-- IC9: Sequential shortest path queries (verifies state is fresh per query)
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH ANY SHORTEST (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,1}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH ANY SHORTEST (a:Person WHERE a.name = 'Charlie')-[k:Knows]->{1,2}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- IC10: Mixed operator pipeline — scan + expand + project with filter
-- (verifies all operators' cached state works in a single pipeline)
SELECT * FROM GRAPH_TABLE(ic_graph
    MATCH (a:Person WHERE a.age >= 30)-[k:Knows]->(b:Person WHERE b.age < 30)
    COLUMNS (a.name AS older_person, b.name AS younger_person, a.age AS older_age, b.age AS younger_age)
) gt
ORDER BY older_person, younger_person;

-- IC11: Aggregation over shortest path (verifies state works with downstream operators)
SELECT src, count(*) AS reachable_count
FROM GRAPH_TABLE(ic_graph
    MATCH ANY SHORTEST (a:Person)-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
GROUP BY src
ORDER BY reachable_count DESC, src;

-- IC12: Parallel scan with cached index (threads=4)
SET threads = 4;
SELECT count(*) AS edge_count
FROM GRAPH_TABLE(ic_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.id AS src)
) gt;
SET threads = 1;

-- Cleanup
DROP PROPERTY GRAPH ic_graph;
DROP TABLE ic_works_at;
DROP TABLE ic_knows;
DROP TABLE ic_company;
DROP TABLE ic_person;

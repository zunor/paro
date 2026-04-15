-- T5.4.2.1 / T5.4.2.2 Bulk Rowid Lookup Regression Tests
-- Verifies that:
-- 1. Segment::read_by_rowids correctly reads specified columns by row offsets
-- 2. TabletReader::get_by_rowids correctly routes cross-segment rowid lookups
-- 3. Graph queries with property projections produce correct results
--    (these exercise the rowid-based lookup path in GraphProject)
-- 4. Multiple vertex/edge property columns are correctly materialized
-- 5. Results remain correct with various data types and NULL values

-- Setup: create tables with diverse column types
CREATE TABLE br_person (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    age INT,
    score INT,
    city VARCHAR
);
CREATE TABLE br_company (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    founded INT,
    size INT
);
CREATE TABLE br_knows (
    person1_id BIGINT,
    person2_id BIGINT,
    since DATE,
    weight INT
);
CREATE TABLE br_works_at (
    person_id BIGINT,
    company_id BIGINT,
    role VARCHAR,
    salary INT
);

INSERT INTO br_person VALUES
    (1, 'Alice',   30, 95,  'NYC'),
    (2, 'Bob',     25, 88,  'SF'),
    (3, 'Charlie', 35, 72,  'NYC'),
    (4, 'Diana',   28, 91,  'LA'),
    (5, 'Eve',     40, 85,  'NYC'),
    (6, 'Frank',   22, 60,  'SF'),
    (7, 'Grace',   33, 78,  NULL),
    (8, 'Hank',    45, NULL, 'LA');

INSERT INTO br_company VALUES
    (100, 'Acme',  2000, 500),
    (200, 'Beta',  2010, 100),
    (300, 'Gamma', 2020, 50);

INSERT INTO br_knows VALUES
    (1, 2, '2020-01-01', 9),
    (1, 3, '2019-03-20', 8),
    (2, 3, '2021-06-15', 7),
    (3, 4, '2022-01-01', 6),
    (4, 5, '2023-05-10', 5),
    (1, 5, '2023-08-01', 4),
    (5, 6, '2024-01-01', 3),
    (6, 1, '2024-02-01', 2),
    (7, 8, '2023-06-01', 1),
    (8, 1, '2023-07-01', 10);

INSERT INTO br_works_at VALUES
    (1, 100, 'Engineer',  120000),
    (2, 200, 'Manager',   130000),
    (3, 100, 'Designer',  110000),
    (4, 300, 'Analyst',   90000),
    (5, 100, 'CTO',       200000),
    (6, 200, 'Intern',    40000),
    (7, 300, 'Consultant', 95000),
    (8, 100, 'VP',        180000);

CREATE PROPERTY GRAPH br_graph
VERTEX TABLES (
    br_person LABEL Person,
    br_company LABEL Company
)
EDGE TABLES (
    br_knows
        SOURCE KEY (person1_id) REFERENCES br_person (id)
        DESTINATION KEY (person2_id) REFERENCES br_person (id)
        LABEL Knows,
    br_works_at
        SOURCE KEY (person_id) REFERENCES br_person (id)
        DESTINATION KEY (company_id) REFERENCES br_company (id)
        LABEL WorksAt
);

-- BR1: Single vertex property projection (basic rowid lookup)
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY dst;

-- BR2: Multiple vertex properties from both source and destination
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, a.age AS src_age, a.score AS src_score,
             b.name AS dst, b.age AS dst_age, b.city AS dst_city)
) gt
ORDER BY dst;

-- BR3: Edge property projection (edge rowid lookup)
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, k.since AS since, k.weight AS weight)
) gt
ORDER BY dst;

-- BR4: Cross-label edge with multiple property projections
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (p:Person)-[w:WorksAt]->(c:Company)
    COLUMNS (p.name AS person, p.age AS age, c.name AS company,
             c.founded AS founded, w.role AS role, w.salary AS salary)
) gt
ORDER BY person;

-- BR5: Filtered query with property projection (combines predicate pushdown + rowid lookup)
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (a:Person WHERE a.age >= 30)-[k:Knows]->(b:Person WHERE b.age < 30)
    COLUMNS (a.name AS src, a.age AS src_age, b.name AS dst, b.age AS dst_age,
             k.weight AS weight)
) gt
ORDER BY src, dst;

-- BR6: Multi-hop with property projection
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,2}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY dst;

-- BR7: Shortest path with property projection
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH ANY SHORTEST (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.city AS dst_city)
) gt
ORDER BY dst;

-- BR8: Query involving NULL values in projected columns
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (a:Person WHERE a.name = 'Grace')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, a.city AS src_city, b.name AS dst, b.score AS dst_score)
) gt
ORDER BY dst;

-- BR9: Backward edge with property projection
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (b:Person)<-[k:Knows]-(a:Person WHERE a.name = 'Hank')
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age, k.weight AS weight)
) gt
ORDER BY dst;

-- BR10: All vertices expand with aggregation over projected properties
SELECT src, count(*) AS cnt, sum(dst_age) AS total_age
FROM GRAPH_TABLE(br_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.age AS dst_age)
) gt
GROUP BY src
ORDER BY cnt DESC, src;

-- BR11: Company properties with filter on integer company attributes
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (p:Person)-[w:WorksAt]->(c:Company WHERE c.size > 200)
    COLUMNS (p.name AS person, c.name AS company, c.size AS company_size, w.salary AS salary)
) gt
ORDER BY person;

-- BR12: Integer property correctness with WHERE filter
SELECT * FROM GRAPH_TABLE(br_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, a.score AS src_score, b.name AS dst, b.score AS dst_score,
             k.weight AS edge_weight)
) gt
WHERE gt.src_score > 80 AND gt.dst_score > 80
ORDER BY src, dst;

-- Cleanup
DROP PROPERTY GRAPH br_graph;
DROP TABLE br_works_at;
DROP TABLE br_knows;
DROP TABLE br_company;
DROP TABLE br_person;

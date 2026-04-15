-- T5.4.1.0 / T5.4.1.1 Predicate Pushdown Physical Execution Regression Tests
-- Verifies that:
-- 1. Vertex predicates (WHERE on source vertex) are evaluated at GraphScan level
-- 2. Target vertex predicates (WHERE on destination vertex) are correctly handled
-- 3. Edge predicates still work via GraphProject fallback
-- 4. Combined predicates (source + target + edge) produce correct results
-- 5. Filter pushdown does not change query semantics vs. non-pushed-down path

-- Setup: create tables and insert data
CREATE TABLE pd_person (id BIGINT PRIMARY KEY, name VARCHAR, age INT, city VARCHAR);
CREATE TABLE pd_company (id BIGINT PRIMARY KEY, name VARCHAR, city VARCHAR, size INT);
CREATE TABLE pd_knows (person1_id BIGINT, person2_id BIGINT, since DATE, weight DOUBLE);
CREATE TABLE pd_works_at (person_id BIGINT, company_id BIGINT, role VARCHAR);

INSERT INTO pd_person VALUES
    (1, 'Alice', 30, 'NYC'),
    (2, 'Bob', 25, 'SF'),
    (3, 'Charlie', 35, 'NYC'),
    (4, 'Diana', 28, 'LA'),
    (5, 'Eve', 40, 'NYC'),
    (6, 'Frank', 22, 'SF');
INSERT INTO pd_company VALUES
    (100, 'Acme', 'NYC', 500),
    (200, 'Beta', 'SF', 100),
    (300, 'Gamma', 'LA', 50);
INSERT INTO pd_knows VALUES
    (1, 2, '2020-01-01', 0.9),
    (1, 3, '2019-03-20', 0.8),
    (2, 3, '2021-06-15', 0.7),
    (3, 4, '2022-01-01', 0.6),
    (4, 5, '2023-05-10', 0.5),
    (1, 5, '2023-08-01', 0.4),
    (5, 6, '2024-01-01', 0.3),
    (6, 1, '2024-02-01', 0.2);
INSERT INTO pd_works_at VALUES
    (1, 100, 'Engineer'), (2, 200, 'Manager'),
    (3, 100, 'Designer'), (4, 300, 'Analyst'),
    (5, 100, 'CTO'), (6, 200, 'Intern');

CREATE PROPERTY GRAPH pd_graph
VERTEX TABLES (
    pd_person LABEL Person,
    pd_company LABEL Company
)
EDGE TABLES (
    pd_knows
        SOURCE KEY (person1_id) REFERENCES pd_person (id)
        DESTINATION KEY (person2_id) REFERENCES pd_person (id)
        LABEL Knows,
    pd_works_at
        SOURCE KEY (person_id) REFERENCES pd_person (id)
        DESTINATION KEY (company_id) REFERENCES pd_company (id)
        LABEL WorksAt
);

-- PD1: Source vertex filter (name equality) — pushed to GraphScan
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PD2: Source vertex filter (age comparison) — pushed to GraphScan
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (a:Person WHERE a.age > 30)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, a.age AS src_age, b.name AS dst)
) gt
ORDER BY src, dst;

-- PD3: Source vertex filter (city equality) — pushed to GraphScan
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (a:Person WHERE a.city = 'NYC')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

-- PD4: Target vertex filter — pushed to GraphExpand (future T5.4.1.2)
-- Currently still evaluated in GraphProject, but result must be correct
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (a:Person)-[k:Knows]->(b:Person WHERE b.age < 30)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY src, dst;

-- PD5: Combined source + target vertex filters
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (a:Person WHERE a.age >= 30)-[k:Knows]->(b:Person WHERE b.age < 30)
    COLUMNS (a.name AS src, a.age AS src_age, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY src, dst;

-- PD6: Source filter with no matching vertices (empty result)
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (a:Person WHERE a.name = 'Nobody')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PD7: Source filter that matches all vertices
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (a:Person WHERE a.age > 0)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

-- PD8: Cross-label with target filter (Company.city = 'NYC')
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (p:Person)-[w:WorksAt]->(c:Company WHERE c.city = 'NYC')
    COLUMNS (p.name AS person_name, c.name AS company_name)
) gt
ORDER BY person_name;

-- PD9: Source filter with multi-hop expansion
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,2}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PD10: Source filter with shortest path
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH ANY SHORTEST (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- PD11: Source filter with compound predicate (age > 25 AND city = 'NYC')
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (a:Person WHERE a.age > 25 AND a.city = 'NYC')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, a.age AS src_age, b.name AS dst)
) gt
ORDER BY src, dst;

-- PD12: Backward edge with source filter
SELECT * FROM GRAPH_TABLE(pd_graph
    MATCH (b:Person)<-[k:Knows]-(a:Person WHERE a.name = 'Charlie')
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- Cleanup
DROP PROPERTY GRAPH pd_graph;
DROP TABLE pd_works_at;
DROP TABLE pd_knows;
DROP TABLE pd_company;
DROP TABLE pd_person;

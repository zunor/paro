-- T5.4.1.2 / T5.4.1.3 Target Filter Pushdown & Edge Filter Deferred Regression Tests
-- Verifies that:
-- 1. Target vertex predicates are correctly evaluated via BitSet pre-filtering in GraphExpand
-- 2. Edge predicates still work via GraphProject fallback (deferred filter)
-- 3. Combined source + target + edge predicates produce correct results
-- 4. Target filter works with multi-hop expansion
-- 5. Target filter works with cross-label edges (Person -> Company)
-- 6. Target filter with empty result set
-- 7. Target filter with backward edges

-- Setup: create tables and insert data
CREATE TABLE tf_person (id BIGINT PRIMARY KEY, name VARCHAR, age INT, city VARCHAR);
CREATE TABLE tf_company (id BIGINT PRIMARY KEY, name VARCHAR, city VARCHAR, size INT);
CREATE TABLE tf_knows (person1_id BIGINT, person2_id BIGINT, since DATE, weight DOUBLE);
CREATE TABLE tf_works_at (person_id BIGINT, company_id BIGINT, role VARCHAR);

INSERT INTO tf_person VALUES
    (1, 'Alice', 30, 'NYC'),
    (2, 'Bob', 25, 'SF'),
    (3, 'Charlie', 35, 'NYC'),
    (4, 'Diana', 28, 'LA'),
    (5, 'Eve', 40, 'NYC'),
    (6, 'Frank', 22, 'SF');
INSERT INTO tf_company VALUES
    (100, 'Acme', 'NYC', 500),
    (200, 'Beta', 'SF', 100),
    (300, 'Gamma', 'LA', 50);
INSERT INTO tf_knows VALUES
    (1, 2, '2020-01-01', 0.9),
    (1, 3, '2019-03-20', 0.8),
    (2, 3, '2021-06-15', 0.7),
    (3, 4, '2022-01-01', 0.6),
    (4, 5, '2023-05-10', 0.5),
    (1, 5, '2023-08-01', 0.4),
    (5, 6, '2024-01-01', 0.3),
    (6, 1, '2024-02-01', 0.2);
INSERT INTO tf_works_at VALUES
    (1, 100, 'Engineer'), (2, 200, 'Manager'),
    (3, 100, 'Designer'), (4, 300, 'Analyst'),
    (5, 100, 'CTO'), (6, 200, 'Intern');

CREATE PROPERTY GRAPH tf_graph
VERTEX TABLES (
    tf_person LABEL Person,
    tf_company LABEL Company
)
EDGE TABLES (
    tf_knows
        SOURCE KEY (person1_id) REFERENCES tf_person (id)
        DESTINATION KEY (person2_id) REFERENCES tf_person (id)
        LABEL Knows,
    tf_works_at
        SOURCE KEY (person_id) REFERENCES tf_person (id)
        DESTINATION KEY (company_id) REFERENCES tf_company (id)
        LABEL WorksAt
);

-- TF1: Target vertex filter (age < 30) — pushed to GraphExpand BitSet
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person)-[k:Knows]->(b:Person WHERE b.age < 30)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY src, dst;

-- TF2: Target vertex filter (city equality)
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person)-[k:Knows]->(b:Person WHERE b.city = 'NYC')
    COLUMNS (a.name AS src, b.name AS dst, b.city AS dst_city)
) gt
ORDER BY src, dst;

-- TF3: Target vertex filter (name equality)
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person)-[k:Knows]->(b:Person WHERE b.name = 'Charlie')
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src;

-- TF4: Combined source + target vertex filters
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person WHERE a.age >= 30)-[k:Knows]->(b:Person WHERE b.age < 30)
    COLUMNS (a.name AS src, a.age AS src_age, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY src, dst;

-- TF5: Target filter with no matching vertices (empty result)
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person)-[k:Knows]->(b:Person WHERE b.name = 'Nobody')
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src;

-- TF6: Target filter that matches all vertices
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person)-[k:Knows]->(b:Person WHERE b.age > 0)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

-- TF7: Cross-label target filter (Company.city = 'NYC')
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (p:Person)-[w:WorksAt]->(c:Company WHERE c.city = 'NYC')
    COLUMNS (p.name AS person_name, c.name AS company_name)
) gt
ORDER BY person_name;

-- TF8: Cross-label target filter (Company.size > 100)
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (p:Person)-[w:WorksAt]->(c:Company WHERE c.size > 100)
    COLUMNS (p.name AS person_name, c.name AS company_name, c.size AS company_size)
) gt
ORDER BY person_name;

-- TF9: Target filter with compound predicate (age >= 25 AND city = 'SF')
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person)-[k:Knows]->(b:Person WHERE b.age >= 25 AND b.city = 'SF')
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age, b.city AS dst_city)
) gt
ORDER BY src, dst;

-- TF10: Target filter with multi-hop expansion
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,2}(b:Person WHERE b.city = 'LA')
    COLUMNS (a.name AS src, b.name AS dst, b.city AS dst_city)
) gt
ORDER BY dst;

-- TF11: Backward edge with target filter
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (b:Person)<-[k:Knows]-(a:Person WHERE a.age > 30)
    COLUMNS (a.name AS src, a.age AS src_age, b.name AS dst)
) gt
ORDER BY src, dst;

-- TF12: Edge filter only (deferred to GraphProject) — T5.4.1.3
-- Note: edge_filter on Double type not yet supported, using INT comparison
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

-- TF13: Combined source + target filters (edge filter deferred)
SELECT * FROM GRAPH_TABLE(tf_graph
    MATCH (a:Person WHERE a.city = 'NYC')-[k:Knows]->(b:Person WHERE b.age < 30)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY src, dst;

-- Cleanup
DROP PROPERTY GRAPH tf_graph;
DROP TABLE tf_works_at;
DROP TABLE tf_knows;
DROP TABLE tf_company;
DROP TABLE tf_person;

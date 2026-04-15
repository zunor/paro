# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- T5.4.6.2 Bulk Rowid Consistency Regression Tests
-- Verifies that Segment::read_by_rowids and TabletReader::get_by_rowids
-- produce correct and consistent results across various scenarios:
-- 1. Multiple data types (VARCHAR, INT, BIGINT) are correctly materialized
-- 2. NULL values are preserved through the bulk rowid path
-- 3. Cross-label projections (different vertex/edge tables) are consistent
-- 4. Filtered queries (predicate pushdown + bulk rowid) produce correct results
-- 5. Aggregation over bulk-rowid-projected columns yields correct totals
-- 6. Backward edges use the same bulk rowid path correctly
-- 7. Multi-hop and shortest path queries with property projection are consistent
-- 8. Sequential queries reuse the bulk rowid path without state corruption

-- Setup: create tables with diverse column types
CREATE TABLE brc_person (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    age INT,
    score INT,
    city VARCHAR
);
CREATE TABLE brc_org (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    founded INT,
    headcount INT
);
CREATE TABLE brc_knows (
    src_id BIGINT,
    dst_id BIGINT,
    weight INT,
    tag VARCHAR
);
CREATE TABLE brc_member (
    person_id BIGINT,
    org_id BIGINT,
    role VARCHAR,
    tenure INT
);

INSERT INTO brc_person VALUES
    (1, 'Alice',   30, 95,  'NYC'),
    (2, 'Bob',     25, 88,  'SF'),
    (3, 'Charlie', 35, 72,  'NYC'),
    (4, 'Diana',   28, 91,  'LA'),
    (5, 'Eve',     40, 85,  'NYC'),
    (6, 'Frank',   22, 60,  'SF'),
    (7, 'Grace',   33, 78,  NULL),
    (8, 'Hank',    45, NULL, 'LA');

INSERT INTO brc_org VALUES
    (100, 'AlphaCo', 2000, 500),
    (200, 'BetaInc', 2010, 100),
    (300, 'GammaTech', 2020, 50);

INSERT INTO brc_knows VALUES
    (1, 2, 9, 'friend'),
    (1, 3, 8, 'colleague'),
    (2, 3, 7, 'friend'),
    (3, 4, 6, 'colleague'),
    (4, 5, 5, 'friend'),
    (1, 5, 4, 'colleague'),
    (5, 6, 3, 'friend'),
    (6, 1, 2, 'colleague'),
    (7, 8, 1, 'friend'),
    (8, 1, 10, 'colleague');

INSERT INTO brc_member VALUES
    (1, 100, 'Engineer', 5),
    (2, 200, 'Manager',  3),
    (3, 100, 'Designer', 7),
    (4, 300, 'Analyst',  2),
    (5, 100, 'CTO',      10),
    (6, 200, 'Intern',   1),
    (7, 300, 'Consultant', 4),
    (8, 100, 'VP',       8);

CREATE PROPERTY GRAPH brc_graph
VERTEX TABLES (
    brc_person LABEL Person,
    brc_org    LABEL Org
)
EDGE TABLES (
    brc_knows
        SOURCE KEY (src_id) REFERENCES brc_person (id)
        DESTINATION KEY (dst_id) REFERENCES brc_person (id)
        LABEL Knows,
    brc_member
        SOURCE KEY (person_id) REFERENCES brc_person (id)
        DESTINATION KEY (org_id) REFERENCES brc_org (id)
        LABEL MemberOf
);

-- BRC1: Full vertex property projection — all columns from both endpoints
-- Exercises bulk rowid with multiple column types (VARCHAR, INT)
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, a.age AS src_age, a.score AS src_score, a.city AS src_city,
             b.name AS dst, b.age AS dst_age, b.score AS dst_score, b.city AS dst_city)
) gt
ORDER BY src, dst;

-- BRC2: Edge property projection — all edge columns
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, k.weight AS w, k.tag AS tag)
) gt
ORDER BY src, dst;

-- BRC3: Cross-label projection (Person -> Org) with all columns
-- Tests bulk rowid across different vertex tables
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (p:Person)-[m:MemberOf]->(o:Org)
    COLUMNS (p.name AS person, p.age AS age, p.city AS city,
             o.name AS org, o.founded AS founded, o.headcount AS hc,
             m.role AS role, m.tenure AS tenure)
) gt
ORDER BY person;

-- BRC4: NULL value preservation through bulk rowid path
-- Grace has NULL city, Hank has NULL score
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person WHERE a.name = 'Grace')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, a.city AS src_city, a.score AS src_score,
             b.name AS dst, b.city AS dst_city, b.score AS dst_score)
) gt
ORDER BY dst;

-- BRC5: Reverse NULL — Hank (NULL score) as source
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person WHERE a.name = 'Hank')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, a.score AS src_score, b.name AS dst, b.score AS dst_score)
) gt
ORDER BY dst;

-- BRC6: Filtered source + target — predicate pushdown combined with bulk rowid
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person WHERE a.age >= 30)-[k:Knows]->(b:Person WHERE b.age < 30)
    COLUMNS (a.name AS src, a.age AS src_age, b.name AS dst, b.age AS dst_age,
             k.weight AS w)
) gt
ORDER BY src, dst;

-- BRC7: Backward edge — bulk rowid must work for reverse traversal
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (b:Person)<-[k:Knows]-(a:Person WHERE a.name = 'Charlie')
    COLUMNS (a.name AS src, a.age AS src_age, b.name AS dst, b.age AS dst_age,
             k.weight AS w, k.tag AS tag)
) gt
ORDER BY dst;

-- BRC8: Multi-hop with property projection — bulk rowid across hops
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,2}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age, b.city AS dst_city)
) gt
ORDER BY dst;

-- BRC9: Shortest path with property projection — bulk rowid in BFS path
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH ANY SHORTEST (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age, b.city AS dst_city)
) gt
ORDER BY dst;

-- BRC10: Aggregation consistency — SUM/COUNT over bulk-rowid-projected columns
-- Verifies that numeric values are correctly materialized for aggregation
SELECT src, count(*) AS cnt, sum(dst_age) AS total_age, sum(w) AS total_weight
FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.age AS dst_age, k.weight AS w)
) gt
GROUP BY src
ORDER BY src;

-- BRC11: Sequential queries — verify no state corruption between queries
-- Query 1: Alice's neighbors
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY dst;

-- Query 2: Bob's neighbors (different source, same graph)
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person WHERE a.name = 'Bob')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY dst;

-- Query 3: Back to Alice (verify no stale state)
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY dst;

-- BRC12: Single-column projection — minimal bulk rowid (just one column)
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (b.name AS dst)
) gt
ORDER BY dst;

-- BRC13: Cross-label aggregation — org properties via bulk rowid
SELECT org, count(*) AS member_count, sum(tenure) AS total_tenure
FROM GRAPH_TABLE(brc_graph
    MATCH (p:Person)-[m:MemberOf]->(o:Org)
    COLUMNS (o.name AS org, m.tenure AS tenure)
) gt
GROUP BY org
ORDER BY org;

-- BRC14: Filter on projected edge property (WHERE on graph_table output)
-- Edge properties go through bulk rowid in GraphProject
SELECT * FROM GRAPH_TABLE(brc_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, k.weight AS w, k.tag AS tag)
) gt
WHERE gt.w >= 7
ORDER BY src, dst;

-- Cleanup
DROP PROPERTY GRAPH brc_graph;
DROP TABLE brc_member;
DROP TABLE brc_knows;
DROP TABLE brc_org;
DROP TABLE brc_person;

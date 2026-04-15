# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- T5.4.2.3 GraphProject Bulk Rowid Regression Tests
-- Verifies that PhysicalGraphProject::execute uses TabletReader::get_by_rowids
-- for O(n log n) bulk rowid lookup instead of O(table_size) full-table scan.
-- These tests exercise various property projection patterns through GraphProject.

-- Setup: create vertex and edge tables
CREATE TABLE gp_person (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    age INT,
    city VARCHAR
);
CREATE TABLE gp_org (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    founded INT
);
CREATE TABLE gp_friend (
    src_id BIGINT,
    dst_id BIGINT,
    since INT,
    strength INT
);
CREATE TABLE gp_member (
    person_id BIGINT,
    org_id BIGINT,
    role VARCHAR,
    years INT
);

INSERT INTO gp_person VALUES
    (1, 'Alice',   30, 'NYC'),
    (2, 'Bob',     25, 'SF'),
    (3, 'Charlie', 35, 'LA'),
    (4, 'Diana',   28, 'NYC'),
    (5, 'Eve',     40, NULL);

INSERT INTO gp_org VALUES
    (10, 'AlphaCo', 2000),
    (20, 'BetaInc', 2015);

INSERT INTO gp_friend VALUES
    (1, 2, 2020, 9),
    (1, 3, 2019, 7),
    (2, 4, 2021, 8),
    (3, 5, 2022, 5),
    (4, 1, 2023, 6);

INSERT INTO gp_member VALUES
    (1, 10, 'Engineer', 5),
    (2, 20, 'Manager',  3),
    (3, 10, 'Designer', 7),
    (4, 20, 'Analyst',  2),
    (5, 10, 'CTO',      10);

CREATE PROPERTY GRAPH gp_graph
VERTEX TABLES (
    gp_person LABEL Person,
    gp_org    LABEL Org
)
EDGE TABLES (
    gp_friend
        SOURCE KEY (src_id) REFERENCES gp_person (id)
        DESTINATION KEY (dst_id) REFERENCES gp_person (id)
        LABEL Friend,
    gp_member
        SOURCE KEY (person_id) REFERENCES gp_person (id)
        DESTINATION KEY (org_id) REFERENCES gp_org (id)
        LABEL MemberOf
);

-- GP1: Single source vertex property
SELECT * FROM GRAPH_TABLE(gp_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[f:Friend]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- GP2: Multiple properties from both endpoints
SELECT * FROM GRAPH_TABLE(gp_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[f:Friend]->(b:Person)
    COLUMNS (a.name AS src, a.age AS src_age, b.name AS dst, b.age AS dst_age, b.city AS dst_city)
) gt
ORDER BY dst;

-- GP3: Edge properties via bulk rowid
SELECT * FROM GRAPH_TABLE(gp_graph
    MATCH (a:Person)-[f:Friend]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, f.since AS since, f.strength AS strength)
) gt
ORDER BY src, dst;

-- GP4: Cross-label projection (Person -> Org)
SELECT * FROM GRAPH_TABLE(gp_graph
    MATCH (p:Person)-[m:MemberOf]->(o:Org)
    COLUMNS (p.name AS person, p.age AS age, o.name AS org, o.founded AS founded, m.role AS role, m.years AS years)
) gt
ORDER BY person;

-- GP5: Projection with NULL values in results
SELECT * FROM GRAPH_TABLE(gp_graph
    MATCH (a:Person)-[f:Friend]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.city AS dst_city)
) gt
ORDER BY src, dst;

-- GP6: Multi-hop with property projection (exercises bulk rowid across hops)
SELECT * FROM GRAPH_TABLE(gp_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[f:Friend]->{1,2}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY dst;

-- GP7: Aggregation over projected properties
SELECT src, count(*) AS cnt, sum(dst_age) AS total_age
FROM GRAPH_TABLE(gp_graph
    MATCH (a:Person)-[f:Friend]->(b:Person)
    COLUMNS (a.name AS src, b.age AS dst_age)
) gt
GROUP BY src
ORDER BY src;

-- GP8: Filter after projection (WHERE on graph_table output)
SELECT * FROM GRAPH_TABLE(gp_graph
    MATCH (a:Person)-[f:Friend]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, f.strength AS strength)
) gt
WHERE gt.strength >= 7
ORDER BY src, dst;

-- GP9: Shortest path with property projection
SELECT * FROM GRAPH_TABLE(gp_graph
    MATCH ANY SHORTEST (a:Person WHERE a.name = 'Alice')-[f:Friend]->{1,3}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.city AS dst_city)
) gt
ORDER BY dst;

-- Cleanup
DROP PROPERTY GRAPH gp_graph;
DROP TABLE gp_member;
DROP TABLE gp_friend;
DROP TABLE gp_org;
DROP TABLE gp_person;

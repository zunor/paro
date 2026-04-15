# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

CREATE TABLE ssp_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE ssp_company (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE ssp_city (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE ssp_works_at (person_id BIGINT, company_id BIGINT);
CREATE TABLE ssp_located_in (company_id BIGINT, city_id BIGINT);

INSERT INTO ssp_person VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie'),
    (4, 'Dora');
INSERT INTO ssp_company VALUES
    (10, 'Acme'),
    (20, 'Beta');
INSERT INTO ssp_city VALUES
    (100, 'NYC'),
    (200, 'SF');
INSERT INTO ssp_works_at VALUES
    (1, 10),
    (2, 20),
    (3, 10),
    (4, 20);
INSERT INTO ssp_located_in VALUES
    (10, 100),
    (20, 200);

CREATE PROPERTY GRAPH start_graph
VERTEX TABLES (
    ssp_person LABEL Person,
    ssp_company LABEL Company,
    ssp_city LABEL City
)
EDGE TABLES (
    ssp_works_at
        SOURCE KEY (person_id) REFERENCES ssp_person (id)
        DESTINATION KEY (company_id) REFERENCES ssp_company (id)
        LABEL WorksAt,
    ssp_located_in
        SOURCE KEY (company_id) REFERENCES ssp_company (id)
        DESTINATION KEY (city_id) REFERENCES ssp_city (id)
        LABEL LocatedIn
);

-- SS1: selective middle vertex should become the start of the graph plan
EXPLAIN SELECT * FROM GRAPH_TABLE(start_graph
    MATCH (p:Person)-[w:WorksAt]->(c:Company WHERE c.name = 'Acme')-[l:LocatedIn]->(city:City)
    COLUMNS (p.name AS person_name, c.name AS company_name, city.name AS city_name)
) gt;

-- SS2: middle-start execution must still materialize both branches correctly
SELECT * FROM GRAPH_TABLE(start_graph
    MATCH (p:Person)-[w:WorksAt]->(c:Company WHERE c.name = 'Acme')-[l:LocatedIn]->(city:City)
    COLUMNS (p.name AS person_name, c.name AS company_name, city.name AS city_name)
) gt
ORDER BY person_name;

DROP PROPERTY GRAPH start_graph;
DROP TABLE ssp_located_in;
DROP TABLE ssp_works_at;
DROP TABLE ssp_city;
DROP TABLE ssp_company;
DROP TABLE ssp_person;

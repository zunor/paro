-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP PROPERTY GRAPH IF EXISTS bad_string_key;
DROP PROPERTY GRAPH IF EXISTS bad_composite_key;
DROP TABLE IF EXISTS knows_str;
DROP TABLE IF EXISTS knows_pair;
DROP TABLE IF EXISTS person_pair;
DROP TABLE IF EXISTS person_str;

CREATE TABLE person (id BIGINT PRIMARY KEY, name VARCHAR, age INT);
CREATE TABLE company (id BIGINT PRIMARY KEY, name VARCHAR, city VARCHAR);
CREATE TABLE knows (person1_id BIGINT, person2_id BIGINT, since DATE);
CREATE TABLE works_at (person_id BIGINT, company_id BIGINT, role VARCHAR);

INSERT INTO person VALUES (1, 'Alice', 30), (2, 'Bob', NULL), (3, 'Charlie', 35);
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

SELECT * FROM GRAPH_TABLE(social_network
    MATCH (p:Person)
    COLUMNS (p.name AS name, p.age IS NULL AS age_is_null)
) gt
ORDER BY name;

DROP TABLE person;

CREATE PROPERTY GRAPH IF NOT EXISTS social_network
VERTEX TABLES (person)
EDGE TABLES (
    knows
        SOURCE KEY (person1_id) REFERENCES person (id)
        DESTINATION KEY (person2_id) REFERENCES person (id)
);

CREATE PROPERTY GRAPH social_network
VERTEX TABLES (person)
EDGE TABLES (
    knows
        SOURCE KEY (person1_id) REFERENCES person (id)
        DESTINATION KEY (person2_id) REFERENCES person (id)
);

DROP PROPERTY GRAPH social_network;

DROP PROPERTY GRAPH IF EXISTS social_network;

DROP PROPERTY GRAPH nonexistent;

DROP TABLE works_at;
DROP TABLE knows;
DROP TABLE company;
DROP TABLE person;

CREATE TABLE person_str (id VARCHAR PRIMARY KEY, name VARCHAR);
CREATE TABLE person_pair (id1 BIGINT, id2 BIGINT, name VARCHAR, PRIMARY KEY (id1, id2));
CREATE TABLE knows_str (src_id VARCHAR, dst_id VARCHAR);
CREATE TABLE knows_pair (src_id1 BIGINT, src_id2 BIGINT, dst_id1 BIGINT, dst_id2 BIGINT);

CREATE PROPERTY GRAPH bad_string_key
VERTEX TABLES (person_str)
EDGE TABLES (
    knows_str
        SOURCE KEY (src_id) REFERENCES person_str (id)
        DESTINATION KEY (dst_id) REFERENCES person_str (id)
);

CREATE PROPERTY GRAPH bad_composite_key
VERTEX TABLES (person_pair)
EDGE TABLES (
    knows_pair
        SOURCE KEY (src_id1, src_id2) REFERENCES person_pair (id1, id2)
        DESTINATION KEY (dst_id1, dst_id2) REFERENCES person_pair (id1, id2)
);

DROP PROPERTY GRAPH bad_composite_key;
DROP PROPERTY GRAPH bad_string_key;
DROP TABLE knows_str;
DROP TABLE knows_pair;
DROP TABLE person_pair;
DROP TABLE person_str;

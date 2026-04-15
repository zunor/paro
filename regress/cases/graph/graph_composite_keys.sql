# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

CREATE TABLE ck_person (
    tenant_id BIGINT,
    person_code VARCHAR,
    name VARCHAR,
    PRIMARY KEY (tenant_id, person_code)
);

CREATE TABLE ck_knows (
    tenant_id BIGINT,
    src_code VARCHAR,
    dst_code VARCHAR,
    since INT
);

INSERT INTO ck_person VALUES
    (1, 'alice', 'Alice'),
    (1, 'bob', 'Bob'),
    (1, 'carol', 'Carol'),
    (2, 'alice', 'Alice T2'),
    (2, 'bob', 'Bob T2');

INSERT INTO ck_knows VALUES
    (1, 'alice', 'bob', 2020),
    (1, 'bob', 'carol', 2021),
    (2, 'alice', 'bob', 2030);

CREATE PROPERTY GRAPH ck_graph
VERTEX TABLES (
    ck_person LABEL Person
)
EDGE TABLES (
    ck_knows
        SOURCE KEY (tenant_id, src_code) REFERENCES ck_person (tenant_id, person_code)
        DESTINATION KEY (tenant_id, dst_code) REFERENCES ck_person (tenant_id, person_code)
        LABEL Knows
);

SELECT * FROM GRAPH_TABLE(ck_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.tenant_id AS tenant, a.name AS src, b.name AS dst, k.since AS since)
) gt
ORDER BY tenant, src, dst;

DROP PROPERTY GRAPH ck_graph;
DROP TABLE ck_knows;
DROP TABLE ck_person;

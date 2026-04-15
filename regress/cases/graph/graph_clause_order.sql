CREATE TABLE gco_person (
    tenant_id BIGINT,
    person_code VARCHAR,
    name VARCHAR
);

CREATE TABLE gco_knows (
    src_tenant BIGINT,
    src_code VARCHAR,
    dst_tenant BIGINT,
    dst_code VARCHAR,
    since INT
);

INSERT INTO gco_person VALUES
    (1, 'alice', 'Alice'),
    (1, 'bob', 'Bob'),
    (2, 'alice', 'Alice T2'),
    (2, 'bob', 'Bob T2');

INSERT INTO gco_knows VALUES
    (1, 'alice', 1, 'bob', 2020),
    (2, 'alice', 2, 'bob', 2030);

CREATE PROPERTY GRAPH gco_graph
VERTEX TABLES (
    gco_person LABEL Person PROPERTIES (name) KEY (tenant_id, person_code)
)
EDGE TABLES (
    gco_knows
        SOURCE KEY (src_tenant, src_code) REFERENCES gco_person (tenant_id, person_code)
        DESTINATION KEY (dst_tenant, dst_code) REFERENCES gco_person (tenant_id, person_code)
        LABEL Knows
        PROPERTIES (since)
);

SELECT * FROM GRAPH_TABLE(gco_graph
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, k.since AS since)
) gt
ORDER BY src, dst, since;

DROP PROPERTY GRAPH gco_graph;
DROP TABLE gco_knows;
DROP TABLE gco_person;

-- ============================================================
-- Graph DML Hook
-- Phase 6.3.5: commit-time property-graph maintenance
-- ============================================================

CREATE TABLE gh_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE gh_knows (src_id BIGINT, dst_id BIGINT, note VARCHAR);

INSERT INTO gh_person VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
INSERT INTO gh_knows VALUES (1, 2, 'ab');

CREATE PROPERTY GRAPH gh_graph
VERTEX TABLES (
    gh_person LABEL Node
)
EDGE TABLES (
    gh_knows
        SOURCE KEY (src_id) REFERENCES gh_person (id)
        DESTINATION KEY (dst_id) REFERENCES gh_person (id)
        LABEL Knows
);

-- GH1: initial snapshot counts
SELECT graph_name, vertex_count, edge_count
FROM paro_property_graphs()
WHERE graph_name = 'gh_graph';

-- GH2: edge INSERT is auto-refreshed after commit
INSERT INTO gh_knows VALUES (2, 3, 'bc');

SELECT * FROM GRAPH_TABLE(gh_graph
    MATCH (a:Node)-[k:Knows]->(b:Node)
    COLUMNS (a.name AS src_name, b.name AS dst_name)
) gt
ORDER BY src_name, dst_name;

SELECT graph_name, vertex_count, edge_count
FROM paro_property_graphs()
WHERE graph_name = 'gh_graph';

-- GH3: endpoint UPDATE is auto-refreshed after commit
UPDATE gh_knows
SET dst_id = 1
WHERE src_id = 2 AND dst_id = 3;

SELECT * FROM GRAPH_TABLE(gh_graph
    MATCH (a:Node)-[k:Knows]->(b:Node)
    COLUMNS (a.name AS src_name, b.name AS dst_name)
) gt
ORDER BY src_name, dst_name;

-- GH4: edge DELETE is auto-refreshed after commit
DELETE FROM gh_knows
WHERE src_id = 1 AND dst_id = 2;

SELECT * FROM GRAPH_TABLE(gh_graph
    MATCH (a:Node)-[k:Knows]->(b:Node)
    COLUMNS (a.name AS src_name, b.name AS dst_name)
) gt
ORDER BY src_name, dst_name;

SELECT graph_name, vertex_count, edge_count
FROM paro_property_graphs()
WHERE graph_name = 'gh_graph';

DROP PROPERTY GRAPH gh_graph;
DROP TABLE gh_knows;
DROP TABLE gh_person;

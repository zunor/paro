-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- ============================================================
-- Graph System Tables / Introspection Functions
-- Task 5.3: paro_property_graphs() and paro_graph_statistics()
-- ============================================================

-- Setup: create base tables
CREATE TABLE person (id BIGINT PRIMARY KEY, name VARCHAR, age INT);
CREATE TABLE company (id BIGINT PRIMARY KEY, name VARCHAR, city VARCHAR);
CREATE TABLE knows (person1_id BIGINT, person2_id BIGINT, since DATE);
CREATE TABLE works_at (person_id BIGINT, company_id BIGINT, role VARCHAR);

INSERT INTO person VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);
INSERT INTO company VALUES (100, 'Acme', 'NYC'), (200, 'Beta', 'SF');
INSERT INTO knows VALUES (1, 2, '2020-01-01'), (2, 3, '2021-06-15'), (1, 3, '2019-03-20');
INSERT INTO works_at VALUES (1, 100, 'Engineer'), (2, 200, 'Manager'), (3, 100, 'Designer');

-- ST1: paro_property_graphs() returns empty when no graphs exist
SELECT * FROM paro_property_graphs();

-- Create a property graph
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

-- ST2: paro_property_graphs() shows the created graph
SELECT
    graph_name,
    vertex_tables,
    edge_tables,
    vertex_count,
    edge_count,
    state,
    delta_size,
    (last_rebuild IS NOT NULL) AS has_last_rebuild,
    (fingerprint <> '') AS has_fingerprint
FROM paro_property_graphs();

-- ST3: paro_graph_statistics() shows vertex and edge statistics
SELECT label, type, count FROM paro_graph_statistics('social_network') ORDER BY type, label;

-- ST4: paro_graph_statistics() shows avg_degree for edges
SELECT label, type, count, avg_degree FROM paro_graph_statistics('social_network') WHERE type = 'edge' ORDER BY label;

-- ST5: paro_graph_statistics() for non-existent graph returns empty
SELECT * FROM paro_graph_statistics('nonexistent');

-- ST6: index_size_bytes is positive for all labels
SELECT label, type, (index_size_bytes > 0) AS has_index FROM paro_graph_statistics('social_network') ORDER BY type, label;

-- ST7: edge DML updates state/delta metadata immediately
INSERT INTO knows VALUES (3, 1, '2024-01-01');
SELECT graph_name, state, delta_size, edge_count FROM paro_property_graphs();

-- ST8: vertex insert enters the async rebuild window; metadata stays internally consistent
INSERT INTO person VALUES (4, 'Dora', 28);
SELECT
    graph_name,
    (
        (state = 'STALE' AND delta_size = 1 AND vertex_count = 5)
        OR (state = 'READY' AND delta_size = 0 AND vertex_count = 6)
    ) AS async_window_valid
FROM paro_property_graphs();

-- ST9: REFRESH clears delta/stale metadata and rebuilds counts
REFRESH PROPERTY GRAPH social_network;
SELECT graph_name, state, delta_size, vertex_count, edge_count FROM paro_property_graphs();

-- ST10: statistics are refreshed after REFRESH PROPERTY GRAPH
SELECT label, type, count, avg_degree FROM paro_graph_statistics('social_network') WHERE type = 'edge' ORDER BY label;

-- Cleanup
DROP PROPERTY GRAPH social_network;
DROP TABLE works_at;
DROP TABLE knows;
DROP TABLE company;
DROP TABLE person;

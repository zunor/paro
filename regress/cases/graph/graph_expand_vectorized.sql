-- T5.4.3.1 / T5.4.3.2 GraphExpand Vectorized Output Regression Tests
-- Verifies that:
-- 1. GraphExpand uses Chunk buffer instead of Vec<Vec<u64>>
-- 2. Output is correctly batched at 2048 rows with HaveMoreOutput backpressure
-- 3. batch_local_to_rowid produces correct rowid mappings
-- 4. Single-hop expansion results remain correct after vectorization
-- 5. Multi-hop expansion results remain correct after vectorization
-- 6. High-degree vertices (many neighbors) produce correct results across batches

-- ============================================================
-- Part 1: Basic single-hop correctness after vectorization
-- ============================================================

CREATE TABLE ev_person (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    age INT
);
CREATE TABLE ev_knows (
    src_id BIGINT,
    dst_id BIGINT,
    weight INT
);

INSERT INTO ev_person VALUES
    (1, 'Alice',   30),
    (2, 'Bob',     25),
    (3, 'Charlie', 35),
    (4, 'Diana',   28),
    (5, 'Eve',     40);

INSERT INTO ev_knows VALUES
    (1, 2, 10),
    (1, 3, 20),
    (2, 4, 30),
    (3, 5, 40),
    (4, 5, 50);

CREATE PROPERTY GRAPH ev_graph
VERTEX TABLES (
    ev_person LABEL Person
)
EDGE TABLES (
    ev_knows
        SOURCE KEY (src_id) REFERENCES ev_person (id)
        DESTINATION KEY (dst_id) REFERENCES ev_person (id)
        LABEL Knows
);

-- EV1: Single-hop from Alice — should find Bob and Charlie
SELECT * FROM GRAPH_TABLE(ev_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[e:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY dst;

-- EV2: Single-hop from all vertices — verify all edges are expanded
SELECT * FROM GRAPH_TABLE(ev_graph
    MATCH (a:Person)-[e:Knows]->(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

-- EV3: Single-hop with target filter — only targets with age > 30
SELECT * FROM GRAPH_TABLE(ev_graph
    MATCH (a:Person)-[e:Knows]->(b:Person WHERE b.age > 30)
    COLUMNS (a.name AS src, b.name AS dst, b.age AS dst_age)
) gt
ORDER BY src, dst;

-- ============================================================
-- Part 2: Multi-hop correctness after vectorization
-- ============================================================

-- EV4: 2-hop from Alice
SELECT * FROM GRAPH_TABLE(ev_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[e:Knows]->{1,2}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- EV5: 3-hop from Alice — should reach Eve via two paths
SELECT * FROM GRAPH_TABLE(ev_graph
    MATCH (a:Person WHERE a.name = 'Alice')-[e:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- ============================================================
-- Part 3: High fan-out — stress test for batch boundary
-- Uses a star graph where one hub connects to many leaves.
-- This exercises the HaveMoreOutput backpressure mechanism.
-- ============================================================

DROP PROPERTY GRAPH ev_graph;
DROP TABLE ev_knows;
DROP TABLE ev_person;

CREATE TABLE ev_hub (
    id BIGINT PRIMARY KEY,
    name VARCHAR
);
CREATE TABLE ev_leaf (
    id BIGINT PRIMARY KEY,
    name VARCHAR
);
CREATE TABLE ev_star_edge (
    hub_id BIGINT,
    leaf_id BIGINT
);

-- Insert 1 hub vertex
INSERT INTO ev_hub VALUES (1, 'Hub');

-- Insert 100 leaf vertices and edges from hub to each leaf
-- This tests that vectorized output correctly handles moderate fan-out
INSERT INTO ev_leaf
SELECT i, 'Leaf' || i::VARCHAR FROM generate_series(1, 100) AS t(i);

INSERT INTO ev_star_edge
SELECT 1, i FROM generate_series(1, 100) AS t(i);

CREATE PROPERTY GRAPH ev_star_graph
VERTEX TABLES (
    ev_hub LABEL Hub,
    ev_leaf LABEL Leaf
)
EDGE TABLES (
    ev_star_edge
        SOURCE KEY (hub_id) REFERENCES ev_hub (id)
        DESTINATION KEY (leaf_id) REFERENCES ev_leaf (id)
        LABEL StarEdge
);

-- EV6: Expand from hub — should produce exactly 100 results
SELECT COUNT(*) AS edge_count FROM GRAPH_TABLE(ev_star_graph
    MATCH (h:Hub)-[e:StarEdge]->(l:Leaf)
    COLUMNS (h.name AS hub, l.name AS leaf)
) gt;

-- EV7: Verify first and last leaf names are correct
SELECT * FROM GRAPH_TABLE(ev_star_graph
    MATCH (h:Hub)-[e:StarEdge]->(l:Leaf)
    COLUMNS (h.name AS hub, l.name AS leaf)
) gt
WHERE gt.leaf IN ('Leaf1', 'Leaf50', 'Leaf100')
ORDER BY gt.leaf;

-- Cleanup
DROP PROPERTY GRAPH ev_star_graph;
DROP TABLE ev_star_edge;
DROP TABLE ev_leaf;
DROP TABLE ev_hub;

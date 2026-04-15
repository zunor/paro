-- T5.4.3.3 / T5.4.3.4 Multi-hop Vectorized Expansion Regression Tests
-- Verifies that:
-- 1. Multi-hop BFS uses FixedBitSet (Vec<u64>) instead of HashSet<u32> for visited
-- 2. Frontier uses dense Vec<u32> for cache-friendly traversal
-- 3. Multi-hop output is batch-limited via MultiHopState + HaveMoreOutput
-- 4. Path info (emit_path_info) correctly reports hop counts with parent tracking
-- 5. Cycle detection works correctly with BitSet visited set
-- 6. Target filter works correctly with vectorized multi-hop

-- ============================================================
-- Part 1: Basic multi-hop correctness with FixedBitSet visited
-- ============================================================

CREATE TABLE mv_person (id BIGINT PRIMARY KEY, name VARCHAR, city VARCHAR);
CREATE TABLE mv_follows (src_id BIGINT, dst_id BIGINT, weight INT);

-- Linear chain: A -> B -> C -> D -> E
-- Shortcut: A -> C
-- Cycle edge: D -> B (creates cycle B -> C -> D -> B)
INSERT INTO mv_person VALUES
    (1, 'A', 'NYC'),
    (2, 'B', 'SF'),
    (3, 'C', 'LA'),
    (4, 'D', 'NYC'),
    (5, 'E', 'SF');

INSERT INTO mv_follows VALUES
    (1, 2, 10),
    (2, 3, 20),
    (3, 4, 30),
    (4, 5, 40),
    (1, 3, 50),
    (4, 2, 60);

CREATE PROPERTY GRAPH mv_graph
VERTEX TABLES (
    mv_person LABEL Node
)
EDGE TABLES (
    mv_follows
        SOURCE KEY (src_id) REFERENCES mv_person (id)
        DESTINATION KEY (dst_id) REFERENCES mv_person (id)
        LABEL Follows
);

-- MV1: {1,1} single hop from A — should find B, C
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{1,1}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MV2: {1,4} from A — should find B, C, D, E (cycle detection prevents revisiting B)
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MV3: {2,4} from A — skip first hop, should find D, E
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{2,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MV4: {1,3} from B — cycle: B->C->D->B(visited), D->E
-- hop1: C, hop2: D, hop3: B(visited skip), E
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH (a:Node WHERE a.name = 'B')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MV5: From leaf E — no outgoing edges, empty result
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH (a:Node WHERE a.name = 'E')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MV6: {3,4} from A — only 3+ hops
-- hop1: B,C; hop2: D(from C or B); hop3: E(from D)
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{3,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- ============================================================
-- Part 2: Path info (T5.4.3.4) — path_length with parent tracking
-- ============================================================

-- MV7: path_length with multi-hop from A
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH p = (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

-- MV8: path_length with ANY SHORTEST from A
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH p = ANY SHORTEST (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

-- MV9: path_length from B with cycle
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH p = (a:Node WHERE a.name = 'B')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst, path_length(p) AS hops)
) gt
ORDER BY dst;

-- ============================================================
-- Part 3: Multi-hop with target filter
-- ============================================================

-- MV10: Multi-hop from A with target filter city = 'SF'
-- Only B(SF) and E(SF) should appear
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node WHERE b.city = 'SF')
    COLUMNS (a.name AS src, b.name AS dst, b.city AS dst_city)
) gt
ORDER BY dst;

-- MV11: Multi-hop from A with target filter city = 'NYC'
-- Only D(NYC) should appear (A is source, not target)
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node WHERE b.city = 'NYC')
    COLUMNS (a.name AS src, b.name AS dst, b.city AS dst_city)
) gt
ORDER BY dst;

-- ============================================================
-- Part 4: Multiple source vertices with multi-hop
-- ============================================================

-- MV12: Multi-hop from all vertices — tests batch processing across input rows
SELECT * FROM GRAPH_TABLE(mv_graph
    MATCH (a:Node)-[e:Follows]->{1,2}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY src, dst;

-- MV12A: force_external should preserve multi-hop correctness and path reconstruction.
SET temp_directory = '/tmp/paro_regress_graph_spill';
SET force_external = true;

SELECT count(*) AS total_rows, min(hops) AS min_hops, max(hops) AS max_hops
FROM GRAPH_TABLE(mv_graph
    MATCH p = (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (b.name AS dst, path_length(p) AS hops)
) gt;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;

-- ============================================================
-- Part 5: Wider graph for batch boundary testing
-- ============================================================

DROP PROPERTY GRAPH mv_graph;
DROP TABLE mv_follows;
DROP TABLE mv_person;

CREATE TABLE mv_hub (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE mv_mid (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE mv_leaf (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE mv_h2m (hub_id BIGINT, mid_id BIGINT);
CREATE TABLE mv_m2l (mid_id BIGINT, leaf_id BIGINT);

-- Hub -> 10 mid nodes -> 5 leaves each = 50 total leaf results at 2 hops
INSERT INTO mv_hub VALUES (1, 'Hub');
INSERT INTO mv_mid SELECT i, 'M' || i::VARCHAR FROM generate_series(1, 10) AS t(i);
INSERT INTO mv_leaf SELECT i, 'L' || i::VARCHAR FROM generate_series(1, 50) AS t(i);

-- Hub connects to all mid nodes
INSERT INTO mv_h2m SELECT 1, i FROM generate_series(1, 10) AS t(i);
-- Each mid node connects to 5 leaves: mid_i -> leaf_{(i-1)*5+1..i*5}
INSERT INTO mv_m2l SELECT ((i-1)/5)+1, i FROM generate_series(1, 50) AS t(i);

CREATE PROPERTY GRAPH mv_wide_graph
VERTEX TABLES (
    mv_hub LABEL Hub,
    mv_mid LABEL Mid,
    mv_leaf LABEL Leaf
)
EDGE TABLES (
    mv_h2m
        SOURCE KEY (hub_id) REFERENCES mv_hub (id)
        DESTINATION KEY (mid_id) REFERENCES mv_mid (id)
        LABEL H2M,
    mv_m2l
        SOURCE KEY (mid_id) REFERENCES mv_mid (id)
        DESTINATION KEY (leaf_id) REFERENCES mv_leaf (id)
        LABEL M2L
);

-- MV13: Single hop from Hub — should find 10 mid nodes
SELECT COUNT(*) AS mid_count FROM GRAPH_TABLE(mv_wide_graph
    MATCH (h:Hub)-[e:H2M]->(m:Mid)
    COLUMNS (h.name AS hub, m.name AS mid)
) gt;

-- MV14: Single hop from each mid — should find 5 leaves each, 50 total
SELECT COUNT(*) AS leaf_count FROM GRAPH_TABLE(mv_wide_graph
    MATCH (m:Mid)-[e:M2L]->(l:Leaf)
    COLUMNS (m.name AS mid, l.name AS leaf)
) gt;

-- MV15: Verify specific mid-to-leaf connections
SELECT * FROM GRAPH_TABLE(mv_wide_graph
    MATCH (m:Mid WHERE m.name = 'M1')-[e:M2L]->(l:Leaf)
    COLUMNS (m.name AS mid, l.name AS leaf)
) gt
ORDER BY leaf;

-- Cleanup
DROP PROPERTY GRAPH mv_wide_graph;
DROP TABLE mv_m2l;
DROP TABLE mv_h2m;
DROP TABLE mv_leaf;
DROP TABLE mv_mid;
DROP TABLE mv_hub;

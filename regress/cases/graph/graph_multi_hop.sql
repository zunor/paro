-- Multi-hop expand (BFS) regression tests
-- Tests bounded path quantifier {min,max} with various scenarios

-- Setup: a longer chain graph for multi-hop testing
-- Graph: A -> B -> C -> D -> E (linear chain)
--         A -> C (shortcut)
CREATE TABLE mh_person (id BIGINT PRIMARY KEY, name VARCHAR);
CREATE TABLE mh_follows (src_id BIGINT, dst_id BIGINT, weight INT);

INSERT INTO mh_person VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');
INSERT INTO mh_follows VALUES
    (1, 2, 10),
    (2, 3, 20),
    (3, 4, 30),
    (4, 5, 40),
    (1, 3, 50);

CREATE PROPERTY GRAPH mh_graph
VERTEX TABLES (
    mh_person LABEL Node
)
EDGE TABLES (
    mh_follows
        SOURCE KEY (src_id) REFERENCES mh_person (id)
        DESTINATION KEY (dst_id) REFERENCES mh_person (id)
        LABEL Follows
);

-- MH1: {1,1} 等价于单跳 (验收标准 1)
SELECT * FROM GRAPH_TABLE(mh_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{1,1}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MH2: {1,4} 从 A 出发，应到达 B,C,D,E (验收标准 2)
SELECT * FROM GRAPH_TABLE(mh_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{1,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MH3: {2,4} 跳过第一跳，从 A 出发
-- hop1: B,C (不输出); hop2: C已访问,D(从B或C); hop3: E(从D)
SELECT * FROM GRAPH_TABLE(mh_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{2,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MH4: {3,4} 从 A 出发
-- hop1: B,C; hop2: D; hop3: E → 输出 E
SELECT * FROM GRAPH_TABLE(mh_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{3,4}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MH5: 从末端 E 出发，无出边，空结果 (验收标准 4)
SELECT * FROM GRAPH_TABLE(mh_graph
    MATCH (a:Node WHERE a.name = 'E')-[e:Follows]->{1,3}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MH6: {1,1} 从中间节点 C 出发
SELECT * FROM GRAPH_TABLE(mh_graph
    MATCH (a:Node WHERE a.name = 'C')-[e:Follows]->{1,1}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MH7: {1,2} 从中间节点 B 出发
-- hop1: C; hop2: D
SELECT * FROM GRAPH_TABLE(mh_graph
    MATCH (a:Node WHERE a.name = 'B')-[e:Follows]->{1,2}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- MH8: {5,10} 超过图直径，从 A 出发
-- 图最长路径 4 跳，所以 min_hops=5 不会有结果
SELECT * FROM GRAPH_TABLE(mh_graph
    MATCH (a:Node WHERE a.name = 'A')-[e:Follows]->{5,10}(b:Node)
    COLUMNS (a.name AS src, b.name AS dst)
) gt
ORDER BY dst;

-- Cleanup
DROP PROPERTY GRAPH mh_graph;
DROP TABLE mh_follows;
DROP TABLE mh_person;

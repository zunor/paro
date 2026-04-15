# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- Setup: create tables and insert data (same as Phase 1)
CREATE TABLE person (id BIGINT PRIMARY KEY, name VARCHAR, age INT);
CREATE TABLE company (id BIGINT PRIMARY KEY, name VARCHAR, city VARCHAR);
CREATE TABLE knows (person1_id BIGINT, person2_id BIGINT, since DATE);
CREATE TABLE works_at (person_id BIGINT, company_id BIGINT, role VARCHAR);

INSERT INTO person VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);
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

-- Q1: 一跳模式匹配 + 顶点 WHERE 过滤 + DATE/VARCHAR 投影
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name, k.since AS since_date)
) gt;

-- Q2: 两跳路径（VARCHAR 投影）
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k1:Knows]->(b:Person)-[k2:Knows]->(c:Person)
    COLUMNS (a.name AS a_name, b.name AS b_name, c.name AS c_name)
) gt;

-- Q3: 反向边匹配
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (b:Person)<-[k:Knows]-(a:Person)
    COLUMNS (a.name AS src_name, b.name AS dst_name)
) gt
ORDER BY src_name, dst_name;

-- Q4: 无向边匹配
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]-(b:Person)
    COLUMNS (a.name AS left_name, b.name AS right_name)
) gt
ORDER BY left_name, right_name;

-- Q5: 跨 label 的模式 (Person -> WorksAt -> Company)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (p:Person)-[w:WorksAt]->(c:Company)
    COLUMNS (p.name AS person_name, c.name AS company_name, w.role AS role_name)
) gt
ORDER BY person_name;

-- Q6: 与普通 SQL 组合 (GROUP BY + ORDER BY)
SELECT from_name, count(*) AS friend_count
FROM GRAPH_TABLE(social_network
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.name AS from_name)
) gt
GROUP BY from_name
ORDER BY friend_count DESC, from_name;

-- Q7: 跨 label 反向边匹配 (Company <- WorksAt - Person)
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (c:Company)<-[w:WorksAt]-(p:Person)
    COLUMNS (c.name AS company_name, p.name AS person_name)
) gt
ORDER BY company_name, person_name;

-- Q8: 目标顶点过滤 (Company.city = 'NYC')
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (p:Person)-[w:WorksAt]->(c:Company WHERE c.city = 'NYC')
    COLUMNS (p.name AS person_name, c.name AS company_name)
) gt
ORDER BY person_name;

-- Q9: Multi-hop {1,1} 等价于单跳
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,1}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

-- Q10: Multi-hop {1,3} 返回 1-3 跳内所有可达顶点
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Alice')-[k:Knows]->{1,3}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

-- Q11: Multi-hop {1,2} 从 Bob 出发
SELECT * FROM GRAPH_TABLE(social_network
    MATCH (a:Person WHERE a.name = 'Bob')-[k:Knows]->{1,2}(b:Person)
    COLUMNS (a.name AS from_name, b.name AS to_name)
) gt
ORDER BY to_name;

-- Cleanup
DROP PROPERTY GRAPH social_network;
DROP TABLE works_at;
DROP TABLE knows;
DROP TABLE company;
DROP TABLE person;

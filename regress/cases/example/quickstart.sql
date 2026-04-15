# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- ============================================================
-- Paro Quick Start
--
-- Scenario: A research team's shared knowledge base.
-- Researchers collaborate with each other (graph), publish
-- papers with abstracts (full-text) and semantic embeddings
-- (vector). An agent finds the most relevant papers from a
-- researcher's collaboration network.
-- ============================================================

DROP PROPERTY GRAPH IF EXISTS collab_graph;
DROP INDEX IF EXISTS idx_papers_embedding;
DROP INDEX IF EXISTS idx_papers_abstract;
DROP TABLE IF EXISTS collaborations;
DROP TABLE IF EXISTS researchers;
DROP TABLE IF EXISTS papers;

-- ── Tables ──────────────────────────────────────────────────

CREATE TABLE researchers (
    id   BIGINT PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE collaborations (
    src_id BIGINT,
    dst_id BIGINT
);

CREATE TABLE papers (
    id        INT PRIMARY KEY,
    author_id BIGINT,
    title     VARCHAR,
    abstract  VARCHAR,
    embedding VECTOR(4)
);

-- ── Data ────────────────────────────────────────────────────

INSERT INTO researchers VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Carol'),
    (4, 'David'),
    (5, 'Eve'),
    (6, 'Frank');

-- Alice collaborates with Bob and Carol;
-- Bob collaborates with David; Carol collaborates with Eve.
-- Frank is outside Alice's network.
INSERT INTO collaborations VALUES
    (1, 2),
    (1, 3),
    (2, 4),
    (3, 5);

INSERT INTO papers VALUES
    (101, 2, 'Retrieval-Augmented Generation for Knowledge-Intensive Tasks',
             'We combine retrieval mechanisms with generative models to ground language model outputs in external knowledge, reducing hallucination in open-domain question answering.',
             '[0.90, 0.12, 0.78, 0.25]'),
    (102, 3, 'Efficient Nearest Neighbor Search in High-Dimensional Spaces',
             'This paper presents an optimized graph-based index for approximate nearest neighbor search, achieving sub-millisecond latency on billion-scale vector datasets.',
             '[0.33, 0.88, 0.15, 0.71]'),
    (103, 4, 'Grounding Language Agents with Retrieval Memory',
             'We propose a retrieval-augmented memory architecture that allows autonomous agents to access and reason over large external knowledge bases during multi-step planning.',
             '[0.91, 0.10, 0.81, 0.22]'),
    (104, 5, 'BM25 Revisited: Tuning Lexical Relevance for Modern Corpora',
             'An empirical study on adapting classical BM25 scoring to web-scale corpora with long documents and heterogeneous retrieval workloads.',
             '[0.18, 0.79, 0.22, 0.85]'),
    (105, 6, 'Scaling Retrieval Pipelines for Autonomous Agents',
             'A systems paper on building low-latency retrieval pipelines that serve autonomous agents performing knowledge-intensive reasoning tasks.',
             '[0.88, 0.15, 0.76, 0.30]'),
    (106, 4, 'Multi-Hop Reasoning over Heterogeneous Knowledge Graphs',
             'We introduce a query planner that decomposes complex questions into sub-queries over typed graph edges, enabling multi-hop retrieval with provenance tracking.',
             '[0.55, 0.40, 0.60, 0.50]');

-- ── Indexes ─────────────────────────────────────────────────

CREATE VECTOR INDEX idx_papers_embedding ON papers (embedding);
CREATE INDEX idx_papers_abstract ON papers USING GIN (to_tsvector('simple', abstract));

-- ── Property Graph ──────────────────────────────────────────

CREATE PROPERTY GRAPH collab_graph
VERTEX TABLES (
    researchers LABEL Researcher
)
EDGE TABLES (
    collaborations
        SOURCE KEY (src_id) REFERENCES researchers (id)
        DESTINATION KEY (dst_id) REFERENCES researchers (id)
        LABEL CollaboratesWith
);

-- ── Hybrid Query ────────────────────────────────────────────
--
-- Goal: An agent is researching "retrieval-augmented generation
-- for autonomous agents". Find the most relevant papers from
-- Alice's collaboration network (up to 2 hops), ranking by a
-- hybrid score that blends semantic similarity with lexical
-- relevance.

WITH network AS (
    SELECT * FROM GRAPH_TABLE(collab_graph
        MATCH (me:Researcher WHERE me.name = 'Alice')
              -[:CollaboratesWith]->{1,2}(peer:Researcher)
        COLUMNS (peer.id AS author_id, peer.name AS author_name)
    )
),
candidates AS (
    SELECT
        id,
        title,
        author_id,
        abstract,
        1.0 / (1.0 + (embedding <-> '[0.91, 0.10, 0.80, 0.22]')) AS vec_score
    FROM papers
    ORDER BY embedding <-> '[0.91, 0.10, 0.80, 0.22]'
    LIMIT 20
)
SELECT
    c.title,
    n.author_name,
    c.vec_score
      + ts_rank(
            to_tsvector('simple', c.abstract),
            plainto_tsquery('simple', 'retrieval augmented generation agents')
        ) AS score
FROM network n
JOIN candidates c ON c.author_id = n.author_id
WHERE to_tsvector('simple', c.abstract)
   @@ plainto_tsquery('simple', 'retrieval augmented generation agents')
ORDER BY score DESC
LIMIT 10;

-- ── Cleanup ─────────────────────────────────────────────────

DROP PROPERTY GRAPH collab_graph;
DROP TABLE collaborations;
DROP TABLE researchers;
DROP TABLE papers;

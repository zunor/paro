-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ft_t025_simple (
    id INT,
    content VARCHAR
);

INSERT INTO ft_t025_simple VALUES
    (1, 'vector database vector'),
    (2, 'vector x database'),
    (3, 'vectors everywhere'),
    (4, 'graph');

-- sequential fallback
SELECT id
FROM ft_t025_simple
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY id;

SELECT id
FROM ft_t025_simple
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', 'vec:*')
ORDER BY id;

SELECT id
FROM ft_t025_simple
WHERE to_tsvector('simple', content) @@ phraseto_tsquery('simple', 'vector database')
ORDER BY id;

SELECT id
FROM ft_t025_simple
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', 'vector <2> database')
ORDER BY id;

CREATE INDEX idx_ft_t025_simple_fts
ON ft_t025_simple USING GIN (to_tsvector('simple', content));

-- indexed path
SELECT id
FROM ft_t025_simple
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY id;

SELECT id
FROM ft_t025_simple
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', 'vec:*')
ORDER BY id;

SELECT id
FROM ft_t025_simple
WHERE to_tsvector('simple', content) @@ phraseto_tsquery('simple', 'vector database')
ORDER BY id;

SELECT id
FROM ft_t025_simple
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', 'vector <2> database')
ORDER BY id;

DROP INDEX IF EXISTS idx_ft_t025_simple_fts;
DROP TABLE ft_t025_simple;

CREATE TABLE ft_t025_english (
    id INT,
    content VARCHAR
);

INSERT INTO ft_t025_english VALUES
    (1, 'The databases are running quickly'),
    (2, 'database operations'),
    (3, 'runs quickly');

SELECT id
FROM ft_t025_english
WHERE to_tsvector('english', content) @@ plainto_tsquery('english', 'database run quick')
ORDER BY id;

CREATE INDEX idx_ft_t025_english_fts
ON ft_t025_english USING GIN (to_tsvector('english', content));

SELECT id
FROM ft_t025_english
WHERE to_tsvector('english', content) @@ plainto_tsquery('english', 'database run quick')
ORDER BY id;

DROP INDEX IF EXISTS idx_ft_t025_english_fts;
DROP TABLE ft_t025_english;

CREATE TABLE ft_t025_chinese (
    id INT,
    content VARCHAR
);

INSERT INTO ft_t025_chinese VALUES
    (1, '向量'),
    (2, '数据库');

SELECT id
FROM ft_t025_chinese
WHERE to_tsvector('chinese', content) @@ plainto_tsquery('chinese', '向')
ORDER BY id;

CREATE INDEX idx_ft_t025_chinese_fts
ON ft_t025_chinese USING GIN (to_tsvector('chinese', content));

SELECT id
FROM ft_t025_chinese
WHERE to_tsvector('chinese', content) @@ plainto_tsquery('chinese', '向')
ORDER BY id;

DROP INDEX IF EXISTS idx_ft_t025_chinese_fts;
DROP TABLE ft_t025_chinese;

CREATE TABLE ft_t025_japanese (
    id INT,
    content VARCHAR
);

INSERT INTO ft_t025_japanese VALUES
    (1, 'ベクトル'),
    (2, 'データベース');

SELECT id
FROM ft_t025_japanese
WHERE to_tsvector('japanese', content) @@ plainto_tsquery('japanese', 'ベ')
ORDER BY id;

CREATE INDEX idx_ft_t025_japanese_fts
ON ft_t025_japanese USING GIN (to_tsvector('japanese', content));

SELECT id
FROM ft_t025_japanese
WHERE to_tsvector('japanese', content) @@ plainto_tsquery('japanese', 'ベ')
ORDER BY id;

DROP INDEX IF EXISTS idx_ft_t025_japanese_fts;
DROP TABLE ft_t025_japanese;

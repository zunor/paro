# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

CREATE TABLE ft_t013_headline (
    id INT,
    content VARCHAR
);

INSERT INTO ft_t013_headline VALUES
    (1, 'Vector database systems are practical'),
    (2, 'Database internals and vector indexes'),
    (3, NULL);

SELECT id,
       ts_headline('simple', content, plainto_tsquery('simple', 'vector database')) AS hl
FROM ft_t013_headline
ORDER BY id;

SELECT ts_headline(content, plainto_tsquery('simple', 'vector')) AS hl
FROM ft_t013_headline
WHERE id = 1;

DROP TABLE ft_t013_headline;

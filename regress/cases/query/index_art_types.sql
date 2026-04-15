-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP INDEX IF EXISTS idx_art_types_is_active;
DROP INDEX IF EXISTS idx_art_types_code;
DROP INDEX IF EXISTS idx_art_types_name;
DROP INDEX IF EXISTS idx_art_types_score;
DROP TABLE IF EXISTS art_types;

CREATE TABLE art_types (
    id INT PRIMARY KEY,
    is_active BOOLEAN,
    code INT,
    name VARCHAR,
    score DOUBLE
);

INSERT INTO art_types VALUES
    (1, true, 10, 'alpha', 1.5),
    (2, false, 20, 'bravo', 2.5),
    (3, true, 30, 'charlie', 3.5),
    (4, false, 40, 'delta', 4.5);

CREATE INDEX idx_art_types_is_active ON art_types (is_active);
CREATE INDEX idx_art_types_code ON art_types (code);
CREATE INDEX idx_art_types_name ON art_types (name);
CREATE INDEX idx_art_types_score ON art_types (score);

SELECT id
FROM art_types
WHERE is_active = true
ORDER BY id;

SELECT id
FROM art_types
WHERE code BETWEEN 15 AND 35
ORDER BY id;

SELECT id
FROM art_types
WHERE name = 'delta';

SELECT id
FROM art_types
WHERE score BETWEEN 2.5 AND 4.5
ORDER BY id;

SELECT index_name, index_type, build_state
FROM paro_indexes()
WHERE table_name = 'art_types'
ORDER BY index_name;

DROP TABLE art_types;

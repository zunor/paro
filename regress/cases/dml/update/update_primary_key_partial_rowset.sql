-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS dml_update_pk_partial_compact_case;
DROP TABLE IF EXISTS dml_update_pk_partial_restart_case;

CREATE TABLE dml_update_pk_partial_compact_case (
    id INT PRIMARY KEY,
    score INT,
    note TEXT
);

INSERT INTO dml_update_pk_partial_compact_case VALUES
    (1, 10, 'alpha'),
    (2, 20, 'beta');

UPDATE dml_update_pk_partial_compact_case
SET note = 'alpha-after-update'
WHERE id = 1;

SELECT id, score, note
FROM dml_update_pk_partial_compact_case
ORDER BY id;

SELECT id, score, note
FROM dml_update_pk_partial_compact_case
ORDER BY id;

CREATE TABLE dml_update_pk_partial_restart_case (
    id INT PRIMARY KEY,
    score INT,
    note TEXT
);

INSERT INTO dml_update_pk_partial_restart_case VALUES
    (1, 100, 'before-restart'),
    (2, 200, 'stable');

UPDATE dml_update_pk_partial_restart_case
SET note = 'after-restart'
WHERE id = 1;

SELECT id, score, note
FROM dml_update_pk_partial_restart_case
ORDER BY id;

-- @statement ok
CHECKPOINT;

-- @control restart

SELECT id, score, note
FROM dml_update_pk_partial_restart_case
ORDER BY id;

DROP TABLE IF EXISTS dml_update_pk_partial_compact_case;
DROP TABLE IF EXISTS dml_update_pk_partial_restart_case;

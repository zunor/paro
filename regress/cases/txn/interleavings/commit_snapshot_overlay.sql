-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS txn_e2e_commit_snapshot_overlay;
CREATE TABLE txn_e2e_commit_snapshot_overlay (
    id INT PRIMARY KEY,
    note VARCHAR
);
INSERT INTO txn_e2e_commit_snapshot_overlay VALUES (1, 'seed');

-- @teardown
DROP TABLE IF EXISTS txn_e2e_commit_snapshot_overlay;

-- @session reader
BEGIN ISOLATION LEVEL SNAPSHOT;

-- @session reader
SELECT id, note
FROM txn_e2e_commit_snapshot_overlay
ORDER BY id;

-- @session writer
INSERT INTO txn_e2e_commit_snapshot_overlay VALUES (2, 'writer-commit');

-- @wait_expect interval=20ms timeout=5s
SELECT id, note
FROM txn_e2e_commit_snapshot_overlay
ORDER BY id;

-- @session reader
SELECT id, note
FROM txn_e2e_commit_snapshot_overlay
ORDER BY id;

-- @session reader
INSERT INTO txn_e2e_commit_snapshot_overlay VALUES (3, 'reader-own-write');

-- @session reader
SELECT id, note
FROM txn_e2e_commit_snapshot_overlay
ORDER BY id;

-- @session reader
SAVEPOINT before_temp;

-- @session reader
INSERT INTO txn_e2e_commit_snapshot_overlay VALUES (4, 'temp-savepoint');

-- @session reader
SELECT id, note
FROM txn_e2e_commit_snapshot_overlay
ORDER BY id;

-- @session reader
ROLLBACK TO SAVEPOINT before_temp;

-- @session reader
SELECT id, note
FROM txn_e2e_commit_snapshot_overlay
ORDER BY id;

-- @session reader
COMMIT;

SELECT id, note
FROM txn_e2e_commit_snapshot_overlay
ORDER BY id;

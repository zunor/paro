-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS txn_e2e_write_conflict;
CREATE TABLE txn_e2e_write_conflict (
    id INT PRIMARY KEY,
    value INT
);
INSERT INTO txn_e2e_write_conflict VALUES (1, 10);

-- @teardown
DROP TABLE IF EXISTS txn_e2e_write_conflict;

-- @session stale
BEGIN ISOLATION LEVEL SERIALIZABLE;

-- @session fresh
BEGIN ISOLATION LEVEL SERIALIZABLE;

-- @session stale
SELECT id, value
FROM txn_e2e_write_conflict
WHERE id = 1;

-- @session fresh
UPDATE txn_e2e_write_conflict SET value = 20 WHERE id = 1;

-- @session fresh
COMMIT;

-- @session stale
UPDATE txn_e2e_write_conflict SET value = 30 WHERE id = 1;

-- @session stale
-- @normalize transaction_ids
COMMIT;

SELECT id, value
FROM txn_e2e_write_conflict
ORDER BY id;

-- @session locker
BEGIN;

-- @session locker
SELECT id, value
FROM txn_e2e_write_conflict
WHERE id = 1
FOR UPDATE;

-- @session blocked async=blocked_insert
-- @normalize transaction_ids
INSERT INTO txn_e2e_write_conflict VALUES (2, 40);

-- @sleep 100ms

-- @session locker
COMMIT;

-- @await blocked_insert timeout=5s

-- @session blocked
INSERT INTO txn_e2e_write_conflict VALUES (2, 40);

SELECT id, value
FROM txn_e2e_write_conflict
ORDER BY id;

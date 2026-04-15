-- ============================================================
-- Transaction finalization regression
-- Covers no-op COMMIT/ROLLBACK, explicit commit/rollback, and auto-commit.
-- ============================================================

-- @teardown
DROP TABLE IF EXISTS txn_finalization_explicit_ddl;

COMMIT;
ROLLBACK;

BEGIN;
CREATE TABLE txn_finalization_explicit_ddl (id INT PRIMARY KEY, note VARCHAR);
INSERT INTO txn_finalization_explicit_ddl VALUES (1, 'explicit-commit');
COMMIT;

SELECT id, note
FROM txn_finalization_explicit_ddl
ORDER BY id;

BEGIN;
INSERT INTO txn_finalization_explicit_ddl VALUES (2, 'rolled-back');
ROLLBACK;

SELECT id, note
FROM txn_finalization_explicit_ddl
ORDER BY id;

INSERT INTO txn_finalization_explicit_ddl VALUES (3, 'auto-commit');

SELECT id, note
FROM txn_finalization_explicit_ddl
ORDER BY id;

DROP TABLE txn_finalization_explicit_ddl;

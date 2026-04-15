# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS alter_regress_conflict_a_v2;
DROP TABLE IF EXISTS alter_regress_conflict_b;
DROP TABLE IF EXISTS alter_regress_conflict_a;

CREATE TABLE alter_regress_conflict_a (
    id BIGINT PRIMARY KEY,
    v BIGINT
);

CREATE TABLE alter_regress_conflict_b (
    id BIGINT PRIMARY KEY,
    v BIGINT
);

INSERT INTO alter_regress_conflict_a VALUES (1, 10);

BEGIN;
SAVEPOINT before_conflict;
RENAME TABLE alter_regress_conflict_a TO alter_regress_conflict_b;
ROLLBACK TO SAVEPOINT before_conflict;
RENAME TABLE alter_regress_conflict_a TO alter_regress_conflict_a_v2;
COMMIT;

SELECT COUNT(*) FROM alter_regress_conflict_a_v2;

DROP TABLE alter_regress_conflict_a_v2;
DROP TABLE alter_regress_conflict_b;

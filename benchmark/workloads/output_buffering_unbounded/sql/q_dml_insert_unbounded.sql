-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- DML path: execute_program() with unbounded output, result_types.is_empty().
-- INSERT with no result columns uses unbounded path; verifies no large
-- result set materialization (DML produces row count only).
INSERT INTO unbuf_sink SELECT id, v1 FROM unbuf_scan WHERE v2 > 300000;

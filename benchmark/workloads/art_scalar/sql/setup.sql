-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS bench_art_scalar;

CREATE TABLE bench_art_scalar (
    id BIGINT PRIMARY KEY,
    key_col BIGINT,
    payload BIGINT
);

INSERT INTO bench_art_scalar
SELECT i, i, i * 10
FROM generate_series(1, ${rows}) AS t(i);

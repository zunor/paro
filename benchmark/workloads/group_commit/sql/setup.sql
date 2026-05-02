-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS benchmark_group_commit_append;
DROP TABLE IF EXISTS benchmark_group_commit_hot;

CREATE TABLE benchmark_group_commit_append (
    id BIGINT,
    shard INT,
    payload VARCHAR
);

CREATE TABLE benchmark_group_commit_hot (
    id BIGINT PRIMARY KEY,
    value BIGINT,
    payload VARCHAR
);

INSERT INTO benchmark_group_commit_hot VALUES (1, 0, '${payload}');

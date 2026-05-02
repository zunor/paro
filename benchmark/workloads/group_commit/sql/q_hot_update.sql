-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

UPDATE benchmark_group_commit_hot
SET value = value + 1,
    payload = '${payload}'
WHERE id = 1;

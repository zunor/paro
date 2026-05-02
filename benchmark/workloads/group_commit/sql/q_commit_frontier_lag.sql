-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT COALESCE(SUM(durable_commit_id - published_commit_id), 0)
FROM paro_commit_frontiers();

-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT id, v
FROM benchmark_primary_key_dml_case
WHERE id = ${hot_id};

# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT COUNT(*)
FROM benchmark_primary_key_dml_case
WHERE id = ${deleted_id};

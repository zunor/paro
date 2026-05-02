-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT COALESCE(SUM(txn_commit_count), 0)
FROM paro_transaction_metrics();

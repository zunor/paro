-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS transaction_time_anchor;
CREATE TABLE transaction_time_anchor (captured_at TIMESTAMP);

-- Transaction-stable time functions must use the transaction lifecycle's wall-clock anchor.
-- Storing the value in one statement and comparing it in a later statement catches runtimes that
-- read the system clock independently for every operator batch or function invocation.
BEGIN;
INSERT INTO transaction_time_anchor SELECT now();
SELECT
  captured_at = now() AS now_is_transaction_stable,
  captured_at = current_timestamp() AS aliases_share_transaction_time
FROM transaction_time_anchor;
ROLLBACK;

DROP TABLE transaction_time_anchor;

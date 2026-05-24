-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Pure streaming: scan + filter + project (fetch-driven bounded path)
-- Verifies bounded output queue does not materialize full result.
SELECT id, v1 FROM buf_scan WHERE v2 > 500;

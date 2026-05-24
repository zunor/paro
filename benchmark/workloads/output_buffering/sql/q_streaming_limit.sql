-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Streaming limit: early termination via StopPipeline.
-- Fetch-driven path should stop after limit rows without scanning full table.
SELECT id FROM buf_scan LIMIT 1000;

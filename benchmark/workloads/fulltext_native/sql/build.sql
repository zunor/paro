-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

CREATE INVERTED INDEX idx_bench_docs_native_fts
ON bench_docs_native (content);

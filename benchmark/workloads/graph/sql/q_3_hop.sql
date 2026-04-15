-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM GRAPH_TABLE(bench_graph
    MATCH (a:Person WHERE a.id = 1)-[e:Link]->{3,3}(b:Person)
    COLUMNS (a.id AS src, b.id AS dst)
) gt;

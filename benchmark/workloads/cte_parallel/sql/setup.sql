# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS cte_parallel_src;

SET threads = 4;

CREATE TABLE cte_parallel_src(id INT, bucket INT, payload INT);
INSERT INTO cte_parallel_src
SELECT g, g % 128, g % 1024
FROM generate_series(1, ${scan_rows}) AS t(g);

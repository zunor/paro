# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

DROP INDEX IF EXISTS idx_art_metrics_metric_key;
DROP TABLE IF EXISTS art_metrics;

CREATE TABLE art_metrics (
    id INT PRIMARY KEY,
    metric_key INT,
    metric_value INT
);

INSERT INTO art_metrics
SELECT i, i * 10, i * 100
FROM generate_series(1, 12) AS t(i);

CREATE INDEX idx_art_metrics_metric_key ON art_metrics (metric_key);

SELECT metric_value
FROM art_metrics
WHERE metric_key = 70;

SELECT id
FROM art_metrics
WHERE metric_key BETWEEN 40 AND 90
ORDER BY id;

SELECT COUNT(*)
FROM art_metrics
WHERE metric_key >= 100;

SELECT index_name, build_state
FROM paro_indexes()
WHERE table_name = 'art_metrics'
ORDER BY index_name;

DROP TABLE art_metrics;

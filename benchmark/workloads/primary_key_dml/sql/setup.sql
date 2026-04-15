# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS benchmark_primary_key_dml_case;

CREATE TABLE benchmark_primary_key_dml_case (
    id INT PRIMARY KEY,
    v INT
);

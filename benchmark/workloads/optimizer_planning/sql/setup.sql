-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS optimizer_plan_e;
DROP TABLE IF EXISTS optimizer_plan_d;
DROP TABLE IF EXISTS optimizer_plan_c;
DROP TABLE IF EXISTS optimizer_plan_b;
DROP TABLE IF EXISTS optimizer_plan_a;

CREATE TABLE optimizer_plan_a (id BIGINT, group_key INT);
CREATE TABLE optimizer_plan_b (id BIGINT, a_id BIGINT);
CREATE TABLE optimizer_plan_c (id BIGINT, b_id BIGINT);
CREATE TABLE optimizer_plan_d (id BIGINT, c_id BIGINT);
CREATE TABLE optimizer_plan_e (d_id BIGINT, metric BIGINT);

INSERT INTO optimizer_plan_a
SELECT i, (i % 32)::INT FROM generate_series(1, ${rows}) AS t(i);
INSERT INTO optimizer_plan_b
SELECT i, i FROM generate_series(1, ${rows}) AS t(i);
INSERT INTO optimizer_plan_c
SELECT i, i FROM generate_series(1, ${rows}) AS t(i);
INSERT INTO optimizer_plan_d
SELECT i, i FROM generate_series(1, ${rows}) AS t(i);
INSERT INTO optimizer_plan_e
SELECT i, (i % 101)::BIGINT FROM generate_series(1, ${rows}) AS t(i);

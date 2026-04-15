DROP TABLE IF EXISTS bench_agg;
DROP TABLE IF EXISTS bench_agg_matrix;

CREATE TABLE bench_agg (
    id BIGINT PRIMARY KEY,
    group_low INT,
    group_high INT,
    metric INT
);

INSERT INTO bench_agg
SELECT
    i,
    (i % 10)::INT,
    (((i - 1) % ${groups}) + 1)::INT,
    ((i * 17) % 1000)::INT
FROM generate_series(1, ${rows}) AS t(i);

CREATE TABLE bench_agg_matrix (
    row_count INT,
    group_card INT,
    id BIGINT,
    key_single INT,
    key_a INT,
    key_b INT,
    m1 INT,
    m2 INT,
    m3 INT,
    m4 INT
);

-- rows_small × card_low
INSERT INTO bench_agg_matrix
SELECT
    ${rows_small}::INT AS row_count,
    ${card_low}::INT AS group_card,
    i AS id,
    ((i - 1) % ${card_low})::INT AS key_single,
    ((i - 1) % 5)::INT AS key_a,
    (((i - 1) / 5) % 2)::INT AS key_b,
    ((i * 17) % 1000)::INT AS m1,
    ((i * 19) % 1000)::INT AS m2,
    ((i * 23) % 1000)::INT AS m3,
    ((i * 29) % 1000)::INT AS m4
FROM generate_series(1, ${rows_small}) AS t(i);

-- rows_small × card_high
INSERT INTO bench_agg_matrix
SELECT
    ${rows_small}::INT AS row_count,
    ${card_high}::INT AS group_card,
    i + ${rows_small}::BIGINT AS id,
    ((i - 1) % ${card_high})::INT AS key_single,
    ((i - 1) % 100)::INT AS key_a,
    (((i - 1) / 100) % 10)::INT AS key_b,
    ((i * 31) % 1000)::INT AS m1,
    ((i * 37) % 1000)::INT AS m2,
    ((i * 41) % 1000)::INT AS m3,
    ((i * 43) % 1000)::INT AS m4
FROM generate_series(1, ${rows_small}) AS t(i);

-- rows_large × card_low
INSERT INTO bench_agg_matrix
SELECT
    ${rows_large}::INT AS row_count,
    ${card_low}::INT AS group_card,
    i + (${rows_small}::BIGINT * 2)::BIGINT AS id,
    ((i - 1) % ${card_low})::INT AS key_single,
    ((i - 1) % 5)::INT AS key_a,
    (((i - 1) / 5) % 2)::INT AS key_b,
    ((i * 47) % 1000)::INT AS m1,
    ((i * 53) % 1000)::INT AS m2,
    ((i * 59) % 1000)::INT AS m3,
    ((i * 61) % 1000)::INT AS m4
FROM generate_series(1, ${rows_large}) AS t(i);

-- rows_large × card_high
INSERT INTO bench_agg_matrix
SELECT
    ${rows_large}::INT AS row_count,
    ${card_high}::INT AS group_card,
    i + (${rows_small}::BIGINT * 2 + ${rows_large}::BIGINT)::BIGINT AS id,
    ((i - 1) % ${card_high})::INT AS key_single,
    ((i - 1) % 100)::INT AS key_a,
    (((i - 1) / 100) % 10)::INT AS key_b,
    ((i * 67) % 1000)::INT AS m1,
    ((i * 71) % 1000)::INT AS m2,
    ((i * 73) % 1000)::INT AS m3,
    ((i * 79) % 1000)::INT AS m4
FROM generate_series(1, ${rows_large}) AS t(i);

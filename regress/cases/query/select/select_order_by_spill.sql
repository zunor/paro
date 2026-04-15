-- @setup
DROP TABLE IF EXISTS order_spill_test;

-- @setup
DROP TABLE IF EXISTS order_spill_null_test;

-- @setup
DROP TABLE IF EXISTS order_spill_smallmem_test;

-- @setup
CREATE TABLE order_spill_test (id INT, k1 INT, k2 INT, payload VARCHAR);

INSERT INTO order_spill_test VALUES
    (1, 1, 10, 'payload_alpha_aaaaaaaaaaaaaaaa'),
    (2, 1, 20, 'payload_beta_bbbbbbbbbbbbbbbb'),
    (3, 2, 30, 'payload_gamma_cccccccccccccccc'),
    (4, 2, 40, 'payload_delta_dddddddddddddddd'),
    (5, 3, 50, 'payload_epsilon_eeeeeeeeeeeeeeee'),
    (6, 3, 60, 'payload_zeta_ffffffffffffffff'),
    (7, 4, 70, 'payload_eta_gggggggggggggggg'),
    (8, 4, 80, 'payload_theta_hhhhhhhhhhhhhhhh');

INSERT INTO order_spill_test
SELECT id + 1000, k1 + 1, k2, payload || '_dup1' FROM order_spill_test;

INSERT INTO order_spill_test
SELECT id + 2000, k1 + 2, k2, payload || '_dup2' FROM order_spill_test;

INSERT INTO order_spill_test
SELECT id + 4000, k1 + 3, k2, payload || '_dup3' FROM order_spill_test;

INSERT INTO order_spill_test
SELECT id + 8000, k1 + 4, k2, payload || '_dup4' FROM order_spill_test;

SET temp_directory = '/tmp/paro_regress_order_spill';

SELECT id, k1, k2
FROM order_spill_test
ORDER BY k2 DESC, id ASC
LIMIT 12;

SET force_external = true;

SELECT id, k1, k2
FROM order_spill_test
ORDER BY k2 DESC, id ASC
LIMIT 12;

SELECT id, payload
FROM order_spill_test
ORDER BY payload DESC, id ASC
LIMIT 6;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE SELECT id FROM order_spill_test ORDER BY id DESC, k1 ASC;

SET force_external = DEFAULT;

SET force_external = true;

CREATE TABLE order_spill_smallmem_test (id INT, k1 INT);

INSERT INTO order_spill_smallmem_test VALUES
    (5, 1),
    (2, 2),
    (9, 3),
    (1, 4),
    (7, 5),
    (3, 6);

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE SELECT id FROM order_spill_smallmem_test ORDER BY id ASC, k1 DESC;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;

DROP TABLE IF EXISTS order_spill_smallmem_test;

CREATE TABLE order_spill_null_test (id INT, a INT, b INT);

INSERT INTO order_spill_null_test VALUES
    (1, 1, 10),
    (2, 1, NULL),
    (3, NULL, 5),
    (4, 2, 7),
    (5, 1, 7),
    (6, NULL, NULL),
    (7, 2, NULL),
    (8, 1, 10);

SELECT id, a, b
FROM order_spill_null_test
ORDER BY a ASC NULLS LAST, b DESC NULLS FIRST, id ASC;

-- @teardown
DROP TABLE IF EXISTS order_spill_test;

-- @teardown
DROP TABLE IF EXISTS order_spill_null_test;

-- @teardown
DROP TABLE IF EXISTS order_spill_smallmem_test;

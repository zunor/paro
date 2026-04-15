DROP TABLE IF EXISTS bench_order_numeric;
DROP TABLE IF EXISTS bench_order_payload;
DROP TABLE IF EXISTS bench_order_variable;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET memory_limit = DEFAULT;

CREATE TABLE bench_order_numeric(id INT, score INT, shard INT);
INSERT INTO bench_order_numeric
SELECT g, ${numeric_rows} - g + 1, (g % 8) + 1
FROM generate_series(1, ${numeric_rows}) AS t(g);

CREATE TABLE bench_order_payload(id INT, priority INT, tie_break INT, payload VARCHAR);
INSERT INTO bench_order_payload
SELECT
    g,
    100 - (g % 9),
    g % 7,
    'payload_' || CAST(g % 997 AS VARCHAR) || '_xxxxxxxxxxxxxxxx'
FROM generate_series(1, ${payload_rows}) AS t(g);

INSERT INTO bench_order_payload VALUES
    (90001, 500, 2, 'seed_payload_alpha_aaaaaaaaaaaaaaaa'),
    (90002, 500, 1, 'seed_payload_beta_bbbbbbbbbbbbbbbb'),
    (90003, 499, 3, 'seed_payload_gamma_cccccccccccccccc'),
    (90004, 499, 3, 'seed_payload_delta_dddddddddddddddd'),
    (90005, 498, 2, 'seed_payload_epsilon_eeeeeeeeeeeeeeee'),
    (90006, 498, 2, 'seed_payload_zeta_ffffffffffffffff'),
    (90007, 498, 5, 'seed_payload_eta_gggggggggggggggg'),
    (90008, 497, 1, 'seed_payload_theta_hhhhhhhhhhhhhhhh'),
    (90009, 497, 1, 'seed_payload_iota_iiiiiiiiiiiiiiii'),
    (90010, 496, 4, 'seed_payload_kappa_jjjjjjjjjjjjjjjj'),
    (90011, 495, 2, 'seed_payload_lambda_kkkkkkkkkkkkkkkk'),
    (90012, 494, 8, 'seed_payload_mu_llllllllllllllll');

CREATE TABLE bench_order_variable(id INT, sort_key VARCHAR, payload VARCHAR);
INSERT INTO bench_order_variable
SELECT
    g,
    'mid_' || CAST(((${string_rows} - g) % 997) AS VARCHAR) || '_m_m_m_m',
    'payload_' || CAST(g % 251 AS VARCHAR) || '_p_p_p_p_p_p_p_p_p_p_p_p'
FROM generate_series(1, ${string_rows}) AS t(g);

INSERT INTO bench_order_variable VALUES
    (91001, 'aa', 'seed_var_alpha'),
    (91002, 'ab', 'seed_var_beta'),
    (91003, 'ab', 'seed_var_beta_dup'),
    (91004, 'ac', 'seed_var_gamma'),
    (91005, 'b', 'seed_var_delta'),
    (91006, 'ba', 'seed_var_epsilon'),
    (91007, 'bb', 'seed_var_zeta'),
    (91008, NULL, 'seed_var_null');

DROP TABLE IF EXISTS spill_order;
DROP TABLE IF EXISTS spill_window;
DROP TABLE IF EXISTS spill_agg;
DROP TABLE IF EXISTS spill_join_l;
DROP TABLE IF EXISTS spill_join_r;

SET force_external = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET temp_directory = DEFAULT;
SET memory_limit = DEFAULT;

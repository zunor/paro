-- @setup
DROP TABLE IF EXISTS proj_test;

-- @setup
CREATE TABLE proj_test (id INT, a INT, b VARCHAR, c DOUBLE);

INSERT INTO proj_test VALUES (1, 10, 'hello', 1.5);

-- SELECT specific columns
SELECT id, b FROM proj_test;

-- SELECT columns in different order
SELECT c, a FROM proj_test;

-- @teardown
DROP TABLE IF EXISTS proj_test;

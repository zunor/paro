-- @setup
DROP TABLE IF EXISTS filter_test;

-- @setup
CREATE TABLE filter_test (id INT PRIMARY KEY, name VARCHAR, score INT);

INSERT INTO filter_test VALUES (1, 'A', 90), (2, 'B', 85), (3, 'C', 95);

-- Filter by ID
SELECT * FROM filter_test WHERE id = 2;

-- Filter by score
SELECT * FROM filter_test WHERE score > 88 ORDER BY id;

-- Filter by name
SELECT * FROM filter_test WHERE name = 'A';

-- @teardown
DROP TABLE IF EXISTS filter_test;

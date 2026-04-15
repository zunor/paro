-- @setup
DROP TABLE IF EXISTS parallel_execution_case;
CREATE TABLE parallel_execution_case (id INT, bucket INT, value INT);
INSERT INTO parallel_execution_case VALUES
    (1, 1, 5),
    (2, 1, 10),
    (3, 1, 15),
    (4, 2, 20),
    (5, 2, 25),
    (6, 2, 30),
    (7, 3, 35),
    (8, 3, 40),
    (9, 3, 45),
    (10, 4, 50),
    (11, 4, 55),
    (12, 4, 60);

SET threads = 1;
SELECT count(*) FROM parallel_execution_case;
SELECT sum(value) FROM parallel_execution_case;
SELECT count(*) FROM parallel_execution_case WHERE value >= 30;
SELECT id, bucket, value FROM parallel_execution_case WHERE bucket IN (2, 4) ORDER BY id;

SET threads = 4;
SELECT count(*) FROM parallel_execution_case;
SELECT sum(value) FROM parallel_execution_case;
SELECT count(*) FROM parallel_execution_case WHERE value >= 30;
SELECT id, bucket, value FROM parallel_execution_case WHERE bucket IN (2, 4) ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS parallel_execution_case;

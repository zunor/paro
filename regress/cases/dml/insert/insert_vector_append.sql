-- @setup
DROP TABLE IF EXISTS vec_append_test;

CREATE TABLE vec_append_test (id INT, embedding VECTOR(3));

INSERT INTO vec_append_test VALUES (1, '[1,2,3]'), (2, '[4,5,6]');

SELECT * FROM vec_append_test ORDER BY id;

INSERT INTO vec_append_test VALUES (3, '[7,8,9]'), (4, '[10,11,12]');

SELECT * FROM vec_append_test ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS vec_append_test;

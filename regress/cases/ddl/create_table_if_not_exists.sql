-- @setup
DROP TABLE IF EXISTS if_not_exists_test;

CREATE TABLE if_not_exists_test (id INT, name VARCHAR);

INSERT INTO if_not_exists_test VALUES (1, 'first');

-- 再次创建同名表，应该静默成功（不报错）
CREATE TABLE IF NOT EXISTS if_not_exists_test (id INT, value DOUBLE);

-- 验证原始表数据仍在（没有被覆盖）
SELECT id, name FROM if_not_exists_test;

-- @teardown
DROP TABLE IF EXISTS if_not_exists_test;

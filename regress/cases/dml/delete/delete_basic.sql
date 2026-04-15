-- @setup
DROP TABLE IF EXISTS dml_delete_basic_case;

-- @setup
CREATE TABLE dml_delete_basic_case (
  id INT,
  name VARCHAR
);

INSERT INTO dml_delete_basic_case VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT id, name FROM dml_delete_basic_case ORDER BY id;

DELETE FROM dml_delete_basic_case WHERE id = 2;

SELECT id, name FROM dml_delete_basic_case ORDER BY id;

DELETE FROM dml_delete_basic_case;

SELECT id, name FROM dml_delete_basic_case ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS dml_delete_basic_case;

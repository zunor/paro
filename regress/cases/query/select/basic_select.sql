-- @setup
DROP TABLE IF EXISTS dml_select_basic_case;

-- @setup
CREATE TABLE dml_select_basic_case (
  id INT,
  name TEXT
);

INSERT INTO dml_select_basic_case VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT 42 AS answer;

SELECT * FROM dml_select_basic_case ORDER BY id;

SELECT id FROM dml_select_basic_case ORDER BY id;

SELECT id, name FROM dml_select_basic_case WHERE id >= 2 ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS dml_select_basic_case;

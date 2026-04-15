-- @setup
DROP TABLE IF EXISTS dml_delete_primary_key_bulk_case;

-- @setup
CREATE TABLE dml_delete_primary_key_bulk_case (
  id INT PRIMARY KEY,
  v INT
);

INSERT INTO dml_delete_primary_key_bulk_case
SELECT i, i * 10
FROM generate_series(1, 20000) AS t(i);

DELETE FROM dml_delete_primary_key_bulk_case
WHERE id % 10 = 0;

SELECT COUNT(*), MIN(id), MAX(id)
FROM dml_delete_primary_key_bulk_case;

SELECT COUNT(*)
FROM dml_delete_primary_key_bulk_case
WHERE id = 10000;

-- @teardown
DROP TABLE IF EXISTS dml_delete_primary_key_bulk_case;

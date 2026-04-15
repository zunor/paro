-- @setup
DROP TABLE IF EXISTS dml_insert_primary_key_bulk_case;

-- @setup
CREATE TABLE dml_insert_primary_key_bulk_case (
  id INT PRIMARY KEY,
  v INT
);

INSERT INTO dml_insert_primary_key_bulk_case
SELECT i, i * 10
FROM generate_series(1, 100000) AS t(i);

SELECT COUNT(*), MIN(id), MAX(id)
FROM dml_insert_primary_key_bulk_case;

SELECT id, v
FROM dml_insert_primary_key_bulk_case
WHERE id IN (1, 50000, 100000)
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS dml_insert_primary_key_bulk_case;

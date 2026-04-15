-- @setup
DROP TABLE IF EXISTS type_roundtrip_floats;

CREATE TABLE type_roundtrip_floats (
  id INT,
  f FLOAT,
  d DOUBLE
);

INSERT INTO type_roundtrip_floats VALUES
  (1, 0.0, 0.0),
  (2, -0.0, -0.0),
  (3, 1.5, 2.25),
  (4, CAST('nan' AS FLOAT), CAST('nan' AS DOUBLE)),
  (5, CAST('inf' AS FLOAT), CAST('-inf' AS DOUBLE)),
  (6, NULL, NULL);

-- @query rowsort
SELECT id, f, d
FROM type_roundtrip_floats
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_floats;

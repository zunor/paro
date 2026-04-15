-- @setup
DROP TABLE IF EXISTS type_roundtrip_boolean;

CREATE TABLE type_roundtrip_boolean (
  id INT,
  b BOOLEAN
);

INSERT INTO type_roundtrip_boolean VALUES
  (1, true),
  (2, false),
  (3, NULL);

-- @query rowsort
SELECT id, b
FROM type_roundtrip_boolean
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_boolean;

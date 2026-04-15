-- @setup
DROP TABLE IF EXISTS type_roundtrip_blob;

CREATE TABLE type_roundtrip_blob (
  id INT,
  b BLOB
);

INSERT INTO type_roundtrip_blob VALUES
  (1, 'hello'),
  (2, ''),
  (3, 'binary_data_0123456789'),
  (4, NULL);

-- @query rowsort
SELECT id, b
FROM type_roundtrip_blob
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_blob;

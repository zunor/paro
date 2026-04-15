-- @setup
DROP TABLE IF EXISTS type_roundtrip_strings;

CREATE TABLE type_roundtrip_strings (
  id INT,
  v VARCHAR
);

INSERT INTO type_roundtrip_strings VALUES
  (1, 'hello'),
  (2, ''),
  (3, NULL),
  (4, 'Hello, 世界'),
  (5, 'long_string_abcdefghijklmnopqrstuvwxyz_0123456789'),
  (6, 'O''Reilly');

-- @query rowsort
SELECT id, v
FROM type_roundtrip_strings
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_strings;

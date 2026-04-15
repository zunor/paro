-- @setup
DROP TABLE IF EXISTS type_roundtrip_array;

CREATE TABLE type_roundtrip_array (
  id INT,
  ints ARRAY(INTEGER),
  words ARRAY(VARCHAR)
);

INSERT INTO type_roundtrip_array VALUES
  (1, [1, 2, 3], ['alpha', 'beta']),
  (2, [42, -7], ['hello', '']),
  (3, NULL, NULL),
  (4, [0, 1, 2, 3], ['x', 'y', 'z']);

-- @query rowsort
SELECT id, ints, words
FROM type_roundtrip_array
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_array;

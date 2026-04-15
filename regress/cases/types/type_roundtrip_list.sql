-- @setup
DROP TABLE IF EXISTS type_roundtrip_list;

CREATE TABLE type_roundtrip_list (
  id INT,
  ints LIST(INTEGER),
  words LIST(VARCHAR),
  nums LIST(DOUBLE),
  flags LIST(BOOLEAN)
);

INSERT INTO type_roundtrip_list VALUES
  (1, [1, 2, 3], ['alpha', 'beta'], [1.5, 2.5], [true, false, NULL]),
  (2, [42, -7], ['hello', ''], [0.0], NULL),
  (3, NULL, NULL, NULL, []),
  (4, [0, 1, 2, 3], ['x', 'y', 'z'], [3.14, -2.0], [NULL, true]);

-- @query rowsort
SELECT id, ints, words, nums, flags
FROM type_roundtrip_list
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_list;

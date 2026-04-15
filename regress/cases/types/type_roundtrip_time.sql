-- @setup
DROP TABLE IF EXISTS type_roundtrip_time;

CREATE TABLE type_roundtrip_time (
  id INT,
  t TIME
);

INSERT INTO type_roundtrip_time VALUES
  (1, '00:00:00'),
  (2, '12:34:56.123456'),
  (3, NULL);

-- @query rowsort
SELECT id, t
FROM type_roundtrip_time
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_time;

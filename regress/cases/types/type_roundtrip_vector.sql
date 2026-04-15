# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS type_roundtrip_vector;

CREATE TABLE type_roundtrip_vector (
  id INT,
  v VECTOR(3)
);

INSERT INTO type_roundtrip_vector VALUES
  (1, '[1,2,3]'),
  (2, '[0,0,0]'),
  (3, '[-1.5, 2.25, 0]'),
  (4, NULL);

-- @query rowsort
SELECT id, v
FROM type_roundtrip_vector
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_vector;

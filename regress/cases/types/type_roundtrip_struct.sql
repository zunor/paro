-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS type_roundtrip_struct;

CREATE TABLE type_roundtrip_struct (
  id INT,
  person STRUCT(name VARCHAR, age INTEGER, score DOUBLE, active BOOLEAN)
);

INSERT INTO type_roundtrip_struct VALUES
  (1, ('alice', 30, 98.5, true)),
  (2, ('', NULL, -1.0, false)),
  (3, NULL),
  (4, ('bob', 0, 0.0, NULL));

-- @query rowsort
SELECT id, person
FROM type_roundtrip_struct
ORDER BY id;

-- @query rowsort
SELECT id, person IS NULL AS person_is_null
FROM type_roundtrip_struct
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_struct;

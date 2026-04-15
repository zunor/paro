# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS type_roundtrip_json;

CREATE TABLE type_roundtrip_json (
  id INT,
  j JSON,
  jb JSONB
);

INSERT INTO type_roundtrip_json VALUES
  (1, '{"a":1,"b":[true,false,null]}', '{"a":1,"b":[true,false,null]}'),
  (2, '["x","y","z"]', '["x","y","z"]'),
  (3, NULL, NULL),
  (4, '{"nested":{"k":"v"}}', '{"k":[1,2,3]}');

-- @query rowsort
SELECT id, j, jb
FROM type_roundtrip_json
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_json;

# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS type_roundtrip_timestamptz;

CREATE TABLE type_roundtrip_timestamptz (
  id INT,
  ts TIMESTAMP WITH TIME ZONE
);

INSERT INTO type_roundtrip_timestamptz VALUES
  (1, '1970-01-01 00:00:00Z'),
  (2, '2024-01-01 12:34:56.789012+02:00'),
  (3, '2024-01-01 08:30:00-05:00'),
  (4, NULL);

-- @query rowsort
SELECT id, ts
FROM type_roundtrip_timestamptz
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_timestamptz;

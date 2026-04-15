# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS type_roundtrip_temporal;

CREATE TABLE type_roundtrip_temporal (
  id INT,
  d DATE,
  ts TIMESTAMP,
  iv INTERVAL
);

INSERT INTO type_roundtrip_temporal VALUES
  (1, '1970-01-01', '1970-01-01 00:00:00', '1 day'),
  (2, '2000-02-29', '2024-01-01 12:34:56.789', '2 months 3 days 4 hours 5 minutes 6 seconds'),
  (3, NULL, NULL, NULL);

-- @query rowsort
SELECT id, d, ts, iv
FROM type_roundtrip_temporal
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_temporal;

# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS type_roundtrip_uuid;

CREATE TABLE type_roundtrip_uuid (
  id INT,
  u UUID
);

INSERT INTO type_roundtrip_uuid VALUES
  (1, '00000000-0000-0000-0000-000000000000'),
  (2, 'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF'),
  (3, '6ba7b810-9dad-11d1-80b4-00c04fd430c8'),
  (4, NULL);

-- @query rowsort
SELECT id, u
FROM type_roundtrip_uuid
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_uuid;

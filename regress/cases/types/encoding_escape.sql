# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS types_encoding_escape_case;

-- @setup
CREATE TABLE types_encoding_escape_case (
  id INT,
  c_null TEXT,
  c_empty TEXT,
  c_literal_null TEXT,
  c_literal_empty TEXT,
  c_tab TEXT,
  c_newline TEXT
);

-- @statement ok
INSERT INTO types_encoding_escape_case VALUES (
  1,
  NULL,
  '',
  'NULL',
  '(empty)',
  'a	b',
  'line1
line2'
);

-- @query nosort
SELECT id, c_null, c_empty, c_literal_null, c_literal_empty, c_tab, c_newline
FROM types_encoding_escape_case ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS types_encoding_escape_case;

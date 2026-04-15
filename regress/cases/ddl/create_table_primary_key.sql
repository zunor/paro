# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- Create table with inline PRIMARY KEY
CREATE TABLE pk_test (
    id INT PRIMARY KEY,
    name TEXT
);

-- Insert data
INSERT INTO pk_test VALUES (1, 'alice'), (2, 'bob');

-- Check data
SELECT * FROM pk_test ORDER BY id;

-- Cleanup
DROP TABLE pk_test;

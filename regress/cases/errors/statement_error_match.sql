-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @statement error does not exist|not found
SELECT * FROM statement_error_missing_case;

-- @statement error SQLSTATE=42P01|SQLSTATE=42601
SELECT * FROM statement_error_missing_case;

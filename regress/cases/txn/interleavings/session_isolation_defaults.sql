-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @session iso_a
SHOW transaction_isolation;

-- @session iso_b
SHOW transaction_isolation;

-- @session iso_a
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SNAPSHOT READ ONLY;

-- @session iso_a
SHOW transaction_isolation;

-- @session iso_b
SHOW transaction_isolation;

-- @session iso_a
BEGIN;

-- @session iso_a
SHOW transaction_isolation;

-- @session iso_a
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE;

-- @session iso_a
SHOW transaction_isolation;

-- @session iso_a
COMMIT;

-- @session iso_a
SHOW transaction_isolation;

-- @session iso_a
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE;

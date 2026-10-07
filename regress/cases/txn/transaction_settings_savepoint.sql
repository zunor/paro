-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SHOW application_name;

SET application_name = 'session-app';
SHOW application_name;

SET LOCAL application_name = 'outside';

BEGIN;
SET LOCAL application_name = 'local-app';
SHOW application_name;
SAVEPOINT sp1;
SET LOCAL application_name = 'local-2';
SHOW application_name;
ROLLBACK TO SAVEPOINT sp1;
SHOW application_name;
ROLLBACK;

SHOW application_name;

BEGIN;
SAVEPOINT rel1;
SAVEPOINT rel2;
RELEASE SAVEPOINT rel1;
ROLLBACK TO SAVEPOINT rel2;
ROLLBACK;

-- This is a session-setting contract, not an assumption about the server's
-- startup memory envelope. The integration runner verifies plans explicitly.
SET memory_limit = '1GB';
SET optimizer_verify = true;

SELECT name, setting, short_desc AS description
FROM pg_settings
WHERE name NOT IN ('threads', 'temp_directory')
ORDER BY name;

SELECT current_setting('temp_directory') <> '(empty)' AS has_default_temp_directory;

DISCARD ALL;
SHOW application_name;

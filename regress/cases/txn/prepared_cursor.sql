CREATE TABLE prepared_cursor_t (v INT);
INSERT INTO prepared_cursor_t VALUES (1), (2), (3);

PREPARE stmt1 AS SELECT v FROM prepared_cursor_t ORDER BY v;
EXECUTE stmt1;
SELECT COUNT(*) AS prepared_count
FROM pg_catalog.pg_prepared_statements
WHERE name = 'stmt1';
DEALLOCATE stmt1;
SELECT COUNT(*) AS prepared_count
FROM pg_catalog.pg_prepared_statements
WHERE name = 'stmt1';
PREPARE stmt1 AS SELECT v FROM prepared_cursor_t ORDER BY v;
PREPARE stmt1 AS SELECT v + 1 FROM prepared_cursor_t ORDER BY v;
DEALLOCATE stmt1;

DECLARE c_outside CURSOR FOR SELECT v FROM prepared_cursor_t ORDER BY v;

BEGIN;
DECLARE c1 CURSOR FOR SELECT v FROM prepared_cursor_t ORDER BY v;
SELECT COUNT(*) AS cursor_count
FROM pg_catalog.pg_cursors
WHERE name = 'c1';
FETCH 2 FROM c1;
MOVE 1 FROM c1;
COMMIT;
FETCH NEXT FROM c1;

BEGIN;
DECLARE c_hold CURSOR WITH HOLD FOR SELECT v FROM prepared_cursor_t ORDER BY v;
COMMIT;
FETCH NEXT FROM c_hold;
FETCH 2 FROM c_hold;
CLOSE c_hold;
SELECT COUNT(*) AS cursor_count
FROM pg_catalog.pg_cursors
WHERE name = 'c_hold';

BEGIN;
DECLARE c_rollback CURSOR WITH HOLD FOR SELECT v FROM prepared_cursor_t ORDER BY v;
ROLLBACK;
FETCH NEXT FROM c_rollback;

BEGIN;
DECLARE c_scroll CURSOR FOR SELECT v FROM prepared_cursor_t ORDER BY v;
FETCH 2 FROM c_scroll;
FETCH PRIOR FROM c_scroll;
DECLARE c_no_scroll NO SCROLL CURSOR FOR SELECT v FROM prepared_cursor_t ORDER BY v;
FETCH 2 FROM c_no_scroll;
FETCH PRIOR FROM c_no_scroll;
ROLLBACK;

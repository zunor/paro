DROP DATABASE IF EXISTS active_db;
DROP DATABASE IF EXISTS dup_db;
DROP DATABASE IF EXISTS test_db1;

CREATE DATABASE test_db1;
SELECT count(*) AS exists_count FROM paro_databases() WHERE database_name = 'test_db1';

CREATE DATABASE IF NOT EXISTS test_db1;

DROP DATABASE test_db1;
SELECT count(*) AS exists_count FROM paro_databases() WHERE database_name = 'test_db1';

DROP DATABASE IF EXISTS test_db1;

CREATE DATABASE dup_db;
CREATE DATABASE dup_db;

DROP DATABASE nonexistent_db;

DROP DATABASE dup_db;

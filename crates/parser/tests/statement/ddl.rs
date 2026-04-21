// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::common::{write_statement_cases, write_statement_error_cases};

#[test]
fn test_ddl_statements() {
    let cases = &[
        r#"show create table a.b;"#,
        r#"show create table a.b with quoted_identifiers;"#,
        r#"show create table a.b format TabSeparatedWithNamesAndTypes;"#,
        r#"describe a;"#,
        r#"describe a format TabSeparatedWithNamesAndTypes;"#,
        r#"CREATE AGGREGATING INDEX idx1 AS SELECT SUM(a), b FROM t1 WHERE b > 3 GROUP BY b;"#,
        r#"CREATE OR REPLACE AGGREGATING INDEX idx1 AS SELECT SUM(a), b FROM t1 WHERE b > 3 GROUP BY b;"#,
        r#"CREATE OR REPLACE INVERTED INDEX idx2 ON t1 (a, b);"#,
        r#"CREATE OR REPLACE NGRAM INDEX idx2 ON t1 (a, b);"#,
        r#"CREATE INDEX idx_fts ON documents USING GIN (to_tsvector('simple', content));"#,
        r#"create table a (c decimal(38, 0))"#,
        r#"create table a (c decimal(38))"#,
        r#"create table a (c1 decimal(38), c2 int) partition by (c1, c2) PROPERTIES ("read.split.target-size"='134217728', "read.split.metadata-target-size"='33554432');"#,
        r#"create or replace table a (c decimal(38))"#,
        r#"create or replace table a (c int(10) unsigned)"#,
        r#"create table if not exists a.b (c integer not null default 1, b varchar);"#,
        r#"create table if not exists a.b (c integer default 1 not null, b varchar) as select * from t;"#,
        r#"create table if not exists a.b (c tuple(m integer, n string), d tuple(integer, string));"#,
        r#"create table a (b tuple("c-1" int, "c-2" uint64));"#,
        r#"create table if not exists a.b (a string, b string, c string as (concat(a, ' ', b)) stored );"#,
        r#"create table if not exists a.b (a int, b int, c int generated always as (a + b) virtual );"#,
        r#"create table if not exists a.b (a string, b string, inverted index idx1 (a,b) tokenizer='chinese');"#,
        r#"create table if not exists a.b (a string, b string, ngram index idx1 (a,b) gram_size=5);"#,
        r#"create table a.b like c.d;"#,
        r#"create table t like t2 engine = memory;"#,
        r#"create table if not exists a.b (a int) 's3://testbucket/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='http://127.0.0.1:9900');"#,
        r#"truncate table a;"#,
        r#"truncate table "a".b;"#,
        r#"drop table a;"#,
        r#"drop table if exists a."b";"#,
        r#"drop index if exists idx_fts;"#,
        r#"drop index public.idx_fts;"#,
        r#"drop index main.public.idx_fts;"#,
        r#"create database ctl;"#,
        r#"create database if not exists ctl;"#,
        r#"create schema if not exists a;"#,
        r#"create schema ctl.t engine = Default;"#,
        r#"create schema t engine = Default;"#,
        r#"CREATE TABLE `t3`(a int not null, b int not null, c int not null) bloom_index_columns='a,b,c' COMPRESSION='zstd' STORAGE_FORMAT='native';"#,
        r#"create or replace schema a;"#,
        r#"drop schema ctl.t;"#,
        r#"drop schema if exists t;"#,
        r#"create table c(a DateTime null, b DateTime(3));"#,
        r#"create view v as select number % 3 as a from numbers(1000);"#,
        r#"alter view v as select number % 3 as a from numbers(1000);"#,
        r#"drop view v;"#,
        r#"create view v1(c1) as select number % 3 as a from numbers(1000);"#,
        r#"create or replace view v1(c1) as select number % 3 as a from numbers(1000);"#,
        r#"alter view v1(c2) as select number % 3 as a from numbers(1000);"#,
        r#"show views"#,
        r#"show views format TabSeparatedWithNamesAndTypes;"#,
        r#"show full views"#,
        r#"show full views from db"#,
        r#"show full views from ctl.db"#,
        r#"create stream test2.s1 on table test.t append_only = false;"#,
        r#"create stream if not exists test2.s2 on table test.t at (stream => test1.s1) comment = 'this is a stream';"#,
        r#"create stream if not exists test2.s3 on table test.t at (TIMESTAMP => '2023-06-26 09:49:02.038483'::TIMESTAMP) append_only = false;"#,
        r#"create stream if not exists test2.s3 on table test.t at (SNAPSHOT => '9828b23f74664ff3806f44bbc1925ea5') append_only = true;"#,
        r#"create or replace stream test2.s1 on table test.t append_only = false;"#,
        r#"describe stream test2.s2;"#,
        r#"drop stream if exists test2.s2;"#,
        r#"rename table d.t to e.s;"#,
        r#"truncate table test;"#,
        r#"truncate table test_db.test;"#,
        r#"DROP table table1;"#,
        r#"DROP table IF EXISTS table1;"#,
        r#"ALTER TABLE t refresh cache;"#,
        r#"ALTER TABLE t COMMENT='t1-commnet';"#,
        r#"ALTER TABLE t ADD c int null;"#,
        r#"ALTER TABLE t ADD COLUMN c int null;"#,
        r#"ALTER TABLE t ADD COLUMN a float default 1.1 COMMENT 'hello' FIRST;"#,
        r#"ALTER TABLE t ADD COLUMN b string default 'b' AFTER a;"#,
        r#"ALTER TABLE t RENAME COLUMN a TO b;"#,
        r#"ALTER TABLE t DROP COLUMN b;"#,
        r#"ALTER TABLE t DROP b;"#,
        r#"ALTER TABLE t MODIFY COLUMN a int DEFAULT 1, COLUMN b float;"#,
        r#"ALTER TABLE t MODIFY COLUMN a int NULL DEFAULT 1, b float NOT NULL;"#,
        r#"ALTER TABLE t MODIFY COLUMN a int NULL DEFAULT 1, COLUMN b float NOT NULL COMMENT 'column b';"#,
        r#"ALTER TABLE t MODIFY COLUMN a int NULL DEFAULT 1 comment 'column a', COLUMN b float NOT NULL COMMENT 'column b';"#,
        r#"ALTER TABLE t MODIFY COLUMN a comment 'column a', COLUMN b COMMENT 'column b';"#,
        r#"ALTER TABLE t MODIFY COLUMN a int;"#,
        r#"ALTER TABLE t MODIFY a int;"#,
        r#"ALTER TABLE t MODIFY COLUMN a DROP STORED;"#,
        r#"ALTER TABLE t SET OPTIONS(SNAPSHOT_LOCATION='1/7/_ss/101fd790dbbe4238a31a8f2e2f856179_v4.mpk',block_per_segment = 500);"#,
        r#"ALTER TABLE t ADD CONSTRAINT a_not_1 CHECK (a != 1);"#,
        r#"ALTER TABLE t ADD CHECK (a != 1);"#,
        r#"ALTER TABLE t DROP CONSTRAINT a_not_1;"#,
        r#"ALTER SCHEMA IF EXISTS ctl.c RENAME TO a;"#,
        r#"ALTER SCHEMA c RENAME TO a;"#,
        r#"ALTER SCHEMA ctl.c RENAME TO a;"#,
        r#"ALTER SCHEMA ctl.c refresh cache;"#,
        r#"CREATE TABLE t (a INT COMMENT 'col comment') COMMENT='Comment types type speedily \' \\\\ \'\' Fun!';"#,
        r#"COMMENT IF EXISTS ON TABLE t IS 'test'"#,
        r#"COMMENT ON COLUMN t.C1 IS 'test'"#,
        r#"COMMENT ON network policy n1 IS 'test'"#,
        r#"COMMENT ON password policy p1 IS 'test'"#,
        r#"CREATE TEMPORARY TABLE t (a INT COMMENT 'col comment')"#,
        r#"CREATE STAGE ~"#,
        r#"CREATE STAGE IF NOT EXISTS test_stage 's3://load/files/' connection=(aws_key_id='1a2b3c', aws_secret_key='4x5y6z') file_format=(type = CSV, compression = GZIP record_delimiter=',')"#,
        r#"CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' connection=(aws_key_id='1a2b3c', aws_secret_key='4x5y6z') file_format=(type = CSV, compression = GZIP record_delimiter=',')"#,
        r#"CREATE STAGE IF NOT EXISTS test_stage url='azblob://load/files/' connection=(account_name='1a2b3c' account_key='4x5y6z') file_format=(type = CSV compression = GZIP record_delimiter=',')"#,
        r#"CREATE OR REPLACE STAGE test_stage url='azblob://load/files/' connection=(account_name='1a2b3c' account_key='4x5y6z') file_format=(type = CSV compression = GZIP record_delimiter=',')"#,
        r#"DROP STAGE abc"#,
        r#"DROP STAGE ~"#,
        r#"
            CREATE FILE FORMAT my_csv
                type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1;
        "#,
        r#"
            CREATE OR REPLACE FILE FORMAT my_csv
                type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1;
        "#,
        r#"CREATE file format my_orc type = orc"#,
        r#"CREATE file format my_orc type = orc missing_field_as=field_default"#,
        r#"CREATE file format my_orc type = orc missing_field_as='field_default'"#,
        r#"CREATE STAGE s file_format=(record_delimiter='\n' escape='\\');"#,
        r#"
            CREATE OR REPLACE DYNAMIC TABLE db.MyDynamic LIKE t
                TARGET_LAG = 10 SECOND
                WAREHOUSE = 'MyWarehouse'
                REFRESH_MODE = FULL
                INITIALIZE = ON_CREATE
                COMMENT = 'This is test dynamic table'
            AS
                SELECT * FROM t
        "#,
        r#"
            CREATE DYNAMIC TABLE IF NOT EXISTS db.MyDynamic (a int, b string)
                TARGET_LAG = 10 MINUTE
                WAREHOUSE = 'MyWarehouse'
                REFRESH_MODE = INCREMENTAL
                INITIALIZE = ON_SCHEDULE
                COMMENT = 'This is test dynamic table'
            AS
                SELECT * FROM t
        "#,
        r#"
            CREATE DYNAMIC TABLE db.MyDynamic (a int, b string)

                TARGET_LAG = 10 HOUR
                REFRESH_MODE = AUTO
                COMMENT = 'This is test dynamic table'
                STORAGE_FORMAT = 'native'
            AS
                SELECT c, d FROM t
        "#,
        r#"
            CREATE DYNAMIC TABLE IF NOT EXISTS MyDynamic (a int, b string)

                TARGET_LAG = 10 DAY
            AS
                SELECT avg(a), d FROM t GROUP BY d
        "#,
        r#"
            CREATE DYNAMIC TABLE IF NOT EXISTS MyDynamic (a int, b string)

                REFRESH_MODE = INCREMENTAL
                TARGET_LAG = DOWNSTREAM
            AS
                SELECT avg(a), d FROM db.t GROUP BY d
        "#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 WAREHOUSE = 'MyWarehouse' SCHEDULE = 15 MINUTE SUSPEND_TASK_AFTER_NUM_FAILURES = 3 ERROR_INTEGRATION = 'notification_name' COMMENT = 'This is test task 1' DATABASE = 'target', TIMEZONE = 'America/Los Angeles' AS SELECT * FROM MyTable1"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 WAREHOUSE = 'MyWarehouse' SCHEDULE = 15 SECOND SUSPEND_TASK_AFTER_NUM_FAILURES = 3 COMMENT = 'This is test task 1' AS SELECT * FROM MyTable1"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 WAREHOUSE = 'MyWarehouse' SCHEDULE = 1215 SECOND SUSPEND_TASK_AFTER_NUM_FAILURES = 3 COMMENT = 'This is test task 1' AS SELECT * FROM MyTable1"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 COMMENT = '123' SCHEDULE = 1215 SECOND WAREHOUSE = 'MyWarehouse' SUSPEND_TASK_AFTER_NUM_FAILURES = 3 AS SELECT * FROM MyTable1"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 SCHEDULE = USING CRON '0 6 * * *' 'America/Los_Angeles' COMMENT = 'serverless + cron' AS insert into t (c1, c2) values (1, 2), (3, 4)"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 SCHEDULE = USING CRON '0 12 * * *' AS COPY streams_test.paper_table FROM '/tmp/stream_stage.parquet' WITH (FORMAT parquet)"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 SCHEDULE = USING CRON '0 13 * * *' AS COPY canadian_city_population TO '/tmp/canadian_city_population.parquet' WITH (FORMAT parquet)"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 AFTER 'task2', 'task3' WHEN SYSTEM$GET_PREDECESSOR_RETURN_VALUE('task_name') != 'VALIDATION' AS VACUUM TABLE t"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 DATABASE = 'target', TIMEZONE = 'America/Los Angeles'  AS VACUUM TABLE t"#,
        r#"ALTER TASK MyTask1 RESUME"#,
        r#"ALTER TASK MyTask1 SUSPEND"#,
        r#"ALTER TASK MyTask1 ADD AFTER 'task2', 'task3'"#,
        r#"ALTER TASK MyTask1 REMOVE AFTER 'task2'"#,
        r#"ALTER TASK MyTask1 SET WAREHOUSE= 'MyWarehouse' SCHEDULE = USING CRON '0 6 * * *' 'America/Los_Angeles' COMMENT = 'serverless + cron'"#,
        r#"ALTER TASK MyTask1 SET WAREHOUSE= 'MyWarehouse' SCHEDULE = 13 MINUTE SUSPEND_TASK_AFTER_NUM_FAILURES = 10 COMMENT = 'serverless + cron'"#,
        r#"ALTER TASK MyTask1 SET SCHEDULE = 5 SECOND WAREHOUSE='MyWarehouse' SUSPEND_TASK_AFTER_NUM_FAILURES = 10 COMMENT = 'serverless + cron'"#,
        r#"ALTER TASK MyTask1 SET DATABASE='newDB', TIMEZONE='America/Los_Angeles'"#,
        r#"ALTER TASK MyTask1 SET ERROR_INTEGRATION = 'candidate_notifictaion'"#,
        r#"ALTER TASK MyTask2 MODIFY AS SELECT CURRENT_VERSION()"#,
        r#"ALTER TASK MyTask1 MODIFY WHEN SYSTEM$GET_PREDECESSOR_RETURN_VALUE('task_name') != 'VALIDATION'"#,
        r#"DROP TASK MyTask1"#,
        r#"EXECUTE TASK MyTask"#,
        r#"DESC TASK MyTask"#,
        r#"CREATE CONNECTION IF NOT EXISTS my_conn STORAGE_TYPE='s3'"#,
        r#"CREATE CONNECTION IF NOT EXISTS my_conn STORAGE_TYPE='s3' any_arg='any_value'"#,
        r#"CREATE OR REPLACE CONNECTION my_conn STORAGE_TYPE='s3' any_arg='any_value'"#,
        r#"DROP CONNECTION IF EXISTS my_conn;"#,
        r#"DESC CONNECTION my_conn;"#,
        r#"SHOW CONNECTIONS;"#,
        r#"CREATE PIPE IF NOT EXISTS MyPipe1 AUTO_INGEST = TRUE COMMENT = 'This is test pipe 1' AS COPY MyTable1 FROM '/tmp/MyStage1.csv' WITH (FORMAT csv)"#,
        r#"CREATE PIPE pipe1 AS COPY db1.MyTable1 FROM '/tmp/mybucket/data.csv'"#,
        r#"ALTER PIPE mypipe REFRESH"#,
        r#"ALTER PIPE mypipe REFRESH PREFIX='d1/'"#,
        r#"ALTER PIPE mypipe REFRESH PREFIX='d1/' MODIFIED_AFTER='2018-07-30T13:56:46-07:00'"#,
        r#"ALTER PIPE mypipe SET PIPE_EXECUTION_PAUSED = true"#,
        r#"DROP PIPE mypipe"#,
        r#"DESC PIPE mypipe"#,
        r#"CREATE NOTIFICATION INTEGRATION IF NOT EXISTS SampleNotification type = webhook enabled = true webhook = (url = 'https://example.com', method = 'GET', authorization_header = 'bearer auth')"#,
        r#"CREATE NOTIFICATION INTEGRATION SampleNotification type = webhook enabled = true webhook = (url = 'https://example.com') COMMENT = 'notify'"#,
        r#"ALTER NOTIFICATION INTEGRATION SampleNotification SET enabled = true"#,
        r#"ALTER NOTIFICATION INTEGRATION SampleNotification SET webhook = (url = 'https://example.com')"#,
        r#"ALTER NOTIFICATION INTEGRATION SampleNotification SET comment = '1'"#,
        r#"DROP NOTIFICATION INTEGRATION SampleNotification"#,
        r#"DESC NOTIFICATION INTEGRATION SampleNotification"#,
        r#"attach table t 's3://a' connection=(access_key_id ='x' secret_access_key ='y' endpoint_url='http://127.0.0.1:9900')"#,
        r#"CREATE FUNCTION IF NOT EXISTS isnotempty(p BOOLEAN) RETURNS BOOLEAN LANGUAGE python AS $$return [not value for value in p.materialize_py()]$$;"#,
        r#"CREATE FUNCTION IF NOT EXISTS isnotempty(p INT) RETURNS BOOLEAN LANGUAGE python IMMUTABLE AS $$return [value is not None for value in p.materialize_py()]$$;"#,
        r#"CREATE OR REPLACE FUNCTION isnotempty_test_replace(p STRING) RETURNS BOOLEAN LANGUAGE python STRICT AS $$return [value is not None and value != '' for value in p.materialize_py()]$$;"#,
        r#"CREATE OR REPLACE FUNCTION binary_reverse(arg0 BINARY) RETURNS BINARY LANGUAGE python HANDLER 'binary_reverse' AS $$return arg0$$;"#,
        r#"CREATE FUNCTION binary_reverse(arg0 BINARY) RETURNS BINARY LANGUAGE python HANDLER 'binary_reverse' PACKAGES ('numpy') IMPORTS ('@ss/abc') AS $$return arg0$$;"#,
        r#"CREATE FUNCTION binary_reverse_table() RETURNS TABLE (c1 INT) LANGUAGE python ROWS 16 AS $$return {'c1': [1, 2]}$$;"#,
        r#"CREATE OR REPLACE FUNCTION addone(i INT) RETURNS INT LANGUAGE python HANDLER 'addone_py' AS $$\nreturn i + 1\n$$;"#,
        r#"CREATE OR REPLACE FUNCTION addone(i INT) RETURNS INT LANGUAGE python IMPORTS ('@ss/abc') PACKAGES ('numpy', 'pandas') HANDLER 'addone_py' CAPABILITY PROFILE trusted_subinterpreter AS $$\nreturn i + 1\n$$;"#,
        r#"CREATE FUNCTION py_definer(a INT) RETURNS INT LANGUAGE python STABLE SECURITY DEFINER AS $$return a$$;"#,
        r#"
            create or replace function addone(i int)
            returns int
            language python
            handler 'addone_py'
            as
            $$
            def addone_py(i):
            return i+1
            $$;
        "#,
        r#"
            create or replace function addone(i int)
            returns int
            language python
            imports ('@ss/abc')
            packages ('numpy', 'pandas')
            handler 'addone_py'
            as
            $$
            def addone_py(i):
            return i+1
            $$;
        "#,
        r#"DROP FUNCTION binary_reverse(BINARY);"#,
        r#"DROP FUNCTION isnotempty(BOOLEAN);"#,
        r#"CREATE FUNCTION IF NOT EXISTS py_series(a INT) RETURNS TABLE (value INT) LANGUAGE python ROWS 4 AS $$return {'value': [1, 2, 3, 4]}$$;"#,
        r#"CREATE OR REPLACE DICTIONARY my_catalog.my_database.my_dictionary
            (
                user_name String,
                age Int16
            )
            PRIMARY KEY username
            SOURCE (mysql(host='localhost' username='root' password='1234'))
            COMMENT 'This is a comment';"#,
        r#"describe PROCEDURE p1()"#,
        r#"describe PROCEDURE p1(string, timestamp)"#,
        r#"drop PROCEDURE p1()"#,
        r#"drop PROCEDURE if exists p1()"#,
        r#"drop PROCEDURE p1(int, string)"#,
        r#"show PROCEDURES"#,
        r#"create or replace PROCEDURE p1() returns string not null language sql comment = 'test' as $$
            BEGIN
                LET sum := 0;
                FOR x IN SELECT * FROM numbers(100) DO
                    sum := sum + x.number;
                END FOR;
                RETURN sum;
            END;
            $$;"#,
        r#"create PROCEDURE if not exists p1() returns string not null language sql comment = 'test' as $$
            BEGIN
                LET sum := 0;
                FOR x IN SELECT * FROM numbers(100) DO
                    sum := sum + x.number;
                END FOR;
                RETURN sum;
            END;
            $$;"#,
        r#"create PROCEDURE p1() returns string not null language sql comment = 'test' as $$
            BEGIN
                LET sum := 0;
                FOR x IN SELECT * FROM numbers(100) DO
                    sum := sum + x.number;
                END FOR;
                RETURN sum;
            END;
            $$;"#,
        r#"create PROCEDURE p1(a int, b string) returns string not null language sql comment = 'test' as $$
            BEGIN
                LET sum := 0;
                FOR x IN SELECT * FROM numbers(100) DO
                    sum := sum + x.number;
                END FOR;
                RETURN sum;
            END;
            $$;"#,
        r#"create PROCEDURE p1() returns table(a string not null, b int null) language sql comment = 'test' as $$
            BEGIN
                LET sum := 0;
                FOR x IN SELECT * FROM numbers(100) DO
                    sum := sum + x.number;
                END FOR;
                RETURN sum;
            END;
            $$;"#,
        r#"DROP SEQUENCE IF EXISTS seq"#,
        r#"CREATE SEQUENCE seq comment='test'"#,
        r#"CREATE SEQUENCE seq start = 20 INCREMENT = 100 comment='test'"#,
        r#"CREATE SEQUENCE seq start WITH 20 INCREMENT BY 100 comment='test'"#,
        r#"DESCRIBE SEQUENCE seq"#,
        r#"SHOW SEQUENCES"#,
        r#"ALTER TABLE p1 CONNECTION=(CONNECTION_NAME='test')"#,
        r#"ALTER table t connection=(access_key_id ='x' secret_access_key ='y' endpoint_url='http://127.0.0.1:9900')"#,
        r#"create tag if not exists tag_a ALLOWED_VALUES = ('dev', 'prod') COMMENT = 'environment tag'"#,
        r#"REFRESH VIRTUAL COLUMN FOR t"#,
    ];

    write_statement_cases("ddl.txt", cases);
}

#[test]
fn test_ddl_statement_errors() {
    let cases = &[
        r#"create table a.b (c integer not null 1, b float(10))"#,
        r#"create table a (c float(10))"#,
        r#"create table a (c varch)"#,
        r#"create table a (c tuple())"#,
        r#"create table a (b tuple(c int, uint64));"#,
        r#"CREATE TABLE t(c1 NULLABLE(int) NOT NULL);"#,
        r#"create table a (c1 decimal(38), c2 int) partition by ();"#,
        r#"CREATE TABLE t(c1 int, c2 int) partition by (c1, c2) PROPERTIES ("read.split.target-size"='134217728', "read.split.metadata-target-size"=33554432);"#,
        r#"drop table if a.b"#,
        r#"create database ctl type=hive connection=(url='<hive-meta-store>' thrift_protocol='binary' warehouse='default');"#,
        r#"create table if test"#,
        r#"truncate table a.b.c.d"#,
        r#"truncate a"#,
        r#"drop a"#,
        r#"alter schema system x rename to db"#,
        r#"show columns from db1.t from ctl.db"#,
        r#"CREATE CONNECTION IF NOT EXISTS my_conn"#,
        r#"CREATE FUNCTION IF NOT EXISTS isnotempty(p BOOLEAN) RETURNS BOOLEAN LANGUAGE python HANDLER 'batch';"#,
        r#"CREATE FUNCTION py_missing_language(a INT) RETURNS INT AS $$return a$$;"#,
        r#"CREATE FUNCTION py_bad_rows(a INT) RETURNS TABLE (value INT) LANGUAGE python ROWS nope AS $$return {'value': [1]}$$;"#,
        r#"drop table :a"#,
        r#"drop table IDENTIFIER(a)"#,
        r#"drop table IDENTIFIER(:a)"#,
        r#"CREATE OR REPLACE DICTIONARY my_catalog.my_database.my_dictionary
            (
                user_name String,
                age Int16
            );"#,
        r#"CREATE OR REPLACE DICTIONARY my_catalog.my_database.my_dictionary
        (
            user_name tuple(),
            age Int16
        )
        PRIMARY KEY username
        SOURCE ()
        COMMENT 'This is a comment';"#,
        r#"desc procedure p1"#,
        r#"desc procedure p1(array, c int)"#,
        r#"drop procedure p1"#,
        r#"drop procedure p1(a int)"#,
        r#"create PROCEDURE p1() returns table(string not null, int null) language sql comment = 'test' as $$
            BEGIN
                LET sum := 0;
                FOR x IN SELECT * FROM numbers(100) DO
                    sum := sum + x.number;
                END FOR;
                RETURN sum;
            END;
            $$;"#,
        r#"create PROCEDURE p1(int, string) returns table(string not null, int null) language sql comment = 'test' as $$
            BEGIN
                LET sum := 0;
                FOR x IN SELECT * FROM numbers(100) DO
                    sum := sum + x.number;
                END FOR;
                RETURN sum;
            END;
            $$;"#,
    ];

    write_statement_error_cases("ddl-error.txt", cases);
}

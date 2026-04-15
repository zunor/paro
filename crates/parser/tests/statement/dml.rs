// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::common::{write_statement_cases, write_statement_error_cases};

#[test]
fn test_dml_statements() {
    let cases = &[
        r#"replace into test on(c) select sum(c) as c from source group by v;"#,
        r#"insert into t (c1, c2) values (1, 2), (3, 4);"#,
        r#"insert into t (c1, c2) values (1, 2);"#,
        r#"insert into table t select * from t2;"#,
        r#"insert overwrite into table t select * from t2;"#,
        r#"insert overwrite table t select * from t2;"#,
        r#"INSERT ALL
    WHEN c3 = 1 THEN
      INTO t1
    WHEN c3 = 3 THEN
      INTO t2
SELECT * from s;"#,
        r#"INSERT overwrite ALL
    WHEN c3 = 1 THEN
      INTO t1
    WHEN c3 = 3 THEN
      INTO t2
SELECT * from s;"#,
        r#"UPDATE db1.tb1 set a = a + 1, b = 2 WHERE c > 3;"#,
        r#"COPY mytable TO '/tmp/out.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL 'N')"#,
        r#"COPY mytable (c1, c2) FROM '/tmp/in.csv' DELIMITER ',' NULL 'N' CSV HEADER"#,
        r#"COPY (SELECT id, name FROM t WHERE id > 1) TO STDOUT WITH (FORMAT text)"#,
        r#"COPY mytable FROM STDIN WITH (FORMAT csv, FORCE_QUOTE *, FORCE_NOT_NULL (c1, c2)) WHERE id > 10"#,
    ];

    write_statement_cases("dml.txt", cases);
}

#[test]
fn test_dml_statement_errors() {
    let cases = &[
        r#"insert table t select * from t2;"#,
        r#"insert into t format"#,
        r#"COPY mytable FROM '/tmp/bucket' CONECTION= ();"#,
        r#"COPY mytable FROM @mystage CONNECTION = ();"#,
        r#"copy t1 from '' FILE"#,
        r#"copy t1 from '' FILE_FORMAT"#,
        r#"copy t1 from '' FILE_FORMAT = "#,
        r#"copy t1 from '' FILE_FORMAT = ("#,
        r#"copy t1 from '' FILE_FORMAT = (TYPE"#,
        r#"copy t1 from '' FILE_FORMAT = (TYPE ="#,
        r#"COPY t1 FROM '' PATTERN = '.*[.]csv' FILE_FORMAT = (type = TSV field_delimiter = '\t' skip_headerx = 0);"#,
        r#"COPY mytable
                FROM @my_stage
                FILE_FORMAT = (
                    type = CSV,
                    error_on_column_count_mismatch = 1
                )"#,
        r#"copy t1 from (select a from t where a = 1)"#,
    ];

    write_statement_error_cases("dml-error.txt", cases);
}

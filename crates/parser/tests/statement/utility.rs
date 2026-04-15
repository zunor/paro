// Copyright 2024-2026 Zunor
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::common::{write_statement_cases, write_statement_error_cases};

#[test]
fn test_utility_statements() {
    let cases = &[
        r#"checkpoint"#,
        r#"prepare stmt1 as select 1"#,
        r#"prepare stmt2(int, varchar) as select $1, $2"#,
        r#"execute stmt1"#,
        r#"declare c scroll cursor with hold for select 1"#,
        r#"fetch next from c"#,
        r#"move all from c"#,
        r#"close c"#,
        r#"discard all"#,
        r#"show processlist"#,
        r#"show functions"#,
        r#"show engines"#,
        r#"show metrics"#,
        r#"show table_functions"#,
        r#"show indexes"#,
        r#"show locks"#,
        r#"show stages"#,
        r#"show full streams"#,
        r#"show tasks"#,
        r#"list @stage_a;"#,
        r#"list @~;"#,
        r#"PRESIGN @my_stage"#,
        r#"PRESIGN @my_stage/path/to/dir/"#,
        r#"PRESIGN @my_stage/path/to/file"#,
        r#"PRESIGN @my_stage/my\ file.csv"#,
        r#"PRESIGN @my_stage/\"file\".csv"#,
        r#"PRESIGN @my_stage/\'file\'.csv"#,
        r#"PRESIGN @my_stage/\\file\\.csv"#,
        r#"PRESIGN DOWNLOAD @my_stage/path/to/file"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file EXPIRE=7200"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file EXPIRE=7200 CONTENT_TYPE='application/octet-stream'"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file CONTENT_TYPE='application/octet-stream' EXPIRE=7200"#,
        r#"VACUUM TABLE t;"#,
        r#"VACUUM TABLE t DRY RUN;"#,
        r#"VACUUM TABLE t DRY RUN SUMMARY;"#,
        r#"VACUUM DROP TABLE;"#,
        r#"VACUUM DROP TABLE DRY RUN;"#,
        r#"VACUUM DROP TABLE DRY RUN SUMMARY;"#,
        r#"VACUUM DROP TABLE FROM db;"#,
        r#"VACUUM DROP TABLE FROM db LIMIT 10;"#,
        r#"SHOW WAREHOUSES"#,
        r#"REMOVE @t;"#,
        r#"
            EXECUTE IMMEDIATE
            $$
            BEGIN
                LOOP
                    RETURN 1;
                END LOOP;
            END;
            $$
        "#,
    ];

    write_statement_cases("utility.txt", cases);
}

#[test]
fn test_utility_statement_errors() {
    let cases = &[r#"PRESIGN INVALID @my_stage/path/to/file"#];
    write_statement_error_cases("utility-error.txt", cases);
}

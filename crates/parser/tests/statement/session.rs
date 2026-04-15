// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::common::{write_statement_cases, write_statement_error_cases};

#[test]
fn test_session_statements() {
    let cases = &[
        r#"SET SECONDARY ROLES ALL"#,
        r#"SET SECONDARY ROLES NONE"#,
        r#"SET SECONDARY ROLES role1, role2"#,
        r#"SET ROLE `test-user`;"#,
        r#"SET ROLE 'test-user';"#,
        r#"SET ROLE ROLE1;"#,
        r#"SET max_threads = 10;"#,
        r#"SET max_threads = 10*2;"#,
        r#"SET global (max_threads, max_memory_usage) = (10*2, 10*4);"#,
        r#"UNSET max_threads;"#,
        r#"UNSET session max_threads;"#,
        r#"UNSET (max_threads, sql_dialect);"#,
        r#"UNSET session (max_threads, sql_dialect);"#,
        r#"SET variable a = 3"#,
        r#"SET variable a = select 3"#,
        r#"SET variable a = (select max(number) from numbers(10))"#,
        r#"show all"#,
        r#"show max_memory_usage"#,
        r#"USE WAREHOUSE my_wh"#,
    ];

    write_statement_cases("session.txt", cases);
}

#[test]
fn test_session_statement_errors() {
    let cases = &[r#"SET SECONDARY ROLES"#];
    write_statement_error_cases("session-error.txt", cases);
}

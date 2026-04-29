// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::common::write_statement_cases;

#[test]
fn test_transaction_statements() {
    let cases = &[
        r#"BEGIN"#,
        r#"BEGIN TRANSACTION"#,
        r#"BEGIN ISOLATION LEVEL SERIALIZABLE"#,
        r#"BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT READ ONLY"#,
        r#"START TRANSACTION"#,
        r#"START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ WRITE"#,
        r#"SET TRANSACTION ISOLATION LEVEL SNAPSHOT"#,
        r#"SET TRANSACTION READ ONLY"#,
        r#"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SNAPSHOT READ ONLY"#,
        r#"SAVEPOINT sp1"#,
        r#"RELEASE SAVEPOINT sp1"#,
        r#"RELEASE sp1"#,
        r#"ROLLBACK TO SAVEPOINT sp1"#,
        r#"ROLLBACK TO sp1"#,
        r#"COMMIT PREPARED 'gid'"#,
        r#"ROLLBACK PREPARED 'gid'"#,
        r#"COMMIT"#,
        r#"ROLLBACK"#,
        r#"ABORT"#,
    ];

    write_statement_cases("transaction.txt", cases);
}

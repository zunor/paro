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

use crate::common::write_statement_cases;

#[test]
fn test_transaction_statements() {
    let cases = &[
        r#"BEGIN"#,
        r#"BEGIN TRANSACTION"#,
        r#"START TRANSACTION"#,
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

// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::common::write_statement_cases;

#[test]
fn test_explain_statements() {
    let cases = &[
        r#"explain pipeline select a from b;"#,
        r#"explain replace into test on(c) select sum(c) as c from source group by v;"#,
        r#"explain pipeline select a from t1 ignore_result;"#,
        r#"explain(verbose, logical, optimized) select * from t where a = 1"#,
        r#"EXPLAIN ANALYZE SELECT 1"#,
        r#"EXPLAIN ANALYZE PARTIAL SELECT 1"#,
        r#"EXPLAIN ANALYZE GRAPHICAL SELECT 1"#,
    ];

    write_statement_cases("explain.txt", cases);
}

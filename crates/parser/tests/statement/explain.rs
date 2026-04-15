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

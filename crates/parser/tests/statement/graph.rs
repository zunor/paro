// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::common::write_statement_cases;

#[test]
fn test_graph_statements() {
    let cases = &[
        r#"
            CREATE PROPERTY GRAPH IF NOT EXISTS social_graph
              VERTEX TABLES (
                person AS p KEY (id) LABEL Person PROPERTIES (name AS person_name, age),
                company KEY (id) PROPERTIES ALL
              )
              EDGE TABLES (
                knows AS k KEY (id)
                  SOURCE KEY (src_id) REFERENCES person (id)
                  DESTINATION KEY (dst_id) REFERENCES person (id)
                  LABEL Knows
                  PROPERTIES NONE
              )
        "#,
        r#"DROP PROPERTY GRAPH IF EXISTS social_graph"#,
        r#"REFRESH PROPERTY GRAPH social_graph"#,
    ];

    write_statement_cases("graph.txt", cases);
}

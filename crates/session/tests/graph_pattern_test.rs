// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/instance_memory.rs"]
mod instance_memory;
#[path = "common/query_single_i64.rs"]
mod query_single_i64;

use exec_ok::exec_ok;
use instance_memory::create_in_memory_instance;
use paro_session::{CollectingSink, Session};
use query_single_i64::query_single_i64;

#[tokio::test]
async fn shared_vertices_cycles_and_match_predicates_preserve_bags() {
    let mut session = Session::new(1, create_in_memory_instance());
    let mut sink = CollectingSink::new();
    for sql in [
        "SET threads = 1",
        "CREATE TABLE v (id BIGINT PRIMARY KEY)",
        "CREATE TABLE e (src BIGINT, dst BIGINT)",
        "INSERT INTO v VALUES (1), (2), (3), (4)",
        "INSERT INTO e VALUES (1,2), (1,2), (2,3), (3,1), (1,1), (3,4)",
        "CREATE PROPERTY GRAPH g VERTEX TABLES (v LABEL node) EDGE TABLES
          (e SOURCE KEY (src) REFERENCES v(id) DESTINATION KEY (dst) REFERENCES v(id) LABEL link)",
    ] {
        exec_ok(&mut session, &mut sink, sql).await;
    }
    // Independently enumerate the tiny edge bag; no optimizer-dependent oracle.
    let edges = [(1, 2), (1, 2), (2, 3), (3, 1), (1, 1), (3, 4)];
    let triangles = edges
        .iter()
        .flat_map(|x| edges.iter().map(move |y| (x, y)))
        .flat_map(|(x, y)| edges.iter().map(move |z| (x, y, z)))
        .filter(|(x, y, z)| x.1 == y.0 && y.1 == z.0 && z.1 == x.0)
        .count() as i64;
    // Duplicate
    // edges, a self-loop, a cycle, repeated labels, reverse edges and filters
    // involving different variables must all survive the same SQL entry point.
    let cases = [
        ("(a:node)-[x:link]->(b:node), (a WHERE a.id=b.id)", "", 1),
        (
            "(a:node)-[x:link]->(b:node), (a)-[y:link]->(c:node)",
            "",
            14,
        ),
        (
            "(a:node)-[x:link]->(b:node)-[y:link]->(c:node)-[z:link]->(a:node)",
            "",
            triangles,
        ),
        (
            "(a:node), (b:node)-[x:link]->(a), (b)-[y:link]->(c:node)",
            "WHERE a.id <> c.id",
            6,
        ),
        ("(a:node)-[x:link]->(a:node)", "", 1),
        (
            "(a:node)-[x:link]->(b:node), (b:node WHERE b.id=2)",
            "WHERE a.id<>b.id",
            2,
        ),
        ("(a:node)", "WHERE a.id > 2", 2),
    ];
    for (pattern, predicate, expected) in cases {
        let sql = format!(
            "SELECT count(*) FROM GRAPH_TABLE(g MATCH {pattern} {predicate} COLUMNS (a.id AS id))"
        );
        exec_ok(&mut session, &mut sink, &sql).await;
        assert_eq!(query_single_i64(&sink), expected, "{sql}");
    }
}

use paro_parser::ast::{CTEHint, Statement};
use paro_parser::parse_one;
#[cfg(debug_assertions)]
use paro_parser::parser_testing::{parser_stack_stats_snapshot, reset_parser_stack_stats};

#[test]
fn parse_cte_materialization_hints() {
    let statement = parse_one(
        "WITH c1 AS MATERIALIZED (SELECT 1), c2 AS NOT MATERIALIZED (SELECT 2), c3 AS (SELECT 3) SELECT * FROM c1, c2, c3",
    )
    .expect("parse");

    let Statement::Query(query) = statement.stmt else {
        panic!("expected query statement");
    };
    let with = query.with.expect("expected WITH clause");
    assert_eq!(with.ctes.len(), 3);
    assert_eq!(with.ctes[0].materialization, CTEHint::Materialized);
    assert_eq!(with.ctes[1].materialization, CTEHint::NotMaterialized);
    assert_eq!(with.ctes[2].materialization, CTEHint::Default);
}

#[test]
fn cte_display_round_trips_materialization_hints() {
    let sql =
        "WITH c1 AS MATERIALIZED (SELECT 1), c2 AS NOT MATERIALIZED (SELECT 2) SELECT * FROM c1, c2";
    let statement = parse_one(sql).expect("parse");
    assert_eq!(statement.stmt.to_string(), sql);
}

#[test]
fn deeply_nested_cte_parse() {
    let sql = (0..96).fold("SELECT 1".to_string(), |inner, depth| {
        format!("WITH RECURSIVE t{depth} AS ({inner}) SELECT * FROM t{depth}")
    });

    #[cfg(debug_assertions)]
    reset_parser_stack_stats();

    let statement = parse_one(&sql).expect("parse");
    assert!(matches!(statement.stmt, Statement::Query(_)));

    #[cfg(debug_assertions)]
    {
        let stats = parser_stack_stats_snapshot();
        assert!(
            stats.samples > 0,
            "expected stack samples for deep nested CTEs"
        );
    }
}

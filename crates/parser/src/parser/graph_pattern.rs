// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use nom::Parser;
use nom_rule::rule;

use crate::ast::EdgeDirection;
use crate::ast::EdgePattern;
use crate::ast::GraphColumnDef;
use crate::ast::GraphMatchClause;
use crate::ast::GraphPattern;
use crate::ast::GraphTableRef;
use crate::ast::PathMode;
use crate::ast::PathPattern;
use crate::ast::PathQuantifier;
use crate::ast::PatternElement;
use crate::ast::VertexPattern;
use crate::parser::common::map;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::expr::expr;
use crate::parser::input::Input;
use crate::parser::token::TokenKind::*;
use crate::parser::ErrorKind;

fn graph_table_keyword(i: Input) -> IResult<()> {
    map_res(rule! { #ident }, |name| {
        if name.name.eq_ignore_ascii_case("GRAPH_TABLE") {
            Ok(())
        } else {
            Err(nom::Err::Error(ErrorKind::ExpectText("GRAPH_TABLE")))
        }
    })
    .parse(i)
}

fn match_keyword(i: Input) -> IResult<()> {
    map_res(rule! { #ident }, |name| {
        if name.name.eq_ignore_ascii_case("MATCH") {
            Ok(())
        } else {
            Err(nom::Err::Error(ErrorKind::ExpectText("MATCH")))
        }
    })
    .parse(i)
}

pub fn graph_table_ref(i: Input) -> IResult<GraphTableRef> {
    map(
        rule! {
            #graph_table_keyword ~ ^"("
            ~ ^#ident
            ~ ^#graph_match_clause
            ~ ^COLUMNS ~ ^"(" ~ ^#comma_separated_list1(graph_column_def) ~ ^")"
            ~ ^")"
        },
        |(_, _, graph_name, match_clause, _, _, columns, _, _)| GraphTableRef {
            graph_name,
            match_clause,
            columns,
        },
    )
    .parse(i)
}

fn path_mode(i: Input) -> IResult<PathMode> {
    alt((
        map(rule! { ANY ~ SHORTEST }, |_| PathMode::AnyShortest),
        map(rule! { ALL ~ SHORTEST }, |_| PathMode::AllShortest),
        map(rule! { ANY }, |_| PathMode::Any),
        map(rule! { ALL }, |_| PathMode::All),
    ))
    .parse(i)
}

fn graph_match_clause(i: Input) -> IResult<GraphMatchClause> {
    map(
        rule! {
            #match_keyword
            ~ ( #ident ~ "=" )?
            ~ #path_mode?
            ~ #graph_pattern
            ~ ( WHERE ~ ^#expr )?
        },
        |(_, path_variable, path_mode, pattern, where_clause)| GraphMatchClause {
            path_variable: path_variable.map(|(variable, _)| variable),
            path_mode,
            pattern,
            where_clause: where_clause.map(|(_, expr)| Box::new(expr)),
        },
    )
    .parse(i)
}

// MVP: single path only; AST supports Vec<PathPattern> for future multi-path extension.
fn graph_pattern(i: Input) -> IResult<GraphPattern> {
    map(rule! { #path_pattern }, |path| GraphPattern {
        paths: vec![path],
    })
    .parse(i)
}

fn path_pattern(i: Input) -> IResult<PathPattern> {
    let (mut rest, first_vertex) = vertex_pattern(i)?;
    let mut elements = vec![PatternElement::Vertex(first_vertex)];

    loop {
        match edge_pattern(rest) {
            Ok((after_edge, edge)) => {
                let (after_vertex, vertex) = vertex_pattern(after_edge)?;
                elements.push(PatternElement::Edge(edge));
                elements.push(PatternElement::Vertex(vertex));
                rest = after_vertex;
            }
            Err(nom::Err::Error(_)) => break,
            Err(err) => return Err(err),
        }
    }

    Ok((rest, PathPattern { elements }))
}

fn vertex_pattern(i: Input) -> IResult<VertexPattern> {
    map(
        rule! {
            "(" ~ #ident? ~ ( ":" ~ ^#ident )? ~ ( WHERE ~ ^#expr )? ~ ")"
        },
        |(_, variable, label, where_clause, _)| VertexPattern {
            variable,
            label: label.map(|(_, label)| label),
            where_clause: where_clause.map(|(_, expr)| Box::new(expr)),
        },
    )
    .parse(i)
}

fn edge_pattern(i: Input) -> IResult<EdgePattern> {
    let left_arrow = alt((
        map(rule! { "<" ~ "-" ~ "[" }, |_| EdgeDirection::Left),
        map(rule! { "-" ~ "[" }, |_| EdgeDirection::Right),
    ));

    let edge_inner = map(
        rule! { #ident? ~ ( ":" ~ ^#ident )? ~ ( WHERE ~ ^#expr )? ~ "]" },
        |(variable, label, where_clause, _)| {
            (
                variable,
                label.map(|(_, label)| label),
                where_clause.map(|(_, expr)| Box::new(expr)),
            )
        },
    );

    let right_arrow = alt((
        map(rule! { RArrow }, |_| true),
        map(rule! { "-" }, |_| false),
    ));

    map_res(
        rule! { #left_arrow ~ #edge_inner ~ #right_arrow ~ #path_quantifier? },
        |(left, (variable, label, where_clause), right, quantifier)| {
            let direction = match (left, right) {
                (EdgeDirection::Right, true) => EdgeDirection::Right,
                (EdgeDirection::Right, false) => EdgeDirection::Undirected,
                (EdgeDirection::Left, false) => EdgeDirection::Left,
                (EdgeDirection::Left, true) => EdgeDirection::LeftRight,
                _ => {
                    return Err(nom::Err::Failure(ErrorKind::Other(
                        "invalid graph edge direction",
                    )))
                }
            };

            Ok(EdgePattern {
                variable,
                label,
                direction,
                quantifier,
                where_clause,
            })
        },
    )
    .parse(i)
}

fn uint_literal(i: Input) -> IResult<u64> {
    map_res(rule! { LiteralInteger }, |token| {
        token
            .text()
            .replace('_', "")
            .parse::<u64>()
            .map_err(|_| nom::Err::Failure(ErrorKind::Other("invalid integer literal")))
    })
    .parse(i)
}

fn path_quantifier(i: Input) -> IResult<PathQuantifier> {
    alt((
        map(rule! { "+" }, |_| PathQuantifier::Plus),
        map(rule! { "*" }, |_| PathQuantifier::Star),
        map(
            rule! { "{" ~ #uint_literal ~ "," ~ #uint_literal? ~ "}" },
            |(_, lower, _, upper, _)| PathQuantifier::Bounded { lower, upper },
        ),
    ))
    .parse(i)
}

fn graph_column_def(i: Input) -> IResult<GraphColumnDef> {
    map(
        rule! {
            #expr ~ ( AS ~ ^#ident )?
        },
        |(expr, alias)| GraphColumnDef {
            expr,
            alias: alias.map(|(_, alias)| alias),
        },
    )
    .parse(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::PathMode;
    use crate::ast::TableReference;

    #[test]
    fn test_parse_graph_table_one_hop() {
        let sql = r#"
            SELECT *
            FROM GRAPH_TABLE(
                social_network
                MATCH (a:Person)-[k:Knows]->(b:Person)
                COLUMNS (a.name AS from_name, b.name AS to_name, k.since)
            ) gt
        "#;

        let stmt = crate::parse_one(sql).unwrap().stmt;
        let crate::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let crate::ast::SetExpr::Select(select) = &query.body else {
            panic!("expected select");
        };
        let TableReference::GraphTable {
            graph_table, alias, ..
        } = &select.from[0]
        else {
            panic!("expected graph table");
        };

        assert_eq!(graph_table.graph_name.name, "social_network");
        assert_eq!(graph_table.columns.len(), 3);
        assert_eq!(alias.as_ref().map(|a| a.name.name.as_str()), Some("gt"));
    }

    #[test]
    fn test_parse_graph_table_multi_hop_and_where() {
        let sql = r#"
            SELECT *
            FROM GRAPH_TABLE(
                social_network
                MATCH (a:Person WHERE a.name = 'Alice')-[k1:Knows]->(b)-[k2:Knows]->(c:Person)
                WHERE c.age > 18
                COLUMNS (a.name AS a_name, b.name AS b_name, c.name AS c_name)
            )
        "#;

        let stmt = crate::parse_one(sql).unwrap().stmt;
        let crate::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let crate::ast::SetExpr::Select(select) = &query.body else {
            panic!("expected select");
        };
        let TableReference::GraphTable { graph_table, .. } = &select.from[0] else {
            panic!("expected graph table");
        };

        let path = &graph_table.match_clause.pattern.paths[0];
        assert_eq!(path.elements.len(), 5);
        assert!(graph_table.match_clause.where_clause.is_some());
    }

    #[test]
    fn test_parse_graph_table_edge_directions() {
        let sql = r#"
            SELECT *
            FROM GRAPH_TABLE(
                social_network
                MATCH (a)<-[k1:Knows]-(b)-[k2:Knows]-(c)<-[k3:Knows]->(d)
                COLUMNS (a.id, b.id, c.id, d.id)
            )
        "#;

        let stmt = crate::parse_one(sql).unwrap().stmt;
        let crate::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let crate::ast::SetExpr::Select(select) = &query.body else {
            panic!("expected select");
        };
        let TableReference::GraphTable { graph_table, .. } = &select.from[0] else {
            panic!("expected graph table");
        };

        let path = &graph_table.match_clause.pattern.paths[0];
        let PatternElement::Edge(e1) = &path.elements[1] else {
            panic!("expected first edge");
        };
        let PatternElement::Edge(e2) = &path.elements[3] else {
            panic!("expected second edge");
        };
        let PatternElement::Edge(e3) = &path.elements[5] else {
            panic!("expected third edge");
        };
        assert_eq!(e1.direction, EdgeDirection::Left);
        assert_eq!(e2.direction, EdgeDirection::Undirected);
        assert_eq!(e3.direction, EdgeDirection::LeftRight);
    }

    #[test]
    fn test_parse_graph_table_bounded_quantifier() {
        let sql = r#"
            SELECT *
            FROM GRAPH_TABLE(
                social_network
                MATCH (a)-[k:Knows]->{1,3}(b)
                COLUMNS (a.id, b.id)
            )
        "#;

        let stmt = crate::parse_one(sql).unwrap().stmt;
        let crate::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let crate::ast::SetExpr::Select(select) = &query.body else {
            panic!("expected select");
        };
        let TableReference::GraphTable { graph_table, .. } = &select.from[0] else {
            panic!("expected graph table");
        };

        let path = &graph_table.match_clause.pattern.paths[0];
        let PatternElement::Edge(edge) = &path.elements[1] else {
            panic!("expected edge");
        };
        assert!(matches!(
            edge.quantifier,
            Some(PathQuantifier::Bounded {
                lower: 1,
                upper: Some(3),
            })
        ));
    }

    #[test]
    fn test_parse_graph_table_star_and_plus_quantifiers() {
        for (sql, expected_is_plus) in [
            (
                r#"
                SELECT * FROM GRAPH_TABLE(
                    social_network
                    MATCH (a)-[e:Edge]->*(b)
                    COLUMNS (a.id, b.id)
                )
                "#,
                false,
            ),
            (
                r#"
                SELECT * FROM GRAPH_TABLE(
                    social_network
                    MATCH (a)-[e:Edge]->(b)-[e2:Edge]->+(c)
                    COLUMNS (a.id, b.id, c.id)
                )
                "#,
                true,
            ),
        ] {
            let stmt = crate::parse_one(sql).unwrap().stmt;
            let crate::ast::Statement::Query(query) = stmt else {
                panic!("expected query statement");
            };
            let crate::ast::SetExpr::Select(select) = &query.body else {
                panic!("expected select");
            };
            let TableReference::GraphTable { graph_table, .. } = &select.from[0] else {
                panic!("expected graph table");
            };

            let path = &graph_table.match_clause.pattern.paths[0];
            let edge_index = if expected_is_plus { 3 } else { 1 };
            let PatternElement::Edge(edge) = &path.elements[edge_index] else {
                panic!("expected edge");
            };
            if expected_is_plus {
                assert!(matches!(edge.quantifier, Some(PathQuantifier::Plus)));
            } else {
                assert!(matches!(edge.quantifier, Some(PathQuantifier::Star)));
            }
        }
    }

    #[test]
    fn test_parse_graph_table_path_modes_and_variables() {
        let sql = r#"
            SELECT *
            FROM GRAPH_TABLE(
                social_network
                MATCH p = ANY SHORTEST (a:Person)-[e:Knows]->{1,5}(b:Person)
                COLUMNS (a.name, b.name, path_length(p))
            )
        "#;

        let stmt = crate::parse_one(sql).unwrap().stmt;
        let crate::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let crate::ast::SetExpr::Select(select) = &query.body else {
            panic!("expected select");
        };
        let TableReference::GraphTable { graph_table, .. } = &select.from[0] else {
            panic!("expected graph table");
        };

        assert_eq!(
            graph_table
                .match_clause
                .path_variable
                .as_ref()
                .map(|var| var.name.as_str()),
            Some("p")
        );
        assert_eq!(
            graph_table.match_clause.path_mode,
            Some(PathMode::AnyShortest)
        );
    }
}

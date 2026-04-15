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

use nom::Parser;
use nom_rule::rule;

use crate::ast::CreatePropertyGraphStmt;
use crate::ast::DropPropertyGraphStmt;
use crate::ast::EdgeEndpointDef;
use crate::ast::EdgeTableDef;
use crate::ast::PropertyDef;
use crate::ast::PropertySpec;
use crate::ast::RefreshPropertyGraphStmt;
use crate::ast::Statement;
use crate::ast::VertexTableDef;
use crate::parser::common::map;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::input::Input;
use crate::parser::token::TokenKind::*;

pub fn create_property_graph(i: Input) -> IResult<Statement> {
    map(
        rule! {
            CREATE ~ PROPERTY ~ GRAPH ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ VERTEX ~ TABLES ~ "(" ~ #comma_separated_list1(vertex_table_def) ~ ")"
            ~ EDGE ~ TABLES ~ "(" ~ #comma_separated_list1(edge_table_def) ~ ")"
        },
        |(
            _,
            _,
            _,
            opt_if_not_exists,
            graph_name,
            _,
            _,
            _,
            vertex_tables,
            _,
            _,
            _,
            _,
            edge_tables,
            _,
        )| {
            Statement::CreatePropertyGraph(CreatePropertyGraphStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                graph_name,
                vertex_tables,
                edge_tables,
            })
        },
    )
    .parse(i)
}

pub fn drop_property_graph(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ PROPERTY ~ GRAPH ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, _, opt_if_exists, graph_name)| {
            Statement::DropPropertyGraph(DropPropertyGraphStmt {
                if_exists: opt_if_exists.is_some(),
                graph_name,
            })
        },
    )
    .parse(i)
}

pub fn refresh_property_graph(i: Input) -> IResult<Statement> {
    map(
        rule! {
            REFRESH ~ PROPERTY ~ GRAPH ~ #ident
        },
        |(_, _, _, graph_name)| {
            Statement::RefreshPropertyGraph(RefreshPropertyGraphStmt { graph_name })
        },
    )
    .parse(i)
}

fn vertex_table_def(i: Input) -> IResult<VertexTableDef> {
    let (mut i, table_name) = ident(i)?;
    let (i2, alias) = rule! { ( AS ~ ^#ident )? }.parse(i)?;
    i = i2;

    let mut key_columns = None;
    let mut label = None;
    let mut properties = None;

    loop {
        match i.tokens.first().map(|token| token.kind) {
            Some(KEY) if key_columns.is_none() => {
                let (next, (_, _, cols, _)) =
                    rule! { KEY ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")" }.parse(i)?;
                key_columns = Some(cols);
                i = next;
            }
            Some(LABEL) if label.is_none() => {
                let (next, (_, parsed_label)) = rule! { LABEL ~ ^#ident }.parse(i)?;
                label = Some(parsed_label);
                i = next;
            }
            Some(PROPERTIES) if properties.is_none() => {
                let (next, (_, parsed_properties)) =
                    rule! { PROPERTIES ~ ^#property_spec }.parse(i)?;
                properties = Some(parsed_properties);
                i = next;
            }
            _ => break,
        }
    }

    Ok((
        i,
        VertexTableDef {
            table_name,
            alias: alias.map(|(_, alias)| alias),
            key_columns,
            label,
            properties,
        },
    ))
}

fn edge_table_def(i: Input) -> IResult<EdgeTableDef> {
    map(
        rule! {
            #ident
            ~ ( AS ~ ^#ident )?
            ~ ( KEY ~ ^"(" ~ ^#comma_separated_list1(ident) ~ ^")" )?
            ~ SOURCE ~ ^#edge_endpoint_def
            ~ DESTINATION ~ ^#edge_endpoint_def
            ~ ( LABEL ~ ^#ident )?
            ~ ( PROPERTIES ~ ^#property_spec )?
        },
        |(table_name, alias, key_columns, _, source, _, destination, label, properties)| {
            EdgeTableDef {
                table_name,
                alias: alias.map(|(_, alias)| alias),
                key_columns: key_columns.map(|(_, _, cols, _)| cols),
                source,
                destination,
                label: label.map(|(_, label)| label),
                properties: properties.map(|(_, properties)| properties),
            }
        },
    )
    .parse(i)
}

fn edge_endpoint_def(i: Input) -> IResult<EdgeEndpointDef> {
    map(
        rule! {
            KEY ~ "(" ~ #comma_separated_list1(ident) ~ ")" ~ REFERENCES ~ #ident
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
        },
        |(_, _, key_columns, _, _, references_table, references_columns)| EdgeEndpointDef {
            key_columns,
            references_table,
            references_columns: references_columns.map(|(_, cols, _)| cols),
        },
    )
    .parse(i)
}

fn property_spec(i: Input) -> IResult<PropertySpec> {
    let all = map(rule! { ALL }, |_| PropertySpec::All);
    let none = map(rule! { NONE }, |_| PropertySpec::None);
    let columns = map(
        rule! { "(" ~ #comma_separated_list1(property_def) ~ ")" },
        |(_, columns, _)| PropertySpec::Columns(columns),
    );
    let except = map(
        rule! { EXCEPT ~ "(" ~ #comma_separated_list1(ident) ~ ")" },
        |(_, _, columns, _)| PropertySpec::Except(columns),
    );

    rule!(
        #all
        | #columns
        | #except
        | #none
    )
    .parse(i)
}

fn property_def(i: Input) -> IResult<PropertyDef> {
    map(
        rule! {
            #ident ~ ( AS ~ ^#ident )?
        },
        |(column_name, alias)| PropertyDef {
            column_name,
            alias: alias.map(|(_, alias)| alias),
        },
    )
    .parse(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_property_graph_full() {
        let sql = r#"
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
                  PROPERTIES NONE,
                works_at
                  SOURCE KEY (person_id) REFERENCES person
                  DESTINATION KEY (company_id) REFERENCES company
                  PROPERTIES EXCEPT (created_at)
              )
        "#;

        let stmt = crate::parse_one(sql).unwrap().stmt;
        let Statement::CreatePropertyGraph(stmt) = stmt else {
            panic!("expected CREATE PROPERTY GRAPH");
        };

        assert!(stmt.if_not_exists);
        assert_eq!(stmt.graph_name.name, "social_graph");
        assert_eq!(stmt.vertex_tables.len(), 2);
        assert_eq!(stmt.edge_tables.len(), 2);
        assert_eq!(
            stmt.vertex_tables[0]
                .alias
                .as_ref()
                .map(|i| i.name.as_str()),
            Some("p")
        );
        assert_eq!(stmt.vertex_tables[1].label, None);
        assert_eq!(
            stmt.edge_tables[0].label.as_ref().map(|i| i.name.as_str()),
            Some("Knows")
        );
    }

    #[test]
    fn test_parse_create_property_graph_optional_combinations() {
        let sql = r#"
            CREATE PROPERTY GRAPH g
              VERTEX TABLES (
                v1,
                v2 LABEL L2,
                v3 PROPERTIES EXCEPT (c1, c2)
              )
              EDGE TABLES (
                e1 SOURCE KEY (s) REFERENCES v1 DESTINATION KEY (d) REFERENCES v2,
                e2 KEY (id) SOURCE KEY (s) REFERENCES v1 (id) DESTINATION KEY (d) REFERENCES v3 LABEL E2 PROPERTIES (p AS p_alias)
              )
        "#;

        let stmt = crate::parse_one(sql).unwrap().stmt;
        let Statement::CreatePropertyGraph(stmt) = stmt else {
            panic!("expected CREATE PROPERTY GRAPH");
        };

        assert!(!stmt.if_not_exists);
        assert_eq!(stmt.vertex_tables[0].key_columns, None);
        assert!(matches!(
            stmt.vertex_tables[2].properties,
            Some(PropertySpec::Except(_))
        ));
        assert_eq!(stmt.edge_tables[0].key_columns, None);
        assert!(matches!(
            stmt.edge_tables[1].properties,
            Some(PropertySpec::Columns(_))
        ));
    }

    #[test]
    fn test_parse_create_property_graph_vertex_clause_order() {
        let sql = r#"
            CREATE PROPERTY GRAPH g
              VERTEX TABLES (
                v LABEL Person PROPERTIES (name) KEY (tenant_id, person_code)
              )
              EDGE TABLES (
                e SOURCE KEY (src_tenant, src_code) REFERENCES v (tenant_id, person_code)
                  DESTINATION KEY (dst_tenant, dst_code) REFERENCES v (tenant_id, person_code)
                  LABEL Knows
              )
        "#;

        let stmt = crate::parse_one(sql).unwrap().stmt;
        let Statement::CreatePropertyGraph(stmt) = stmt else {
            panic!("expected CREATE PROPERTY GRAPH");
        };

        assert_eq!(
            stmt.vertex_tables[0]
                .label
                .as_ref()
                .map(|ident| ident.name.as_str()),
            Some("Person")
        );
        assert!(matches!(
            stmt.vertex_tables[0].properties,
            Some(PropertySpec::Columns(_))
        ));
        assert_eq!(
            stmt.vertex_tables[0].key_columns.as_ref().map(|cols| cols
                .iter()
                .map(|ident| ident.name.as_str())
                .collect::<Vec<_>>()),
            Some(vec!["tenant_id", "person_code"])
        );
    }

    #[test]
    fn test_parse_drop_property_graph() {
        let stmt = crate::parse_one("DROP PROPERTY GRAPH IF EXISTS social_graph")
            .unwrap()
            .stmt;
        let Statement::DropPropertyGraph(stmt) = stmt else {
            panic!("expected DROP PROPERTY GRAPH");
        };

        assert!(stmt.if_exists);
        assert_eq!(stmt.graph_name.name, "social_graph");
    }

    #[test]
    fn test_parse_refresh_property_graph() {
        let stmt = crate::parse_one("REFRESH PROPERTY GRAPH social_graph")
            .unwrap()
            .stmt;
        let Statement::RefreshPropertyGraph(stmt) = stmt else {
            panic!("expected REFRESH PROPERTY GRAPH");
        };

        assert_eq!(stmt.graph_name.name, "social_graph");
    }

    #[test]
    fn test_parse_property_graph_syntax_error() {
        let err = crate::parse_one(
            "CREATE PROPERTY GRAPH g VERTEX TABLES (v) EDGE TABLES (e SOURCE KEY (s) REFERENCES v)",
        )
        .unwrap_err();
        let msg = err.message.to_uppercase();
        assert!(msg.contains("DESTINATION") || msg.contains("EXPECTING"));
    }
}

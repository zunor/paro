// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreatePropertyGraphStmt {
    pub if_not_exists: bool,
    pub graph_name: Identifier,
    pub vertex_tables: Vec<VertexTableDef>,
    pub edge_tables: Vec<EdgeTableDef>,
}

impl Display for CreatePropertyGraphStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE PROPERTY GRAPH ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.graph_name)?;

        write!(f, " VERTEX TABLES (")?;
        write_comma_separated_list(f, &self.vertex_tables)?;
        write!(f, ") EDGE TABLES (")?;
        write_comma_separated_list(f, &self.edge_tables)?;
        write!(f, ")")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct VertexTableDef {
    pub table_name: Identifier,
    pub alias: Option<Identifier>,
    pub key_columns: Option<Vec<Identifier>>,
    pub label: Option<Identifier>,
    pub properties: Option<PropertySpec>,
}

impl Display for VertexTableDef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.table_name)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {}", alias)?;
        }
        if let Some(key_columns) = &self.key_columns {
            write!(f, " KEY (")?;
            write_comma_separated_list(f, key_columns)?;
            write!(f, ")")?;
        }
        if let Some(label) = &self.label {
            write!(f, " LABEL {}", label)?;
        }
        if let Some(properties) = &self.properties {
            write!(f, " PROPERTIES {}", properties)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct EdgeTableDef {
    pub table_name: Identifier,
    pub alias: Option<Identifier>,
    pub key_columns: Option<Vec<Identifier>>,
    pub source: EdgeEndpointDef,
    pub destination: EdgeEndpointDef,
    pub label: Option<Identifier>,
    pub properties: Option<PropertySpec>,
}

impl Display for EdgeTableDef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.table_name)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {}", alias)?;
        }
        if let Some(key_columns) = &self.key_columns {
            write!(f, " KEY (")?;
            write_comma_separated_list(f, key_columns)?;
            write!(f, ")")?;
        }

        write!(f, " SOURCE {}", self.source)?;
        write!(f, " DESTINATION {}", self.destination)?;

        if let Some(label) = &self.label {
            write!(f, " LABEL {}", label)?;
        }
        if let Some(properties) = &self.properties {
            write!(f, " PROPERTIES {}", properties)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct EdgeEndpointDef {
    pub key_columns: Vec<Identifier>,
    pub references_table: Identifier,
    pub references_columns: Option<Vec<Identifier>>,
}

impl Display for EdgeEndpointDef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "KEY (")?;
        write_comma_separated_list(f, &self.key_columns)?;
        write!(f, ") REFERENCES {}", self.references_table)?;
        if let Some(columns) = &self.references_columns {
            write!(f, " (")?;
            write_comma_separated_list(f, columns)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum PropertySpec {
    All,
    Columns(Vec<PropertyDef>),
    Except(Vec<Identifier>),
    None,
}

impl Display for PropertySpec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PropertySpec::All => write!(f, "ALL"),
            PropertySpec::Columns(columns) => {
                write!(f, "(")?;
                write_comma_separated_list(f, columns)?;
                write!(f, ")")
            }
            PropertySpec::Except(columns) => {
                write!(f, "EXCEPT (")?;
                write_comma_separated_list(f, columns)?;
                write!(f, ")")
            }
            PropertySpec::None => write!(f, "NONE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct PropertyDef {
    pub column_name: Identifier,
    pub alias: Option<Identifier>,
}

impl Display for PropertyDef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.column_name)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {}", alias)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropPropertyGraphStmt {
    pub if_exists: bool,
    pub graph_name: Identifier,
}

impl Display for DropPropertyGraphStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP PROPERTY GRAPH ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.graph_name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct RefreshPropertyGraphStmt {
    pub graph_name: Identifier,
}

impl Display for RefreshPropertyGraphStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REFRESH PROPERTY GRAPH {}", self.graph_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(name: &str) -> Identifier {
        Identifier::from_name(None, name)
    }

    #[test]
    fn test_create_property_graph_display() {
        let stmt = CreatePropertyGraphStmt {
            if_not_exists: true,
            graph_name: ident("social_graph"),
            vertex_tables: vec![
                VertexTableDef {
                    table_name: ident("person"),
                    alias: Some(ident("p")),
                    key_columns: Some(vec![ident("id")]),
                    label: Some(ident("Person")),
                    properties: Some(PropertySpec::Columns(vec![
                        PropertyDef {
                            column_name: ident("name"),
                            alias: None,
                        },
                        PropertyDef {
                            column_name: ident("age"),
                            alias: Some(ident("years")),
                        },
                    ])),
                },
                VertexTableDef {
                    table_name: ident("company"),
                    alias: None,
                    key_columns: Some(vec![ident("id")]),
                    label: None,
                    properties: Some(PropertySpec::All),
                },
            ],
            edge_tables: vec![
                EdgeTableDef {
                    table_name: ident("knows"),
                    alias: Some(ident("k")),
                    key_columns: Some(vec![ident("id")]),
                    source: EdgeEndpointDef {
                        key_columns: vec![ident("src_id")],
                        references_table: ident("person"),
                        references_columns: Some(vec![ident("id")]),
                    },
                    destination: EdgeEndpointDef {
                        key_columns: vec![ident("dst_id")],
                        references_table: ident("person"),
                        references_columns: Some(vec![ident("id")]),
                    },
                    label: Some(ident("Knows")),
                    properties: Some(PropertySpec::None),
                },
                EdgeTableDef {
                    table_name: ident("works_at"),
                    alias: None,
                    key_columns: None,
                    source: EdgeEndpointDef {
                        key_columns: vec![ident("person_id")],
                        references_table: ident("person"),
                        references_columns: None,
                    },
                    destination: EdgeEndpointDef {
                        key_columns: vec![ident("company_id")],
                        references_table: ident("company"),
                        references_columns: None,
                    },
                    label: None,
                    properties: Some(PropertySpec::Except(vec![ident("created_at")])),
                },
            ],
        };

        let sql = stmt.to_string();
        assert_eq!(
            sql,
            "CREATE PROPERTY GRAPH IF NOT EXISTS social_graph \
VERTEX TABLES (person AS p KEY (id) LABEL Person PROPERTIES (name, age AS years), company KEY (id) PROPERTIES ALL) \
EDGE TABLES (knows AS k KEY (id) SOURCE KEY (src_id) REFERENCES person (id) DESTINATION KEY (dst_id) REFERENCES person (id) LABEL Knows PROPERTIES NONE, works_at SOURCE KEY (person_id) REFERENCES person DESTINATION KEY (company_id) REFERENCES company PROPERTIES EXCEPT (created_at))"
        );
    }

    #[test]
    fn test_drop_property_graph_display() {
        let stmt = DropPropertyGraphStmt {
            if_exists: true,
            graph_name: ident("social_graph"),
        };
        assert_eq!(
            stmt.to_string(),
            "DROP PROPERTY GRAPH IF EXISTS social_graph"
        );
    }
}

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

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;

/// `GRAPH_TABLE ( graph_name MATCH ... COLUMNS (...) )`
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct GraphTableRef {
    pub graph_name: Identifier,
    pub match_clause: GraphMatchClause,
    pub columns: Vec<GraphColumnDef>,
}

impl Display for GraphTableRef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "GRAPH_TABLE({} ", self.graph_name)?;
        write!(f, "{}", self.match_clause)?;
        write!(f, " COLUMNS (")?;
        write_comma_separated_list(f, &self.columns)?;
        write!(f, "))")
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct GraphMatchClause {
    pub path_variable: Option<Identifier>,
    pub path_mode: Option<PathMode>,
    pub pattern: GraphPattern,
    pub where_clause: Option<Box<Expr>>,
}

impl Display for GraphMatchClause {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MATCH ")?;
        if let Some(var) = &self.path_variable {
            write!(f, "{} = ", var)?;
        }
        if let Some(mode) = &self.path_mode {
            write!(f, "{} ", mode)?;
        }
        write!(f, "{}", self.pattern)?;
        if let Some(where_expr) = &self.where_clause {
            write!(f, " WHERE {}", where_expr)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum PathMode {
    AnyShortest,
    AllShortest,
    Any,
    All,
}

impl Display for PathMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PathMode::AnyShortest => write!(f, "ANY SHORTEST"),
            PathMode::AllShortest => write!(f, "ALL SHORTEST"),
            PathMode::Any => write!(f, "ANY"),
            PathMode::All => write!(f, "ALL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct GraphPattern {
    pub paths: Vec<PathPattern>,
}

impl Display for GraphPattern {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        for (i, path) in self.paths.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{path}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct PathPattern {
    pub elements: Vec<PatternElement>,
}

impl Display for PathPattern {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        for element in &self.elements {
            write!(f, "{element}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum PatternElement {
    Vertex(VertexPattern),
    Edge(EdgePattern),
}

impl Display for PatternElement {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PatternElement::Vertex(vertex) => write!(f, "{vertex}"),
            PatternElement::Edge(edge) => write!(f, "{edge}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct VertexPattern {
    pub variable: Option<Identifier>,
    pub label: Option<Identifier>,
    pub where_clause: Option<Box<Expr>>,
}

impl Display for VertexPattern {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "(")?;
        if let Some(var) = &self.variable {
            write!(f, "{var}")?;
        }
        if let Some(label) = &self.label {
            write!(f, ":{label}")?;
        }
        if let Some(where_expr) = &self.where_clause {
            write!(f, " WHERE {where_expr}")?;
        }
        write!(f, ")")
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct EdgePattern {
    pub variable: Option<Identifier>,
    pub label: Option<Identifier>,
    pub direction: EdgeDirection,
    pub quantifier: Option<PathQuantifier>,
    pub where_clause: Option<Box<Expr>>,
}

impl Display for EdgePattern {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self.direction {
            EdgeDirection::Left | EdgeDirection::LeftRight => write!(f, "<-[")?,
            EdgeDirection::Right | EdgeDirection::Undirected => write!(f, "-[")?,
        }

        if let Some(var) = &self.variable {
            write!(f, "{var}")?;
        }
        if let Some(label) = &self.label {
            write!(f, ":{label}")?;
        }
        if let Some(where_expr) = &self.where_clause {
            write!(f, " WHERE {where_expr}")?;
        }

        match self.direction {
            EdgeDirection::Right => write!(f, "]->")?,
            EdgeDirection::Left | EdgeDirection::Undirected => write!(f, "]-")?,
            EdgeDirection::LeftRight => write!(f, "]->")?,
        }

        if let Some(quantifier) = &self.quantifier {
            write!(f, "{quantifier}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum EdgeDirection {
    Right,
    Left,
    Undirected,
    LeftRight,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum PathQuantifier {
    Plus,
    Star,
    Bounded { lower: u64, upper: Option<u64> },
}

impl Display for PathQuantifier {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            PathQuantifier::Plus => write!(f, "+"),
            PathQuantifier::Star => write!(f, "*"),
            PathQuantifier::Bounded { lower, upper } => match upper {
                Some(upper) => write!(f, "{{{lower},{upper}}}"),
                None => write!(f, "{{{lower},}}"),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct GraphColumnDef {
    pub expr: Expr,
    pub alias: Option<Identifier>,
}

impl Display for GraphColumnDef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.expr)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

use std::collections::HashMap;
use std::sync::Arc;

use crate::binder::Binder;
use crate::expression::Expression;
use crate::operator::ColumnBinding;
use paro_catalog::entry::{EdgeTableInfo, PropertyGraphCatalogEntry, VertexTableInfo};
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{
    EdgeDirection, EdgePattern, Expr, GraphColumnDef, PathQuantifier, VertexPattern,
};

#[derive(Debug, Clone)]
pub struct GraphBindContext {
    pub graph_entry: Arc<PropertyGraphCatalogEntry>,
    vertex_bindings: HashMap<String, BoundVertexVariable>,
    edge_bindings: HashMap<String, BoundEdgeVariable>,
    pattern_chain: Vec<BoundPatternElement>,
    next_anon_id: usize,
}

#[derive(Debug, Clone)]
pub struct BoundVertexVariable {
    pub variable_name: String,
    pub vertex_table_info: VertexTableInfo,
    pub table_index: usize,
    pub column_bindings: Vec<ColumnBinding>,
    pub column_names: Vec<String>,
    pub filter: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct BoundEdgeVariable {
    pub variable_name: String,
    pub edge_table_info: EdgeTableInfo,
    pub table_index: usize,
    pub column_bindings: Vec<ColumnBinding>,
    pub column_names: Vec<String>,
    pub direction: EdgeDirection,
    pub quantifier: Option<PathQuantifier>,
    pub filter: Option<Expression>,
    pub source_variable: String,
    pub destination_variable: String,
}

#[derive(Debug, Clone)]
pub enum BoundPatternElement {
    Vertex(BoundVertexVariable),
    Edge(BoundEdgeVariable),
}

impl GraphBindContext {
    pub fn new(graph_entry: Arc<PropertyGraphCatalogEntry>) -> Self {
        Self {
            graph_entry,
            vertex_bindings: HashMap::new(),
            edge_bindings: HashMap::new(),
            pattern_chain: Vec::new(),
            next_anon_id: 0,
        }
    }

    pub fn bind_vertex(
        &mut self,
        binder: &mut Binder,
        vertex: &VertexPattern,
    ) -> Result<BoundVertexVariable> {
        let variable_name =
            self.next_variable_name(vertex.variable.as_ref().map(|v| v.name.as_str()), "v");
        self.ensure_variable_free(&variable_name)?;

        let vertex_table_info =
            self.resolve_vertex_table(vertex.label.as_ref().map(|l| l.name.as_str()))?;
        let table = self.get_table_entry(binder, &vertex_table_info.table_name)?;
        let table_index = binder.bind_context.generate_table_index();
        let column_names = table
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        let column_types = table
            .columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect::<Vec<_>>();
        let column_bindings = (0..column_names.len())
            .map(|idx| ColumnBinding::new(table_index, idx))
            .collect::<Vec<_>>();

        binder.bind_context.add_binding(
            variable_name.clone(),
            table_index,
            column_names.clone(),
            column_types,
        );

        let filter = vertex
            .where_clause
            .as_ref()
            .map(|e| crate::binder::bind::expr::bind_expression(binder, (**e).clone()))
            .transpose()?;

        let bound = BoundVertexVariable {
            variable_name: variable_name.clone(),
            vertex_table_info,
            table_index,
            column_bindings,
            column_names,
            filter,
        };
        self.vertex_bindings
            .insert(variable_name.clone(), bound.clone());
        self.pattern_chain
            .push(BoundPatternElement::Vertex(bound.clone()));
        Ok(bound)
    }

    pub fn bind_edge(
        &mut self,
        binder: &mut Binder,
        edge: &EdgePattern,
        left_vertex_variable: &str,
        right_vertex_variable: &str,
    ) -> Result<BoundEdgeVariable> {
        self.require_vertex_variable(left_vertex_variable)?;
        self.require_vertex_variable(right_vertex_variable)?;

        let variable_name =
            self.next_variable_name(edge.variable.as_ref().map(|v| v.name.as_str()), "e");
        self.ensure_variable_free(&variable_name)?;
        let edge_table_info =
            self.resolve_edge_table(edge.label.as_ref().map(|l| l.name.as_str()))?;

        let left_vertex = self
            .vertex_bindings
            .get(left_vertex_variable)
            .expect("checked above");
        let right_vertex = self
            .vertex_bindings
            .get(right_vertex_variable)
            .expect("checked above");
        let (source_variable, destination_variable) = self.resolve_edge_endpoints(
            &edge_table_info,
            edge.direction,
            left_vertex,
            right_vertex,
            left_vertex_variable,
            right_vertex_variable,
        )?;

        let table = self.get_table_entry(binder, &edge_table_info.table_name)?;
        let table_index = binder.bind_context.generate_table_index();
        let column_names = table
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        let column_types = table
            .columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect::<Vec<_>>();
        let column_bindings = (0..column_names.len())
            .map(|idx| ColumnBinding::new(table_index, idx))
            .collect::<Vec<_>>();

        binder.bind_context.add_binding(
            variable_name.clone(),
            table_index,
            column_names.clone(),
            column_types,
        );

        let filter = edge
            .where_clause
            .as_ref()
            .map(|e| crate::binder::bind::expr::bind_expression(binder, (**e).clone()))
            .transpose()?;

        let bound = BoundEdgeVariable {
            variable_name: variable_name.clone(),
            edge_table_info,
            table_index,
            column_bindings,
            column_names,
            direction: edge.direction,
            quantifier: edge.quantifier.clone(),
            filter,
            source_variable,
            destination_variable,
        };
        self.edge_bindings
            .insert(variable_name.clone(), bound.clone());
        self.pattern_chain
            .push(BoundPatternElement::Edge(bound.clone()));
        Ok(bound)
    }

    pub fn bind_columns(&self, columns: &[GraphColumnDef]) -> Result<Vec<ColumnBinding>> {
        columns
            .iter()
            .filter(|column| !Self::is_path_function_expr(&column.expr))
            .map(|column| self.resolve_column_expr_binding(&column.expr))
            .collect()
    }

    /// Check if an expression is a path function call (path_length, vertices, edges, element_id).
    /// These are handled separately in bind_graph_table and should be skipped here.
    fn is_path_function_expr(expr: &Expr) -> bool {
        if let Expr::FunctionCall { func, .. } = expr {
            let func_name = func.name.name.to_lowercase();
            matches!(
                func_name.as_str(),
                "path_length" | "vertices" | "edges" | "element_id"
            )
        } else {
            false
        }
    }

    /// Swap the last two elements in the pattern chain.
    ///
    /// Used by `bind_graph_table` to fix ordering: `bind_vertex` must be called
    /// before `bind_edge` (so the variable is registered), but the chain needs
    /// `[..., Edge, Vertex]` order for the decomposer.
    pub fn swap_last_two_elements(&mut self) {
        let len = self.pattern_chain.len();
        if len >= 2 {
            self.pattern_chain.swap(len - 2, len - 1);
        }
    }

    pub fn pattern_chain(&self) -> &[BoundPatternElement] {
        &self.pattern_chain
    }

    pub fn into_pattern_chain(self) -> Vec<BoundPatternElement> {
        self.pattern_chain
    }

    fn resolve_vertex_table(&self, label: Option<&str>) -> Result<VertexTableInfo> {
        if let Some(label) = label {
            return self
                .graph_entry
                .info
                .vertex_tables
                .iter()
                .find(|v| v.label == label)
                .cloned()
                .ok_or_else(|| {
                    paro_error::catalog(format!("Vertex label \"{}\" does not exist", label))
                });
        }

        if self.graph_entry.info.vertex_tables.len() != 1 {
            return Err(paro_error::catalog(
                "Vertex label is required when graph has multiple vertex tables".to_string(),
            ));
        }

        Ok(self.graph_entry.info.vertex_tables[0].clone())
    }

    fn resolve_edge_table(&self, label: Option<&str>) -> Result<EdgeTableInfo> {
        if let Some(label) = label {
            return self
                .graph_entry
                .info
                .edge_tables
                .iter()
                .find(|e| e.label == label)
                .cloned()
                .ok_or_else(|| {
                    paro_error::catalog(format!("Edge label \"{}\" does not exist", label))
                });
        }

        if self.graph_entry.info.edge_tables.len() != 1 {
            return Err(paro_error::catalog(
                "Edge label is required when graph has multiple edge tables".to_string(),
            ));
        }

        Ok(self.graph_entry.info.edge_tables[0].clone())
    }

    fn resolve_edge_endpoints(
        &self,
        edge_info: &EdgeTableInfo,
        direction: EdgeDirection,
        left_vertex: &BoundVertexVariable,
        right_vertex: &BoundVertexVariable,
        left_name: &str,
        right_name: &str,
    ) -> Result<(String, String)> {
        let left_right_match = edge_info.source_vertex_table
            == left_vertex.vertex_table_info.table_name
            && edge_info.destination_vertex_table == right_vertex.vertex_table_info.table_name;
        let right_left_match = edge_info.source_vertex_table
            == right_vertex.vertex_table_info.table_name
            && edge_info.destination_vertex_table == left_vertex.vertex_table_info.table_name;

        match direction {
            EdgeDirection::Right => {
                if !left_right_match {
                    return Err(paro_error::catalog(format!(
                        "Edge label \"{}\" is not compatible with {} -> {}",
                        edge_info.label, left_name, right_name
                    )));
                }
                Ok((left_name.to_string(), right_name.to_string()))
            }
            EdgeDirection::Left => {
                if !right_left_match {
                    return Err(paro_error::catalog(format!(
                        "Edge label \"{}\" is not compatible with {} <- {}",
                        edge_info.label, left_name, right_name
                    )));
                }
                Ok((right_name.to_string(), left_name.to_string()))
            }
            EdgeDirection::Undirected | EdgeDirection::LeftRight => {
                if left_right_match {
                    Ok((left_name.to_string(), right_name.to_string()))
                } else if right_left_match {
                    Ok((right_name.to_string(), left_name.to_string()))
                } else {
                    Err(paro_error::catalog(format!(
                        "Edge label \"{}\" is not compatible with variables \"{}\" and \"{}\"",
                        edge_info.label, left_name, right_name
                    )))
                }
            }
        }
    }

    fn resolve_column_expr_binding(&self, expr: &Expr) -> Result<ColumnBinding> {
        let Expr::ColumnRef { column, .. } = expr else {
            return Err(paro_error::catalog(
                "GRAPH_TABLE COLUMNS currently requires direct variable.property references"
                    .to_string(),
            ));
        };
        let variable = column.table.as_ref().ok_or_else(|| {
            paro_error::catalog("GRAPH_TABLE COLUMNS must use qualified column references")
        })?;
        let column_name = column.column.name();

        if let Some(vertex) = self.vertex_bindings.get(&variable.name) {
            return Self::lookup_binding_by_name(
                vertex.column_names.as_slice(),
                &vertex.column_bindings,
                column_name,
            )
            .ok_or_else(|| {
                paro_error::catalog(format!(
                    "Column \"{}\" does not exist on vertex variable \"{}\"",
                    column_name, variable.name
                ))
            });
        }

        if let Some(edge) = self.edge_bindings.get(&variable.name) {
            return Self::lookup_binding_by_name(
                edge.column_names.as_slice(),
                &edge.column_bindings,
                column_name,
            )
            .ok_or_else(|| {
                paro_error::catalog(format!(
                    "Column \"{}\" does not exist on edge variable \"{}\"",
                    column_name, variable.name
                ))
            });
        }

        Err(paro_error::catalog(format!(
            "Variable \"{}\" is not defined in graph pattern",
            variable.name
        )))
    }

    fn lookup_binding_by_name(
        names: &[String],
        bindings: &[ColumnBinding],
        target: &str,
    ) -> Option<ColumnBinding> {
        names
            .iter()
            .position(|name| name == target)
            .and_then(|idx| bindings.get(idx).copied())
    }

    fn get_table_entry(
        &self,
        binder: &Binder,
        table_name: &str,
    ) -> Result<Arc<paro_catalog::entry::TableCatalogEntry>> {
        let entry = binder.catalog().get_table(
            &binder.catalog_txn_view(),
            &self.graph_entry.info.schema,
            table_name,
        )?;
        match entry.as_ref() {
            paro_catalog::entry::CatalogEntryEnum::Table(table) => Ok(Arc::clone(table)),
            _ => Err(paro_error::wrong_object_type("table", table_name)),
        }
    }

    fn ensure_variable_free(&self, variable_name: &str) -> Result<()> {
        if self.vertex_bindings.contains_key(variable_name)
            || self.edge_bindings.contains_key(variable_name)
        {
            return Err(paro_error::catalog(format!(
                "Duplicate graph variable \"{}\"",
                variable_name
            )));
        }
        Ok(())
    }

    fn require_vertex_variable(&self, variable_name: &str) -> Result<()> {
        if self.vertex_bindings.contains_key(variable_name) {
            Ok(())
        } else {
            Err(paro_error::catalog(format!(
                "Vertex variable \"{}\" is not defined",
                variable_name
            )))
        }
    }

    fn next_variable_name(&mut self, explicit: Option<&str>, prefix: &str) -> String {
        if let Some(name) = explicit {
            return name.to_string();
        }
        let id = self.next_anon_id;
        self.next_anon_id += 1;
        format!("__{}_{}", prefix, id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::entry::CreatePropertyGraphInfo;
    use paro_parser::ast::{ColumnID, ColumnRef, Identifier};

    fn ident(name: &str) -> Identifier {
        Identifier::from_name(None, name)
    }

    fn build_graph_entry() -> Arc<PropertyGraphCatalogEntry> {
        let mut info = CreatePropertyGraphInfo::new(
            "main".to_string(),
            "public".to_string(),
            "social".to_string(),
        );
        info.vertex_tables = vec![
            VertexTableInfo {
                table_name: "person".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                label: "Person".to_string(),
                property_column_ids: vec![1, 2],
            },
            VertexTableInfo {
                table_name: "company".to_string(),
                table_oid: 2,
                key_column_ids: vec![0],
                label: "Company".to_string(),
                property_column_ids: vec![1],
            },
        ];
        info.edge_tables = vec![EdgeTableInfo {
            table_name: "works_at".to_string(),
            table_oid: 10,
            key_column_ids: vec![0],
            source_key_column_ids: vec![1],
            source_vertex_table: "person".to_string(),
            source_ref_column_ids: vec![0],
            destination_key_column_ids: vec![2],
            destination_vertex_table: "company".to_string(),
            destination_ref_column_ids: vec![0],
            label: "WorksAt".to_string(),
            property_column_ids: vec![3],
        }];
        Arc::new(PropertyGraphCatalogEntry::new(info, 0, "main".to_string()))
    }

    #[test]
    fn resolve_vertex_table_by_label() {
        let ctx = GraphBindContext::new(build_graph_entry());
        let v = ctx.resolve_vertex_table(Some("Person")).unwrap();
        assert_eq!(v.table_name, "person");
    }

    #[test]
    fn resolve_vertex_table_missing_label() {
        let ctx = GraphBindContext::new(build_graph_entry());
        let err = ctx
            .resolve_vertex_table(Some("Unknown"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not exist"));
    }

    #[test]
    fn edge_direction_compatibility_check() {
        let ctx = GraphBindContext::new(build_graph_entry());
        let edge = ctx.resolve_edge_table(Some("WorksAt")).unwrap();
        let left = BoundVertexVariable {
            variable_name: "p".to_string(),
            vertex_table_info: VertexTableInfo {
                table_name: "person".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                label: "Person".to_string(),
                property_column_ids: vec![],
            },
            table_index: 0,
            column_bindings: vec![],
            column_names: vec![],
            filter: None,
        };
        let right = BoundVertexVariable {
            variable_name: "c".to_string(),
            vertex_table_info: VertexTableInfo {
                table_name: "company".to_string(),
                table_oid: 2,
                key_column_ids: vec![0],
                label: "Company".to_string(),
                property_column_ids: vec![],
            },
            table_index: 1,
            column_bindings: vec![],
            column_names: vec![],
            filter: None,
        };

        let (src, dst) = ctx
            .resolve_edge_endpoints(&edge, EdgeDirection::Right, &left, &right, "p", "c")
            .unwrap();
        assert_eq!(src, "p");
        assert_eq!(dst, "c");

        let err = ctx
            .resolve_edge_endpoints(&edge, EdgeDirection::Left, &left, &right, "p", "c")
            .unwrap_err()
            .to_string();
        assert!(err.contains("not compatible"));
    }

    #[test]
    fn bind_columns_undefined_variable_and_missing_column() {
        let mut ctx = GraphBindContext::new(build_graph_entry());
        ctx.vertex_bindings.insert(
            "a".to_string(),
            BoundVertexVariable {
                variable_name: "a".to_string(),
                vertex_table_info: VertexTableInfo {
                    table_name: "person".to_string(),
                    table_oid: 1,
                    key_column_ids: vec![0],
                    label: "Person".to_string(),
                    property_column_ids: vec![],
                },
                table_index: 7,
                column_bindings: vec![ColumnBinding::new(7, 0), ColumnBinding::new(7, 1)],
                column_names: vec!["id".to_string(), "name".to_string()],
                filter: None,
            },
        );

        let undef_var = GraphColumnDef {
            expr: Expr::ColumnRef {
                span: None,
                column: ColumnRef {
                    schema: None,
                    table: Some(ident("x")),
                    column: ColumnID::Name(ident("id")),
                },
            },
            alias: None,
        };
        assert!(ctx
            .bind_columns(&[undef_var])
            .unwrap_err()
            .to_string()
            .contains("not defined"));

        let missing_col = GraphColumnDef {
            expr: Expr::ColumnRef {
                span: None,
                column: ColumnRef {
                    schema: None,
                    table: Some(ident("a")),
                    column: ColumnID::Name(ident("age")),
                },
            },
            alias: None,
        };
        assert!(ctx
            .bind_columns(&[missing_col])
            .unwrap_err()
            .to_string()
            .contains("does not exist"));
    }
}

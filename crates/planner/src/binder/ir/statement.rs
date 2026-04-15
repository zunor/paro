//! Binder-owned statement IR.

use paro_common::types::LogicalType;

use super::query::BoundQuery;

pub use crate::binder::bind::statement::alter::BoundAlterEntryInfo;
pub use crate::binder::bind::statement::copy::BoundCopyInfo;
pub use crate::binder::bind::statement::create_index::BoundCreateIndexInfo;
pub use crate::binder::bind::statement::create_property_graph::BoundCreatePropertyGraphInfo;
pub use crate::binder::bind::statement::create_schema::BoundCreateSchemaInfo;
pub use crate::binder::bind::statement::create_sequence::BoundCreateSequenceInfo;
pub use crate::binder::bind::statement::create_table::BoundCreateTableInfo;
pub use crate::binder::bind::statement::create_view::BoundCreateViewInfo;
pub use crate::binder::bind::statement::delete::BoundDeleteInfo;
pub use crate::binder::bind::statement::drop::{BoundDropInfo, DropType};
pub use crate::binder::bind::statement::drop_property_graph::BoundDropPropertyGraphInfo;
pub use crate::binder::bind::statement::explain::BoundExplainInfo;
pub use crate::binder::bind::statement::insert::BoundInsertInfo;
pub use crate::binder::bind::statement::refresh_property_graph::BoundRefreshPropertyGraphInfo;
pub use crate::binder::bind::statement::update::BoundUpdateInfo;

#[derive(Debug)]
pub enum BoundStatementKind {
    CreateTable(BoundCreateTableInfo),
    CreateSequence(BoundCreateSequenceInfo),
    CreateSchema(BoundCreateSchemaInfo),
    CreateIndex(BoundCreateIndexInfo),
    CreatePropertyGraph(BoundCreatePropertyGraphInfo),
    CreateView(BoundCreateViewInfo),
    AlterEntry(BoundAlterEntryInfo),
    Copy(BoundCopyInfo),
    Explain(BoundExplainInfo),
    Query(Box<BoundQuery>),
    Insert(BoundInsertInfo),
    Delete(BoundDeleteInfo),
    Update(BoundUpdateInfo),
    Drop(BoundDropInfo),
    DropPropertyGraph(BoundDropPropertyGraphInfo),
    RefreshPropertyGraph(BoundRefreshPropertyGraphInfo),
    Dummy,
}

impl BoundStatementKind {
    pub fn types(&self) -> Vec<LogicalType> {
        match self {
            BoundStatementKind::Query(node) => node.types(),
            BoundStatementKind::Copy(info) => info.types.clone(),
            BoundStatementKind::Explain(_) => vec![LogicalType::Varchar],
            _ => vec![],
        }
    }

    pub fn names(&self) -> Vec<String> {
        match self {
            BoundStatementKind::Query(node) => node.names(),
            BoundStatementKind::Copy(info) => info.names.clone(),
            BoundStatementKind::Explain(_) => vec!["QUERY PLAN".to_string()],
            _ => vec![],
        }
    }
}

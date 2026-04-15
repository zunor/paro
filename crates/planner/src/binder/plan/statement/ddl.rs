//! Plans DDL statements into logical operators.

use crate::binder::bind::statement::alter::BoundAlterEntryInfo;
use crate::binder::bind::statement::create_index::BoundCreateIndexInfo;
use crate::binder::bind::statement::create_property_graph::BoundCreatePropertyGraphInfo;
use crate::binder::bind::statement::create_schema::BoundCreateSchemaInfo;
use crate::binder::bind::statement::create_sequence::BoundCreateSequenceInfo;
use crate::binder::bind::statement::create_table::BoundCreateTableInfo;
use crate::binder::bind::statement::create_view::BoundCreateViewInfo;
use crate::binder::bind::statement::drop::BoundDropInfo;
use crate::binder::bind::statement::drop_property_graph::BoundDropPropertyGraphInfo;
use crate::binder::bind::statement::refresh_property_graph::BoundRefreshPropertyGraphInfo;
use crate::binder::Binder;
use crate::operator::{
    Alter, CreateIndex, CreatePropertyGraph, CreateSchema, CreateSequence, CreateTable, CreateView,
    Drop, DropPropertyGraph, LogicalOperator, RefreshPropertyGraph,
};
use paro_common::error::Result;

impl Binder {
    pub(crate) fn plan_create_table(
        &mut self,
        info: BoundCreateTableInfo,
    ) -> Result<LogicalOperator> {
        let op = CreateTable::new(info);
        Ok(LogicalOperator::CreateTable(op))
    }

    pub(crate) fn plan_create_sequence(
        &mut self,
        info: BoundCreateSequenceInfo,
    ) -> Result<LogicalOperator> {
        let op = CreateSequence::new(info);
        Ok(LogicalOperator::CreateSequence(op))
    }

    pub(crate) fn plan_create_schema(
        &mut self,
        info: BoundCreateSchemaInfo,
    ) -> Result<LogicalOperator> {
        let op = CreateSchema::new(info);
        Ok(LogicalOperator::CreateSchema(op))
    }

    pub(crate) fn plan_create_index(
        &mut self,
        info: BoundCreateIndexInfo,
    ) -> Result<LogicalOperator> {
        let op = CreateIndex::new(info);
        Ok(LogicalOperator::CreateIndex(op))
    }

    pub(crate) fn plan_create_view(
        &mut self,
        info: BoundCreateViewInfo,
    ) -> Result<LogicalOperator> {
        let op = CreateView::new(info);
        Ok(LogicalOperator::CreateView(op))
    }

    pub(crate) fn plan_drop(&mut self, info: BoundDropInfo) -> Result<LogicalOperator> {
        let op = Drop::new(info);
        Ok(LogicalOperator::Drop(op))
    }

    pub(crate) fn plan_alter_entry(
        &mut self,
        info: BoundAlterEntryInfo,
    ) -> Result<LogicalOperator> {
        let op = Alter::new(info);
        Ok(LogicalOperator::Alter(op))
    }

    pub(crate) fn plan_create_property_graph(
        &mut self,
        info: BoundCreatePropertyGraphInfo,
    ) -> Result<LogicalOperator> {
        let op = CreatePropertyGraph::new(info);
        Ok(LogicalOperator::CreatePropertyGraph(op))
    }

    pub(crate) fn plan_drop_property_graph(
        &mut self,
        info: BoundDropPropertyGraphInfo,
    ) -> Result<LogicalOperator> {
        let op = DropPropertyGraph::new(info);
        Ok(LogicalOperator::DropPropertyGraph(op))
    }

    pub(crate) fn plan_refresh_property_graph(
        &mut self,
        info: BoundRefreshPropertyGraphInfo,
    ) -> Result<LogicalOperator> {
        let op = RefreshPropertyGraph::new(info);
        Ok(LogicalOperator::RefreshPropertyGraph(op))
    }
}

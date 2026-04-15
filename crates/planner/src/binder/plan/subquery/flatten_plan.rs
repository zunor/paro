//! Flatten dependent joins in a logical plan tree.

use crate::binder::plan::subquery::flatten_dependent_join;
use crate::binder::Binder;
use crate::operator::{Join, LogicalOperator};
use crate::plan::LogicalPlan;
use paro_common::error::Result;

pub(crate) fn flatten_dependent_joins_in_plan(
    binder: &mut Binder,
    plan: LogicalPlan,
) -> Result<LogicalPlan> {
    let LogicalPlan {
        id,
        stats,
        operator,
    } = plan;
    let operator = match operator {
        LogicalOperator::DependentJoin(dep) => flatten_dependent_join(binder, dep)?,
        LogicalOperator::Filter(mut filter) => {
            filter.child = Box::new(flatten_dependent_joins_in_plan(binder, *filter.child)?);
            LogicalOperator::Filter(filter)
        }
        LogicalOperator::Projection(mut proj) => {
            proj.child = Box::new(flatten_dependent_joins_in_plan(binder, *proj.child)?);
            LogicalOperator::Projection(proj)
        }
        LogicalOperator::Aggregate(mut agg) => {
            agg.child = Box::new(flatten_dependent_joins_in_plan(binder, *agg.child)?);
            LogicalOperator::Aggregate(agg)
        }
        LogicalOperator::Order(mut order) => {
            order.child = Box::new(flatten_dependent_joins_in_plan(binder, *order.child)?);
            LogicalOperator::Order(order)
        }
        LogicalOperator::Limit(mut limit) => {
            limit.child = Box::new(flatten_dependent_joins_in_plan(binder, *limit.child)?);
            LogicalOperator::Limit(limit)
        }
        LogicalOperator::TopN(mut topn) => {
            topn.child = Box::new(flatten_dependent_joins_in_plan(binder, *topn.child)?);
            LogicalOperator::TopN(topn)
        }
        LogicalOperator::Join(join) => match join {
            Join::Comparison(mut comp) => {
                comp.left = Box::new(flatten_dependent_joins_in_plan(binder, *comp.left)?);
                comp.right = Box::new(flatten_dependent_joins_in_plan(binder, *comp.right)?);
                LogicalOperator::Join(Join::Comparison(comp))
            }
            Join::Any(mut any) => {
                any.left = Box::new(flatten_dependent_joins_in_plan(binder, *any.left)?);
                any.right = Box::new(flatten_dependent_joins_in_plan(binder, *any.right)?);
                LogicalOperator::Join(Join::Any(any))
            }
            Join::Cross(mut cross) => {
                cross.left = Box::new(flatten_dependent_joins_in_plan(binder, *cross.left)?);
                cross.right = Box::new(flatten_dependent_joins_in_plan(binder, *cross.right)?);
                LogicalOperator::Join(Join::Cross(cross))
            }
        },
        LogicalOperator::SetOperation(mut setop) => {
            setop.left = Box::new(flatten_dependent_joins_in_plan(binder, *setop.left)?);
            setop.right = Box::new(flatten_dependent_joins_in_plan(binder, *setop.right)?);
            LogicalOperator::SetOperation(setop)
        }
        LogicalOperator::Distinct(mut distinct) => {
            distinct.child = Box::new(flatten_dependent_joins_in_plan(binder, *distinct.child)?);
            LogicalOperator::Distinct(distinct)
        }
        LogicalOperator::Window(mut window) => {
            window.child = Box::new(flatten_dependent_joins_in_plan(binder, *window.child)?);
            LogicalOperator::Window(window)
        }
        LogicalOperator::Explain(mut explain) => {
            explain.child = Box::new(flatten_dependent_joins_in_plan(binder, *explain.child)?);
            LogicalOperator::Explain(explain)
        }
        LogicalOperator::EmptyResult(mut empty) => {
            empty.child = Box::new(flatten_dependent_joins_in_plan(binder, *empty.child)?);
            LogicalOperator::EmptyResult(empty)
        }
        LogicalOperator::MaterializedCTE(mut cte) => {
            cte.cte_query = Box::new(flatten_dependent_joins_in_plan(binder, *cte.cte_query)?);
            cte.child = Box::new(flatten_dependent_joins_in_plan(binder, *cte.child)?);
            LogicalOperator::MaterializedCTE(cte)
        }
        LogicalOperator::RecursiveCTE(mut cte) => {
            cte.anchor = Box::new(flatten_dependent_joins_in_plan(binder, *cte.anchor)?);
            cte.recursive = Box::new(flatten_dependent_joins_in_plan(binder, *cte.recursive)?);
            LogicalOperator::RecursiveCTE(cte)
        }
        LogicalOperator::Insert(mut insert) => {
            insert.child = Box::new(flatten_dependent_joins_in_plan(binder, *insert.child)?);
            LogicalOperator::Insert(insert)
        }
        LogicalOperator::Delete(mut delete) => {
            delete.child = Box::new(flatten_dependent_joins_in_plan(binder, *delete.child)?);
            LogicalOperator::Delete(delete)
        }
        LogicalOperator::Update(mut update) => {
            update.child = Box::new(flatten_dependent_joins_in_plan(binder, *update.child)?);
            LogicalOperator::Update(update)
        }
        LogicalOperator::CopyTo(mut copy) => {
            copy.child = Box::new(flatten_dependent_joins_in_plan(binder, *copy.child)?);
            LogicalOperator::CopyTo(copy)
        }
        LogicalOperator::GraphExpand(mut ge) => {
            ge.child = Box::new(flatten_dependent_joins_in_plan(binder, *ge.child)?);
            LogicalOperator::GraphExpand(ge)
        }
        other @ (LogicalOperator::Get(_)
        | LogicalOperator::DummyScan
        | LogicalOperator::ExpressionGet(_)
        | LogicalOperator::DelimGet(_)
        | LogicalOperator::Alter(_)
        | LogicalOperator::CreateTable(_)
        | LogicalOperator::CreateSequence(_)
        | LogicalOperator::CreateSchema(_)
        | LogicalOperator::CreateIndex(_)
        | LogicalOperator::CreateView(_)
        | LogicalOperator::CreatePropertyGraph(_)
        | LogicalOperator::DropPropertyGraph(_)
        | LogicalOperator::RefreshPropertyGraph(_)
        | LogicalOperator::Drop(_)
        | LogicalOperator::CTERef(_)
        | LogicalOperator::TableFunctionGet(_)
        | LogicalOperator::SearchScan(_)
        | LogicalOperator::FullTextFilterScan(_)
        | LogicalOperator::GraphMatch(_)
        | LogicalOperator::GraphScan(_)) => other,
    };
    Ok(LogicalPlan {
        id,
        stats,
        operator,
    })
}

pub fn has_dependent_join(op: &LogicalOperator) -> bool {
    match op {
        LogicalOperator::DependentJoin(_) => true,
        _ => op
            .children()
            .iter()
            .any(|c| has_dependent_join(&c.operator)),
    }
}

pub fn flatten_all_dependent_joins(binder: &mut Binder, plan: LogicalPlan) -> Result<LogicalPlan> {
    flatten_dependent_joins_in_plan(binder, plan)
}

// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ownership-independent operator payloads. Mapping child links moves scalar
//! payloads unchanged and never constructs a placeholder relation.

use super::*;

macro_rules! child_links {
    ($name:ident { required [$($required:ident),*] optional [$($optional:ident),*] payload [$($payload:ident),*] }) => {
        impl<Child> $name<Child> {
            pub fn try_map_child_links<Output, E>(
                self,
                map: &mut impl FnMut(Child) -> std::result::Result<Output, E>,
            ) -> std::result::Result<$name<Output>, E> {
                let Self { $($required,)* $($optional,)* $($payload,)* } = self;
                Ok($name {
                    $($required: map($required)?,)*
                    $($optional: $optional.map(&mut *map).transpose()?,)*
                    $($payload,)*
                })
            }

            pub fn visit_child_links<'a>(&'a self, visit: &mut impl FnMut(&'a Child)) {
                $(visit(&self.$required);)*
                $(if let Some(child) = &self.$optional { visit(child); })*
            }

            /// Visit every child link mutably in declaration order.  Keeping
            /// this beside the consuming mapper makes adding a new child a
            /// compile-time change to one contract instead of a hunt through
            /// several hand-written operator matches.
            pub fn visit_child_links_mut<'a>(&'a mut self, visit: &mut impl FnMut(&'a mut Child)) {
                $(visit(&mut self.$required);)*
                $(if let Some(child) = &mut self.$optional { visit(child); })*
            }
        }
    };
}

child_links!(Aggregate {
    required [child]
    optional []
    payload [group_index, aggregate_index, groupings_index, groups, grouping_sets, aggregates, post_reduction, group_stats, group_dependencies, group_input_multiplicity, returned_types, grouping_functions]
});

child_links!(CopyTo {
    required [child]
    optional []
    payload [copy_function, bind_data, file_path, source, options, names, types]
});

child_links!(MaterializedCTE {
    required [cte_query, child]
    optional []
    payload [cte_index, cte_name, column_names, column_types, output_columns, materialized, ref_count]
});

child_links!(RecursiveCTE {
    required [anchor, recursive]
    optional []
    payload [cte_index, cte_name, column_names, column_types, union_all]
});

child_links!(Delete {
    required [child]
    optional []
    payload [table, table_index, return_chunk, is_full_table_delete]
});

child_links!(DependentJoin {
    required [left, right]
    optional []
    payload [correlated_columns, kind]
});

child_links!(Distinct {
    required [child]
    optional []
    payload [distinct_type, distinct_targets, order_by]
});

child_links!(EmptyResult {
    required [child]
    optional []
    payload []
});

child_links!(Explain {
    required [child]
    optional []
    payload [spec, logical_plan_unopt, logical_plan_opt]
});

child_links!(LogicalExternalProject {
    required [child]
    optional []
    payload [project_index, expressions, output_names, returned_types, cost]
});

child_links!(LogicalExternalTable {
    required []
    optional [child]
    payload [table_index, output_columns, returned_types, call_expression, call, lateral, parameterized, cost]
});

child_links!(Filter {
    required [child]
    optional []
    payload [expressions, projection_map]
});

child_links!(GraphExpand {
    required [child]
    optional []
    payload [edge_info, direction, source_label, edge_filter, target_filter, quantifier, path_mode, source_table_index, edge_table_index, target_table_index, output_table_index, target_label, source_table_oid, target_table_oid, target_table_name, has_path_functions]
});

child_links!(Insert {
    required [child]
    optional []
    payload [table, column_index_map, expected_types, on_conflict]
});

child_links!(ComparisonJoin {
    required [left, right]
    optional []
    payload [join_type, anti_join_mode, conditions, mark_index, mark_semantics, duplicate_eliminated_columns, delim_flipped, build_side_constraint, left_projection_map, right_projection_map]
});

child_links!(AnyJoin {
    required [left, right]
    optional []
    payload [join_type, condition, mark_index, build_side_constraint, left_projection_map, right_projection_map]
});

child_links!(CrossProduct {
    required [left, right]
    optional []
    payload [build_side_constraint]
});

child_links!(Limit {
    required [child]
    optional []
    payload [limit, offset, hnsw_options]
});

child_links!(Order {
    required [child]
    optional []
    payload [orders, projection_map]
});

child_links!(Projection {
    required [child]
    optional []
    payload [table_index, expressions, visible_names, visible_count, visible_qualifier, returned_types]
});

child_links!(RowFetch {
    required [child]
    optional []
    payload [carrier_table_index, sources]
});

child_links!(SetOperation {
    required [left, right]
    optional []
    payload [table_index, column_count, setop_type, setop_all, allow_out_of_order, types]
});

child_links!(TopN {
    required [child]
    optional []
    payload [orders, limit, offset, hnsw_options, projection_map]
});

child_links!(Update {
    required [child]
    optional []
    payload [table, table_index, return_chunk, columns, expressions]
});

child_links!(Window {
    required [child]
    optional []
    payload [window_index, expressions]
});

impl<Child> Join<Child> {
    pub fn try_map_child_links<Output, E>(
        self,
        map: &mut impl FnMut(Child) -> std::result::Result<Output, E>,
    ) -> std::result::Result<Join<Output>, E> {
        Ok(match self {
            Self::Comparison(join) => Join::Comparison(join.try_map_child_links(map)?),
            Self::Any(join) => Join::Any(Box::new(join.try_map_child_links(map)?)),
            Self::Cross(join) => Join::Cross(join.try_map_child_links(map)?),
        })
    }

    pub fn visit_child_links<'a>(&'a self, visit: &mut impl FnMut(&'a Child)) {
        match self {
            Self::Comparison(join) => join.visit_child_links(visit),
            Self::Any(join) => join.visit_child_links(visit),
            Self::Cross(join) => join.visit_child_links(visit),
        }
    }

    pub fn visit_child_links_mut<'a>(&'a mut self, visit: &mut impl FnMut(&'a mut Child)) {
        match self {
            Self::Comparison(join) => join.visit_child_links_mut(visit),
            Self::Any(join) => join.visit_child_links_mut(visit),
            Self::Cross(join) => join.visit_child_links_mut(visit),
        }
    }
}

impl<Child> LogicalOperator<Child> {
    pub fn try_map_child_links<Output, E>(
        self,
        map: &mut impl FnMut(Child) -> std::result::Result<Output, E>,
    ) -> std::result::Result<LogicalOperator<Output>, E> {
        Ok(match self {
            Self::Aggregate(operator) => {
                LogicalOperator::Aggregate(Box::new((*operator).try_map_child_links(map)?))
            }
            Self::CopyTo(operator) => {
                LogicalOperator::CopyTo(Box::new((*operator).try_map_child_links(map)?))
            }
            Self::MaterializedCTE(operator) => {
                LogicalOperator::MaterializedCTE(operator.try_map_child_links(map)?)
            }
            Self::RecursiveCTE(operator) => {
                LogicalOperator::RecursiveCTE(operator.try_map_child_links(map)?)
            }
            Self::Delete(operator) => LogicalOperator::Delete(operator.try_map_child_links(map)?),
            Self::DependentJoin(operator) => {
                LogicalOperator::DependentJoin(Box::new((*operator).try_map_child_links(map)?))
            }
            Self::Distinct(operator) => {
                LogicalOperator::Distinct(operator.try_map_child_links(map)?)
            }
            Self::EmptyResult(operator) => {
                LogicalOperator::EmptyResult(operator.try_map_child_links(map)?)
            }
            Self::Explain(operator) => LogicalOperator::Explain(operator.try_map_child_links(map)?),
            Self::ExternalProject(operator) => {
                LogicalOperator::ExternalProject(operator.try_map_child_links(map)?)
            }
            Self::ExternalTable(operator) => {
                LogicalOperator::ExternalTable(Box::new((*operator).try_map_child_links(map)?))
            }
            Self::Filter(operator) => LogicalOperator::Filter(operator.try_map_child_links(map)?),
            Self::GraphExpand(operator) => {
                LogicalOperator::GraphExpand(Box::new((*operator).try_map_child_links(map)?))
            }
            Self::Insert(operator) => LogicalOperator::Insert(operator.try_map_child_links(map)?),
            Self::Limit(operator) => {
                LogicalOperator::Limit(Box::new((*operator).try_map_child_links(map)?))
            }
            Self::Order(operator) => LogicalOperator::Order(operator.try_map_child_links(map)?),
            Self::Projection(operator) => {
                LogicalOperator::Projection(operator.try_map_child_links(map)?)
            }
            Self::RowFetch(operator) => {
                LogicalOperator::RowFetch(operator.try_map_child_links(map)?)
            }
            Self::SetOperation(operator) => {
                LogicalOperator::SetOperation(operator.try_map_child_links(map)?)
            }
            Self::TopN(operator) => LogicalOperator::TopN(operator.try_map_child_links(map)?),
            Self::Update(operator) => LogicalOperator::Update(operator.try_map_child_links(map)?),
            Self::Window(operator) => LogicalOperator::Window(operator.try_map_child_links(map)?),
            Self::Join(operator) => LogicalOperator::Join(operator.try_map_child_links(map)?),
            Self::Get(operator) => LogicalOperator::Get(operator),
            Self::CreateTable(operator) => LogicalOperator::CreateTable(operator),
            Self::CreateRoutine(operator) => LogicalOperator::CreateRoutine(operator),
            Self::Alter(operator) => LogicalOperator::Alter(Box::new(*operator)),
            Self::CreateSequence(operator) => LogicalOperator::CreateSequence(operator),
            Self::CreateSchema(operator) => LogicalOperator::CreateSchema(operator),
            Self::CreateIndex(operator) => LogicalOperator::CreateIndex(operator),
            Self::CreateView(operator) => LogicalOperator::CreateView(Box::new(*operator)),
            Self::Drop(operator) => LogicalOperator::Drop(operator),
            Self::CreatePropertyGraph(operator) => LogicalOperator::CreatePropertyGraph(operator),
            Self::DropPropertyGraph(operator) => LogicalOperator::DropPropertyGraph(operator),
            Self::RefreshPropertyGraph(operator) => LogicalOperator::RefreshPropertyGraph(operator),
            Self::ExpressionGet(operator) => LogicalOperator::ExpressionGet(operator),
            Self::DelimGet(operator) => LogicalOperator::DelimGet(operator),
            Self::CTERef(operator) => LogicalOperator::CTERef(operator),
            Self::TableFunctionGet(operator) => {
                LogicalOperator::TableFunctionGet(Box::new(*operator))
            }
            Self::SearchScan(operator) => LogicalOperator::SearchScan(Box::new(*operator)),
            Self::FullTextFilterScan(operator) => {
                LogicalOperator::FullTextFilterScan(Box::new(*operator))
            }
            Self::GraphMatch(operator) => LogicalOperator::GraphMatch(operator),
            Self::GraphScan(operator) => LogicalOperator::GraphScan(Box::new(*operator)),
            Self::BoundReference(operator) => LogicalOperator::BoundReference(operator),
            Self::DummyScan => LogicalOperator::DummyScan,
        })
    }

    pub fn visit_child_links<'a>(&'a self, visit: &mut impl FnMut(&'a Child)) {
        match self {
            Self::Aggregate(operator) => operator.visit_child_links(visit),
            Self::CopyTo(operator) => operator.visit_child_links(visit),
            Self::MaterializedCTE(operator) => operator.visit_child_links(visit),
            Self::RecursiveCTE(operator) => operator.visit_child_links(visit),
            Self::Delete(operator) => operator.visit_child_links(visit),
            Self::DependentJoin(operator) => operator.visit_child_links(visit),
            Self::Distinct(operator) => operator.visit_child_links(visit),
            Self::EmptyResult(operator) => operator.visit_child_links(visit),
            Self::Explain(operator) => operator.visit_child_links(visit),
            Self::ExternalProject(operator) => operator.visit_child_links(visit),
            Self::ExternalTable(operator) => operator.visit_child_links(visit),
            Self::Filter(operator) => operator.visit_child_links(visit),
            Self::GraphExpand(operator) => operator.visit_child_links(visit),
            Self::Insert(operator) => operator.visit_child_links(visit),
            Self::Limit(operator) => operator.visit_child_links(visit),
            Self::Order(operator) => operator.visit_child_links(visit),
            Self::Projection(operator) => operator.visit_child_links(visit),
            Self::RowFetch(operator) => operator.visit_child_links(visit),
            Self::SetOperation(operator) => operator.visit_child_links(visit),
            Self::TopN(operator) => operator.visit_child_links(visit),
            Self::Update(operator) => operator.visit_child_links(visit),
            Self::Window(operator) => operator.visit_child_links(visit),
            Self::Join(operator) => operator.visit_child_links(visit),
            Self::Get(_)
            | Self::CreateTable(_)
            | Self::CreateRoutine(_)
            | Self::Alter(_)
            | Self::CreateSequence(_)
            | Self::CreateSchema(_)
            | Self::CreateIndex(_)
            | Self::CreateView(_)
            | Self::Drop(_)
            | Self::CreatePropertyGraph(_)
            | Self::DropPropertyGraph(_)
            | Self::RefreshPropertyGraph(_)
            | Self::ExpressionGet(_)
            | Self::DelimGet(_)
            | Self::CTERef(_)
            | Self::TableFunctionGet(_)
            | Self::SearchScan(_)
            | Self::FullTextFilterScan(_)
            | Self::GraphMatch(_)
            | Self::GraphScan(_)
            | Self::BoundReference(_)
            | Self::DummyScan => {}
        }
    }

    pub fn visit_child_links_mut<'a>(&'a mut self, visit: &mut impl FnMut(&'a mut Child)) {
        match self {
            Self::Aggregate(operator) => operator.visit_child_links_mut(visit),
            Self::CopyTo(operator) => operator.visit_child_links_mut(visit),
            Self::MaterializedCTE(operator) => operator.visit_child_links_mut(visit),
            Self::RecursiveCTE(operator) => operator.visit_child_links_mut(visit),
            Self::Delete(operator) => operator.visit_child_links_mut(visit),
            Self::DependentJoin(operator) => operator.visit_child_links_mut(visit),
            Self::Distinct(operator) => operator.visit_child_links_mut(visit),
            Self::EmptyResult(operator) => operator.visit_child_links_mut(visit),
            Self::Explain(operator) => operator.visit_child_links_mut(visit),
            Self::ExternalProject(operator) => operator.visit_child_links_mut(visit),
            Self::ExternalTable(operator) => operator.visit_child_links_mut(visit),
            Self::Filter(operator) => operator.visit_child_links_mut(visit),
            Self::GraphExpand(operator) => operator.visit_child_links_mut(visit),
            Self::Insert(operator) => operator.visit_child_links_mut(visit),
            Self::Limit(operator) => operator.visit_child_links_mut(visit),
            Self::Order(operator) => operator.visit_child_links_mut(visit),
            Self::Projection(operator) => operator.visit_child_links_mut(visit),
            Self::RowFetch(operator) => operator.visit_child_links_mut(visit),
            Self::SetOperation(operator) => operator.visit_child_links_mut(visit),
            Self::TopN(operator) => operator.visit_child_links_mut(visit),
            Self::Update(operator) => operator.visit_child_links_mut(visit),
            Self::Window(operator) => operator.visit_child_links_mut(visit),
            Self::Join(operator) => operator.visit_child_links_mut(visit),
            Self::Get(_)
            | Self::CreateTable(_)
            | Self::CreateRoutine(_)
            | Self::Alter(_)
            | Self::CreateSequence(_)
            | Self::CreateSchema(_)
            | Self::CreateIndex(_)
            | Self::CreateView(_)
            | Self::Drop(_)
            | Self::CreatePropertyGraph(_)
            | Self::DropPropertyGraph(_)
            | Self::RefreshPropertyGraph(_)
            | Self::ExpressionGet(_)
            | Self::DelimGet(_)
            | Self::CTERef(_)
            | Self::TableFunctionGet(_)
            | Self::SearchScan(_)
            | Self::FullTextFilterScan(_)
            | Self::GraphMatch(_)
            | Self::GraphScan(_)
            | Self::BoundReference(_)
            | Self::DummyScan => {}
        }
    }
}

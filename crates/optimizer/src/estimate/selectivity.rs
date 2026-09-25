// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_common::runtime_value::Value;
#[cfg(test)]
use paro_common::types::LogicalType;
use paro_external::routine::identity::BuiltinIntrinsicId;
use paro_planner::expression::{
    ComparisonType, ConjunctionType, Expression, ExpressionIterator, OperatorType,
};
use paro_planner::logical::operator::ColumnBinding;
use paro_planner::logical::plan::CardinalityEstimate;
use paro_storage::statistics::{ColumnStatistics, NumericStats};

const MIN_SELECTIVITY: f64 = 0.000_001;

mod predicate_view;
pub(crate) use predicate_view::ColumnPredicateEvidence;
use predicate_view::{PredicateKind, PredicateView};

#[derive(Debug)]
enum SelectivityStop {
    Incomplete,
    Failed(paro_common::error::ParoError),
}

impl std::fmt::Display for SelectivityStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => f.write_str("selectivity work admission stopped"),
            Self::Failed(error) => write!(f, "selectivity work admission failed: {error}"),
        }
    }
}

type SelectivityResult<T> = std::result::Result<T, SelectivityStop>;

/// Admission precedes visiting or retaining another scalar edge. A stopped
/// analysis has no ranking point: callers must not publish a partial estimate
/// as a negative match or as completed costing evidence.
struct SelectivityWork<F>(F);

impl<F: FnMut() -> paro_common::error::Result<bool>> SelectivityWork<F> {
    fn admit(&mut self) -> SelectivityResult<()> {
        if (self.0)().map_err(SelectivityStop::Failed)? {
            Ok(())
        } else {
            Err(SelectivityStop::Incomplete)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SelectivityEstimate {
    fraction: f64,
    proven: bool,
}

impl SelectivityEstimate {
    fn proven(fraction: f64) -> Self {
        Self {
            fraction: clamp_selectivity(fraction),
            proven: true,
        }
    }

    fn estimated(fraction: f64) -> Self {
        Self {
            fraction: clamp_selectivity(fraction).max(MIN_SELECTIVITY),
            proven: false,
        }
    }

    fn complement(self) -> Self {
        if self.proven {
            Self::proven(1.0 - self.fraction)
        } else {
            Self::estimated(1.0 - self.fraction)
        }
    }
}

fn conjunction_estimate(
    estimates: impl Iterator<Item = SelectivityEstimate>,
) -> SelectivityEstimate {
    let mut estimates = estimates.collect::<Vec<_>>();
    // AND is a commutative operation. Fix the floating-point reduction order
    // so equivalent predicate permutations produce bit-identical estimates.
    estimates.sort_by(|left, right| {
        left.fraction
            .total_cmp(&right.fraction)
            .then_with(|| left.proven.cmp(&right.proven))
    });
    let mut fraction = 1.0;
    let mut proven = true;
    for estimate in estimates {
        if estimate.proven && estimate.fraction == 0.0 {
            return SelectivityEstimate::proven(0.0);
        }
        fraction *= estimate.fraction;
        proven &= estimate.proven;
    }
    if proven {
        SelectivityEstimate::proven(fraction)
    } else {
        SelectivityEstimate::estimated(fraction)
    }
}

/// Combine predicates with exponential backoff only across distinct columns
/// of the same relation.
///
/// Per-column statistics cannot prove independence between category-like
/// attributes. Multiplying all such estimates systematically underestimates
/// filtered relations. Predicates on one column are still combined exactly
/// (and ordered ranges are coalesced before reaching this function), while
/// predicates on different relations remain independent.
fn column_aware_conjunction_estimate(
    estimates: impl Iterator<Item = (SelectivityEstimate, Option<ColumnBinding>)>,
) -> SelectivityEstimate {
    let mut independent = Vec::new();
    let mut by_relation = HashMap::<usize, HashMap<usize, Vec<SelectivityEstimate>>>::new();
    for (estimate, binding) in estimates {
        if estimate.proven && estimate.fraction == 0.0 {
            return SelectivityEstimate::proven(0.0);
        }
        match binding {
            Some(binding) => by_relation
                .entry(binding.table_index)
                .or_default()
                .entry(binding.column_index)
                .or_default()
                .push(estimate),
            None => independent.push(estimate),
        }
    }

    for columns in by_relation.into_values() {
        let mut column_estimates = columns
            .into_values()
            .map(|estimates| conjunction_estimate(estimates.into_iter()))
            .collect::<Vec<_>>();
        column_estimates.sort_by(|left, right| left.fraction.total_cmp(&right.fraction));
        let is_damped = column_estimates.len() > 1;
        let proven = column_estimates.iter().all(|estimate| estimate.proven);
        let mut exponent = 1.0;
        let mut fraction = 1.0;
        for estimate in column_estimates {
            fraction *= estimate.fraction.powf(exponent);
            exponent *= 0.5;
        }
        independent.push(if proven && !is_damped {
            SelectivityEstimate::proven(fraction)
        } else {
            SelectivityEstimate::estimated(fraction)
        });
    }
    conjunction_estimate(independent.into_iter())
}

fn disjunction_estimate(
    estimates: impl Iterator<Item = SelectivityEstimate>,
) -> SelectivityEstimate {
    let mut estimates = estimates.collect::<Vec<_>>();
    if estimates.len() == 1 {
        // OR(p) is p, including its provenance and floating-point point.
        // Complementing twice introduces drift from the scalar-only form.
        return estimates[0];
    }
    // OR is commutative too; canonicalize the miss-probability reduction.
    estimates.sort_by(|left, right| {
        left.fraction
            .total_cmp(&right.fraction)
            .then_with(|| left.proven.cmp(&right.proven))
    });
    let mut miss_fraction = 1.0;
    let mut proven = true;
    for estimate in estimates {
        if estimate.proven && estimate.fraction == 1.0 {
            return SelectivityEstimate::proven(1.0);
        }
        miss_fraction *= 1.0 - estimate.fraction;
        proven &= estimate.proven;
    }
    let fraction = 1.0 - miss_fraction;
    if proven {
        SelectivityEstimate::proven(fraction)
    } else {
        SelectivityEstimate::estimated(fraction)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SelectivityModel {
    pub defaults: SelectivityDefaults,
    #[cfg(test)]
    pub scan_access: paro_storage::rowset::scan_cost::ScanAccessCostModel,
}

#[derive(Debug, Clone)]
pub struct SelectivityDefaults {
    pub equality: f64,
    pub range: f64,
    pub not_equal: f64,
    pub semi_anti_match: f64,
    pub predicate: f64,
    pub like_prefix: f64,
    pub like_contains: f64,
    pub fulltext_match: f64,
    pub vector_topk_fraction: f64,
    pub is_not_null: f64,
}

impl Default for SelectivityDefaults {
    fn default() -> Self {
        Self {
            equality: 0.1,
            range: 0.3,
            not_equal: 0.9,
            semi_anti_match: 0.2,
            predicate: 0.75,
            // Prefix anchoring is materially more selective than an
            // unanchored substring search. This is also the ordering required
            // by the LIKE language lattice: `literal%` is a subset of
            // `%literal%`.
            like_prefix: 0.02,
            like_contains: 0.05,
            fulltext_match: 0.1,
            vector_topk_fraction: 0.001,
            is_not_null: 0.9,
        }
    }
}

struct StatisticsResolver<'a> {
    column_stats: &'a dyn crate::estimate::ColumnStatisticsLookup,
    positional_bindings: Option<&'a [ColumnBinding]>,
}

impl<'a> StatisticsResolver<'a> {
    fn logical(column_stats: &'a dyn crate::estimate::ColumnStatisticsLookup) -> Self {
        Self {
            column_stats,
            positional_bindings: None,
        }
    }

    fn with_positions(
        column_stats: &'a dyn crate::estimate::ColumnStatisticsLookup,
        positional_bindings: &'a [ColumnBinding],
    ) -> Self {
        Self {
            column_stats,
            positional_bindings: Some(positional_bindings),
        }
    }

    fn binding(&self, expression: &Expression) -> Option<ColumnBinding> {
        Some(match expression {
            Expression::ColumnRef(column) if column.depth == 0 => column.binding,
            Expression::Reference(reference) => *self.positional_bindings?.get(reference.index)?,
            _ => return None,
        })
    }

    fn get(&self, expression: &Expression) -> Option<&'a Arc<ColumnStatistics>> {
        self.column_stats.get(&self.binding(expression)?)
    }
}

impl SelectivityModel {
    /// Compare carrying payload through a row-preserving operator path with
    /// carrying one stable rowid and gathering the payload at a later
    /// frontier. The stage count makes blocking/serialized intermediates an
    /// explicit cost input instead of a hidden syntactic heuristic.
    #[cfg(test)]
    pub(crate) fn late_row_fetch_benefit(
        &self,
        carrier_rows: u64,
        fetched_rows: u64,
        payload_types: impl IntoIterator<Item = LogicalType>,
        carrier_stages: usize,
    ) -> Option<f64> {
        if carrier_rows == 0 || carrier_stages == 0 {
            return None;
        }
        let payload_width = payload_types
            .into_iter()
            .map(|ty| self.scan_access.estimated_width(&ty))
            .sum::<usize>();
        if payload_width == 0 {
            return None;
        }
        let rowid_width = self.scan_access.estimated_width(&LogicalType::BigInt);
        let carrier_work = carrier_rows as f64 * carrier_stages as f64;
        let eager = carrier_work * payload_width as f64;
        let late = carrier_work * rowid_width as f64
            + fetched_rows as f64 * payload_width as f64 * self.scan_access.gather_access_penalty()
            + self.scan_access.gather_startup_cost() as f64;
        let benefit = eager - late;
        (benefit > 0.0).then_some(benefit)
    }

    pub fn estimate_selectivity(
        &self,
        expr: &Expression,
        column_stats: &dyn crate::estimate::ColumnStatisticsLookup,
    ) -> f64 {
        let resolver = StatisticsResolver::logical(column_stats);
        self.estimate_selectivity_with_provenance(expr, &resolver)
            .fraction
    }

    fn estimate_selectivity_with_provenance<'a, View: PredicateView<'a>>(
        &self,
        expr: View::Node,
        resolver: &View,
    ) -> SelectivityEstimate {
        self.estimate_selectivity_controlled(expr, resolver, &mut SelectivityWork(|| Ok(true)))
            .expect("unrestricted selectivity analysis cannot be interrupted")
    }

    fn estimate_selectivity_controlled<'a, View: PredicateView<'a>>(
        &self,
        expr: View::Node,
        resolver: &View,
        work: &mut SelectivityWork<impl FnMut() -> paro_common::error::Result<bool>>,
    ) -> SelectivityResult<SelectivityEstimate> {
        work.admit()?;
        if !matches!(
            resolver.kind(expr),
            PredicateKind::And | PredicateKind::Or | PredicateKind::Operator(OperatorType::Not)
        ) {
            return Ok(self.estimate_atomic_selectivity(expr, resolver));
        }
        let estimates = self.estimate_nodes([expr], resolver, work)?;
        Ok(estimates[&resolver.key(expr)])
    }

    fn estimate_nodes<'a, View: PredicateView<'a>>(
        &self,
        roots: impl IntoIterator<Item = View::Node>,
        resolver: &View,
        work: &mut SelectivityWork<impl FnMut() -> paro_common::error::Result<bool>>,
    ) -> SelectivityResult<HashMap<View::Key, SelectivityEstimate>> {
        enum Combine<Node> {
            And(Vec<ConjunctionTerm<Node>>),
            Or(AssociativeTerms<Node>),
            Not(Node),
        }
        enum Task<Node> {
            Enter(Node),
            Finish(Node, Combine<Node>),
        }
        let mut pending = Vec::new();
        for root in roots {
            work.admit()?;
            pending.push(Task::Enter(root));
        }
        let mut estimates = HashMap::new();
        while let Some(task) = pending.pop() {
            work.admit()?;
            match task {
                Task::Enter(node) => {
                    if estimates.contains_key(&resolver.key(node)) {
                        continue;
                    }
                    let combine = match resolver.kind(node) {
                        PredicateKind::And => {
                            Some(Combine::And(conjunction_terms([node], resolver, work)?))
                        }
                        PredicateKind::Or => Some(Combine::Or(flatten_associative(
                            [node],
                            resolver,
                            ConjunctionType::Or,
                            work,
                        )?)),
                        PredicateKind::Operator(OperatorType::Not) => {
                            resolver.operator_child(node, 0).map(Combine::Not)
                        }
                        _ => None,
                    };
                    if let Some(combine) = combine {
                        let start = pending.len();
                        match &combine {
                            Combine::And(terms) => {
                                for term in terms {
                                    work.admit()?;
                                    if let ConjunctionTerm::Input(input, _, _) = term {
                                        pending.push(Task::Enter(*input));
                                    }
                                }
                            }
                            Combine::Or(children) => {
                                pending.extend(children.iter().map(|(node, _)| Task::Enter(*node)))
                            }
                            Combine::Not(child) => pending.push(Task::Enter(*child)),
                        }
                        pending.push(Task::Finish(node, combine));
                        pending[start..].reverse();
                    } else {
                        estimates.insert(
                            resolver.key(node),
                            self.estimate_atomic_selectivity(node, resolver),
                        );
                    }
                }
                Task::Finish(node, combine) => {
                    let estimate = |child| estimates[&resolver.key(child)];
                    let value = match combine {
                        Combine::And(terms) => combine_conjunction_terms(terms, estimate),
                        Combine::Or(children) => {
                            if let Some(value) =
                                disjoint_equality_estimate(&children, resolver, work)?
                            {
                                value
                            } else {
                                disjunction_estimate(children.into_iter().map(
                                    |(child, occurrences)| {
                                        repeat_selectivity(
                                            estimate(child),
                                            occurrences,
                                            ConjunctionType::Or,
                                        )
                                    },
                                ))
                            }
                        }
                        Combine::Not(child) => estimate(child).complement(),
                    };
                    estimates.insert(resolver.key(node), value);
                }
            }
        }
        Ok(estimates)
    }

    fn estimate_atomic_selectivity<'a, View: PredicateView<'a>>(
        &self,
        expr: View::Node,
        resolver: &View,
    ) -> SelectivityEstimate {
        match resolver.kind(expr) {
            PredicateKind::Constant(value) => match value {
                Value::Boolean(true) => SelectivityEstimate::proven(1.0),
                Value::Boolean(false) => SelectivityEstimate::proven(0.0),
                _ => SelectivityEstimate::estimated(self.defaults.predicate),
            },
            PredicateKind::Comparison(comparison, left, right) => SelectivityEstimate::estimated(
                self.estimate_comparison_selectivity(comparison, left, right, resolver),
            ),
            PredicateKind::Operator(operator) => match operator {
                OperatorType::Like => SelectivityEstimate::estimated(
                    match like_pattern_shape(
                        resolver
                            .operator_child(expr, 1)
                            .and_then(|child| resolver.constant(child)),
                    ) {
                        LikePatternShape::MatchAll => 1.0,
                        LikePatternShape::Exact => self.estimate_exact_like_selectivity(
                            resolver.operator_child(expr, 0),
                            resolver,
                        ),
                        LikePatternShape::Wildcard(pattern) => pattern.selectivity(&self.defaults),
                        LikePatternShape::Generic => self.defaults.predicate,
                    },
                ),
                OperatorType::ILike => SelectivityEstimate::estimated(
                    match like_pattern_shape(
                        resolver
                            .operator_child(expr, 1)
                            .and_then(|child| resolver.constant(child)),
                    ) {
                        LikePatternShape::MatchAll => 1.0,
                        // Case folding can merge several stored values into one
                        // comparison domain, so the raw column NDV is not a sound
                        // denominator for an exact ILIKE pattern.
                        LikePatternShape::Exact => self.defaults.equality,
                        LikePatternShape::Wildcard(pattern) => pattern.selectivity(&self.defaults),
                        LikePatternShape::Generic => self.defaults.predicate,
                    },
                ),
                OperatorType::IsNull => {
                    SelectivityEstimate::estimated(1.0 - self.defaults.is_not_null)
                }
                OperatorType::IsNotNull => {
                    SelectivityEstimate::estimated(self.defaults.is_not_null)
                }
                OperatorType::In => {
                    SelectivityEstimate::estimated(self.estimate_in_selectivity(expr, resolver))
                }
                OperatorType::NotIn => SelectivityEstimate::estimated(
                    1.0 - self.estimate_in_selectivity(expr, resolver),
                ),
                _ => SelectivityEstimate::estimated(self.defaults.predicate),
            },
            PredicateKind::Function(intrinsic) => SelectivityEstimate::estimated(match intrinsic {
                Some(
                    BuiltinIntrinsicId::FullTextMatch
                    | BuiltinIntrinsicId::FullTextMatchInternal
                    | BuiltinIntrinsicId::Bm25
                    | BuiltinIntrinsicId::Bm25ScoreInternal
                    | BuiltinIntrinsicId::TsRank
                    | BuiltinIntrinsicId::TsRankCd
                    | BuiltinIntrinsicId::ToTsVector
                    | BuiltinIntrinsicId::PlainToTsQuery
                    | BuiltinIntrinsicId::ToTsQuery
                    | BuiltinIntrinsicId::PhraseToTsQuery
                    | BuiltinIntrinsicId::WebSearchToTsQuery,
                ) => self.defaults.fulltext_match,
                Some(
                    BuiltinIntrinsicId::L2Distance
                    | BuiltinIntrinsicId::L1Distance
                    | BuiltinIntrinsicId::CosineDistance
                    | BuiltinIntrinsicId::NegativeInnerProduct
                    | BuiltinIntrinsicId::SparseDistance,
                ) => self.defaults.vector_topk_fraction,
                _ => self.defaults.predicate,
            }),
            _ => SelectivityEstimate::estimated(self.defaults.predicate),
        }
    }

    pub fn estimate_filter_cardinality(
        &self,
        base_cardinality: u64,
        expressions: &[Expression],
        column_stats: &dyn crate::estimate::ColumnStatisticsLookup,
    ) -> CardinalityEstimate {
        self.estimate_filter_cardinality_with_resolver(
            base_cardinality,
            expressions,
            &StatisticsResolver::logical(column_stats),
        )
    }

    pub fn estimate_filter_cardinality_with_positions(
        &self,
        base_cardinality: u64,
        expressions: &[Expression],
        column_stats: &dyn crate::estimate::ColumnStatisticsLookup,
        positional_bindings: &[ColumnBinding],
    ) -> CardinalityEstimate {
        self.estimate_filter_cardinality_with_resolver(
            base_cardinality,
            expressions,
            &StatisticsResolver::with_positions(column_stats, positional_bindings),
        )
    }

    fn estimate_filter_cardinality_with_resolver(
        &self,
        base_cardinality: u64,
        expressions: &[Expression],
        resolver: &StatisticsResolver<'_>,
    ) -> CardinalityEstimate {
        if base_cardinality == 0 {
            return CardinalityEstimate::exact(0);
        }
        if expressions.is_empty() {
            return CardinalityEstimate::exact(base_cardinality);
        }

        let combined = self.estimate_conjunction(expressions.iter(), resolver);
        self.cardinality_from_selectivity(base_cardinality, combined.fraction, combined.proven)
    }

    pub(crate) fn apply_selectivity_to_cardinality(
        &self,
        base: CardinalityEstimate,
        selectivity: f64,
    ) -> CardinalityEstimate {
        let min = self.cardinality_from_selectivity(base.min, selectivity, false);
        let expected = self.cardinality_from_selectivity(base.expected, selectivity, false);
        let max = self.cardinality_from_selectivity(base.max, selectivity, false);
        CardinalityEstimate {
            min: min.min,
            expected: expected.expected,
            max: max.max.max(expected.expected),
        }
    }

    fn cardinality_from_selectivity(
        &self,
        base_cardinality: u64,
        selectivity: f64,
        proven: bool,
    ) -> CardinalityEstimate {
        if base_cardinality == 0 {
            return CardinalityEstimate::exact(0);
        }
        let selectivity = clamp_selectivity(selectivity);
        let expected = ((base_cardinality as f64) * selectivity).round() as u64;
        let expected = if proven && selectivity == 0.0 {
            0
        } else {
            expected.max(1)
        };
        let min = ((base_cardinality as f64) * (selectivity * 0.5).clamp(0.0, 1.0)).floor() as u64;
        let max = ((base_cardinality as f64) * (selectivity * 1.5).clamp(0.0, 1.0)).ceil() as u64;

        CardinalityEstimate {
            min,
            expected: expected.min(base_cardinality),
            max: max.max(expected).min(base_cardinality),
        }
    }

    fn estimate_comparison_selectivity<'a, View: PredicateView<'a>>(
        &self,
        comparison: ComparisonType,
        left: View::Node,
        right: View::Node,
        resolver: &View,
    ) -> f64 {
        let default = if matches!(
            comparison,
            ComparisonType::LessThan
                | ComparisonType::LessThanOrEqual
                | ComparisonType::GreaterThan
                | ComparisonType::GreaterThanOrEqual
        ) {
            self.defaults.range
        } else {
            self.defaults.equality
        };

        let Some((column, constant, comparison_type)) =
            column_constant_comparison(comparison, left, right, resolver)
        else {
            return default;
        };

        let Some(stats) = resolver.statistics(column) else {
            return default;
        };

        match comparison_type {
            ComparisonType::Equal | ComparisonType::NotDistinctFrom => {
                let distinct = stats.point.unwrap_or(0);
                if distinct == 0 {
                    return default;
                }
                (1.0 / distinct as f64).max(MIN_SELECTIVITY)
            }
            ComparisonType::NotEqual | ComparisonType::DistinctFrom => {
                let distinct = stats.point.unwrap_or(0);
                if distinct == 0 {
                    return default;
                }
                (1.0 - (1.0 / distinct as f64)).clamp(MIN_SELECTIVITY, 1.0)
            }
            ComparisonType::LessThan
            | ComparisonType::LessThanOrEqual
            | ComparisonType::GreaterThan
            | ComparisonType::GreaterThanOrEqual => {
                estimate_range_selectivity(stats, constant, comparison_type)
                    .unwrap_or(self.defaults.range)
            }
        }
    }

    /// Estimate an implicit or explicit AND after coalescing ordered bounds on
    /// the same integral column. Treating `x >= a` and `x < b` as independent
    /// events systematically overestimates bounded intervals; their shared
    /// statistics domain makes the intersection directly measurable.
    fn estimate_conjunction<'a, View: PredicateView<'a>>(
        &self,
        expressions: impl IntoIterator<Item = View::Node>,
        resolver: &View,
    ) -> SelectivityEstimate {
        let mut work = SelectivityWork(|| Ok(true));
        let terms = conjunction_terms(expressions, resolver, &mut work)
            .expect("unrestricted conjunction analysis cannot be interrupted");
        let estimates = self
            .estimate_nodes(
                terms.iter().filter_map(|term| match term {
                    ConjunctionTerm::Input(node, _, _) => Some(*node),
                    ConjunctionTerm::Estimate(..) => None,
                }),
                resolver,
                &mut work,
            )
            .expect("unrestricted conjunction analysis cannot be interrupted");
        combine_conjunction_terms(terms, |node| estimates[&resolver.key(node)])
    }

    fn estimate_in_selectivity<'a, View: PredicateView<'a>>(
        &self,
        expression: View::Node,
        resolver: &View,
    ) -> f64 {
        let Some(column) = resolver.operator_child(expression, 0) else {
            return self.defaults.predicate;
        };

        let probe_count = resolver
            .operator_child_count(expression)
            .saturating_sub(1)
            .max(1) as f64;
        let Some(stats) = resolver.statistics(column) else {
            return clamp_selectivity(self.defaults.equality * probe_count);
        };
        let distinct = stats.point.unwrap_or(0);
        if distinct == 0 {
            return clamp_selectivity(self.defaults.equality * probe_count);
        }
        clamp_selectivity((probe_count / distinct as f64).max(MIN_SELECTIVITY))
    }

    fn estimate_exact_like_selectivity<'a, View: PredicateView<'a>>(
        &self,
        candidate: Option<View::Node>,
        resolver: &View,
    ) -> f64 {
        let Some(candidate) = candidate else {
            return self.defaults.equality;
        };
        let distinct = resolver
            .statistics(candidate)
            .and_then(|stats| stats.point)
            .unwrap_or(0);
        if distinct == 0 {
            self.defaults.equality
        } else {
            (1.0 / distinct as f64).max(MIN_SELECTIVITY)
        }
    }
}

enum ConjunctionTerm<Node> {
    Input(Node, Option<ColumnBinding>, u64),
    Estimate(SelectivityEstimate, Option<ColumnBinding>),
}

/// A finite, non-NULL integral filter domain. This is an estimation input,
/// not permission to erase a predicate or assert a hard row-count bound.
struct FiniteFilterEstimate {
    binding: ColumnBinding,
    domain: IntegralDomain,
    point: u64,
    values: HashSet<u128>,
    first_expression: usize,
}

fn finite_filter_estimate<'a, View: PredicateView<'a>>(
    expression: View::Node,
    resolver: &View,
    work: &mut SelectivityWork<impl FnMut() -> paro_common::error::Result<bool>>,
) -> SelectivityResult<Option<FiniteFilterEstimate>> {
    if !matches!(
        resolver.kind(expression),
        PredicateKind::Or
            | PredicateKind::Comparison(ComparisonType::Equal, _, _)
            | PredicateKind::Operator(OperatorType::In)
    ) {
        return Ok(None);
    }
    let terms = flatten_associative([expression], resolver, ConjunctionType::Or, work)?;
    let mut result: Option<FiniteFilterEstimate> = None;
    for (node, _) in terms {
        work.admit()?;
        if !resolver.can_share(node) {
            return Ok(None);
        }
        let (column, constants) = match resolver.kind(node) {
            PredicateKind::Comparison(ComparisonType::Equal, left, right) => {
                let Some((column, constant, _)) =
                    column_constant_comparison(ComparisonType::Equal, left, right, resolver)
                else {
                    return Ok(None);
                };
                (column, smallvec::smallvec![constant])
            }
            PredicateKind::Operator(OperatorType::In) => {
                let Some(column) = resolver.operator_child(node, 0) else {
                    return Ok(None);
                };
                let mut constants = smallvec::SmallVec::<[&Value; 4]>::new();
                for index in 1..resolver.operator_child_count(node) {
                    work.admit()?;
                    let Some(constant) = resolver
                        .operator_child(node, index)
                        .and_then(|child| resolver.constant(child))
                    else {
                        return Ok(None);
                    };
                    constants.push(constant);
                }
                (column, constants)
            }
            _ => return Ok(None),
        };
        let Some(binding) = resolver.binding(column) else {
            return Ok(None);
        };
        let Some(point) = resolver
            .statistics(column)
            .and_then(|s| s.point)
            .filter(|p| *p > 0)
        else {
            return Ok(None);
        };
        for constant in constants {
            work.admit()?;
            let Some(value) = ordered_integral_value(constant) else {
                return Ok(None);
            };
            let current = result.get_or_insert_with(|| FiniteFilterEstimate {
                binding,
                domain: value.domain,
                point,
                values: HashSet::new(),
                first_expression: 0,
            });
            if current.binding != binding
                || current.domain != value.domain
                || current.point != point
            {
                return Ok(None);
            }
            current.values.insert(value.coordinate);
        }
    }
    Ok(result)
}

/// Distinct equality values on one integral column are disjoint events, not
/// independent Bernoulli trials. In particular, reapplying `year = a OR year
/// = b` to its two-value output domain must not repeatedly multiply rows by
/// 0.75. This is a costing estimate, never a proof that a predicate can be
/// removed: the NDV point can be estimated and NULL coverage can be unknown.
fn disjoint_equality_estimate<'a, View: PredicateView<'a>>(
    terms: &[(View::Node, u64)],
    resolver: &View,
    work: &mut SelectivityWork<impl FnMut() -> paro_common::error::Result<bool>>,
) -> SelectivityResult<Option<SelectivityEstimate>> {
    let mut domain = None;
    let mut values = HashSet::new();
    let mut point = None;
    for &(node, _) in terms {
        work.admit()?;
        if !resolver.can_share(node) {
            return Ok(None);
        }
        let PredicateKind::Comparison(comparison, left, right) = resolver.kind(node) else {
            return Ok(None);
        };
        if !matches!(
            comparison,
            ComparisonType::Equal | ComparisonType::NotDistinctFrom
        ) {
            return Ok(None);
        }
        let Some((column, constant, _)) =
            column_constant_comparison(comparison, left, right, resolver)
        else {
            return Ok(None);
        };
        let Some(binding) = resolver.binding(column) else {
            return Ok(None);
        };
        let Some(value) = ordered_integral_value(constant) else {
            return Ok(None);
        };
        let Some(distinct) = resolver
            .statistics(column)
            .and_then(|stats| stats.point)
            .filter(|n| *n > 0)
        else {
            return Ok(None);
        };
        let current = (binding, value.domain);
        if domain.is_some_and(|previous| previous != current)
            || point.is_some_and(|previous| previous != distinct)
        {
            return Ok(None);
        }
        domain = Some(current);
        point = Some(distinct);
        values.insert(value.coordinate);
    }
    Ok(point.map(|point| SelectivityEstimate::estimated(values.len() as f64 / point as f64)))
}

fn repeat_selectivity(
    value: SelectivityEstimate,
    occurrences: u64,
    kind: ConjunctionType,
) -> SelectivityEstimate {
    if occurrences <= 1 {
        return value;
    }
    let fraction = match kind {
        ConjunctionType::And => value.fraction.powf(occurrences as f64),
        ConjunctionType::Or => 1.0 - (1.0 - value.fraction).powf(occurrences as f64),
    };
    if value.proven {
        SelectivityEstimate::proven(fraction)
    } else {
        SelectivityEstimate::estimated(fraction)
    }
}

fn combine_conjunction_terms<Node>(
    terms: Vec<ConjunctionTerm<Node>>,
    mut estimate: impl FnMut(Node) -> SelectivityEstimate,
) -> SelectivityEstimate {
    column_aware_conjunction_estimate(terms.into_iter().map(|term| match term {
        ConjunctionTerm::Input(node, binding, occurrences) => (
            repeat_selectivity(estimate(node), occurrences, ConjunctionType::And),
            binding,
        ),
        ConjunctionTerm::Estimate(value, binding) => (value, binding),
    }))
}

fn conjunction_terms<'a, View: PredicateView<'a>>(
    expressions: impl IntoIterator<Item = View::Node>,
    resolver: &View,
    work: &mut SelectivityWork<impl FnMut() -> paro_common::error::Result<bool>>,
) -> SelectivityResult<Vec<ConjunctionTerm<View::Node>>> {
    let flattened = flatten_associative(expressions, resolver, ConjunctionType::And, work)?;

    let mut intervals = Vec::<IntegralIntervalEstimate>::new();
    let mut interval_by_binding = HashMap::<ColumnBinding, usize>::new();
    let mut interval_for_expression = vec![None; flattened.len()];
    let mut finite = Vec::<FiniteFilterEstimate>::new();
    let mut finite_by_domain = HashMap::<(ColumnBinding, IntegralDomain, u64), usize>::new();
    let mut finite_for_expression = vec![None; flattened.len()];
    for (expression_idx, (expression, _)) in flattened.iter().copied().enumerate() {
        work.admit()?;
        if let Some(mut constraint) = finite_filter_estimate(expression, resolver, work)? {
            let key = (constraint.binding, constraint.domain, constraint.point);
            let index = finite_by_domain.get(&key).copied();
            let index = if let Some(index) = index {
                finite[index]
                    .values
                    .retain(|value| constraint.values.contains(value));
                index
            } else {
                constraint.first_expression = expression_idx;
                finite.push(constraint);
                let index = finite.len() - 1;
                finite_by_domain.insert(key, index);
                index
            };
            finite_for_expression[expression_idx] = Some(index);
            continue;
        }
        let Some(constraint) = integral_range_constraint(expression, resolver) else {
            continue;
        };
        let interval_idx = match interval_by_binding.get(&constraint.binding).copied() {
            Some(interval_idx) => interval_idx,
            None => {
                let interval_idx = intervals.len();
                intervals.push(IntegralIntervalEstimate::new(expression_idx, &constraint));
                interval_by_binding.insert(constraint.binding, interval_idx);
                interval_idx
            }
        };
        let interval = &mut intervals[interval_idx];
        if interval.domain != constraint.domain
            || interval.minimum != constraint.minimum
            || interval.maximum != constraint.maximum
        {
            continue;
        }
        interval.intersect(constraint.bound, constraint.constant);
        interval_for_expression[expression_idx] = Some(interval_idx);
    }

    let mut terms = Vec::new();
    for (expression_idx, (expression, occurrences)) in flattened.into_iter().enumerate() {
        work.admit()?;
        if let Some(index) = finite_for_expression[expression_idx] {
            let estimate = &finite[index];
            if estimate.first_expression == expression_idx {
                terms.push(ConjunctionTerm::Estimate(
                    SelectivityEstimate::estimated(
                        estimate.values.len() as f64 / estimate.point as f64,
                    ),
                    Some(estimate.binding),
                ));
            }
            continue;
        }
        match interval_for_expression[expression_idx] {
            Some(interval_idx) if intervals[interval_idx].first_expression == expression_idx => {
                terms.push(ConjunctionTerm::Estimate(
                    SelectivityEstimate::estimated(intervals[interval_idx].selectivity()),
                    Some(intervals[interval_idx].binding),
                ));
            }
            Some(_) => {}
            None => terms.push(ConjunctionTerm::Input(
                expression,
                resolver.single_binding(expression),
                occurrences,
            )),
        }
    }
    Ok(terms)
}

fn clamp_selectivity(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        1.0
    }
}

fn column_constant_comparison<'a, View: PredicateView<'a>>(
    comparison: ComparisonType,
    left: View::Node,
    right: View::Node,
    view: &View,
) -> Option<(View::Node, &'a Value, ComparisonType)> {
    if view.binding(left).is_some() {
        if let Some(constant) = view.constant(right) {
            return Some((left, constant, comparison));
        }
    }
    if view.binding(right).is_some() {
        if let Some(constant) = view.constant(left) {
            return Some((right, constant, reverse_comparison(comparison)));
        }
    }
    None
}

fn reverse_comparison(comparison_type: ComparisonType) -> ComparisonType {
    match comparison_type {
        ComparisonType::LessThan => ComparisonType::GreaterThan,
        ComparisonType::LessThanOrEqual => ComparisonType::GreaterThanOrEqual,
        ComparisonType::GreaterThan => ComparisonType::LessThan,
        ComparisonType::GreaterThanOrEqual => ComparisonType::LessThanOrEqual,
        other => other,
    }
}

type AssociativeTerms<Node> = smallvec::SmallVec<[(Node, u64); 8]>;

fn same_boolean_kind<Node>(kind: ConjunctionType, node: PredicateKind<'_, Node>) -> bool {
    matches!(
        (kind, node),
        (ConjunctionType::And, PredicateKind::And) | (ConjunctionType::Or, PredicateKind::Or)
    )
}

/// Flat predicate vectors need no topological order or path-count table.
/// A checked shallow pass proves that case, while nested/shared graphs use
/// the same occurrence algebra through the general DAG path below. Inline
/// storage is an implementation detail, never a search-space cutoff.
fn flatten_associative<'a, View: PredicateView<'a>>(
    expressions: impl IntoIterator<Item = View::Node>,
    view: &View,
    kind: ConjunctionType,
    work: &mut SelectivityWork<impl FnMut() -> paro_common::error::Result<bool>>,
) -> SelectivityResult<AssociativeTerms<View::Node>> {
    let mut shallow = smallvec::SmallVec::<[View::Node; 8]>::new();
    let mut nested = false;
    for expression in expressions {
        work.admit()?;
        if same_boolean_kind(kind, view.kind(expression)) {
            view.try_children(expression, |child| {
                work.admit()?;
                nested |= same_boolean_kind(kind, view.kind(child));
                shallow.push(child);
                Ok(())
            })?;
        } else {
            shallow.push(expression);
        }
    }
    if nested {
        return flatten_associative_dag(shallow, view, kind, work);
    }
    let mut terms = AssociativeTerms::<View::Node>::new();
    let mut positions = None::<HashMap<View::Key, usize>>;
    for node in shallow {
        work.admit()?;
        if positions.is_none() && terms.len() == terms.inline_size() {
            positions = Some(
                terms
                    .iter()
                    .enumerate()
                    .map(|(index, (node, _))| (view.key(*node), index))
                    .collect(),
            );
        }
        let position = if let Some(positions) = &positions {
            positions.get(&view.key(node)).copied()
        } else {
            // At most the inline capacity comparisons. Once spilled, use a
            // key index so wide filters cannot turn this into quadratic work.
            terms
                .iter()
                .position(|(seen, _)| view.key(*seen) == view.key(node))
        };
        if let Some(position) = position {
            if !view.can_share(node) {
                terms[position].1 = terms[position].1.saturating_add(1);
            }
        } else {
            if let Some(positions) = &mut positions {
                positions.insert(view.key(node), terms.len());
            }
            terms.push((node, 1));
        }
    }
    Ok(terms)
}

fn flatten_associative_dag<'a, View: PredicateView<'a>>(
    expressions: impl IntoIterator<Item = View::Node>,
    view: &View,
    kind: ConjunctionType,
    work: &mut SelectivityWork<impl FnMut() -> paro_common::error::Result<bool>>,
) -> SelectivityResult<AssociativeTerms<View::Node>> {
    // First discover a topological order, then propagate occurrence counts.
    // Walking paths would expand a tiny shared boolean DAG exponentially. Counts
    // are relevant only to non-shareable evaluations; deterministic repeated
    // predicates describe one domain. Saturation avoids integer overflow for
    // more occurrences than floating-point costing can distinguish.
    let mut pending = Vec::new();
    let mut counts = HashMap::<View::Key, u64>::new();
    for expression in expressions {
        work.admit()?;
        pending.push((expression, false));
        let count = counts.entry(view.key(expression)).or_default();
        *count = count.saturating_add(1);
    }
    let mut seen = HashSet::new();
    let mut post_order = Vec::new();
    while let Some((current, finish)) = pending.pop() {
        work.admit()?;
        if finish {
            post_order.push(current);
            continue;
        }
        if !seen.insert(view.key(current)) {
            continue;
        }
        pending.push((current, true));
        if same_boolean_kind(kind, view.kind(current)) {
            view.try_children(current, |child| {
                work.admit()?;
                pending.push((child, false));
                Ok(())
            })?;
        }
    }
    let mut output = AssociativeTerms::new();
    for current in post_order.into_iter().rev() {
        work.admit()?;
        let occurrences = counts[&view.key(current)];
        if same_boolean_kind(kind, view.kind(current)) {
            view.try_children(current, |child| {
                work.admit()?;
                let count = counts.entry(view.key(child)).or_default();
                *count = count.saturating_add(occurrences);
                Ok(())
            })?;
            continue;
        }
        output.push((
            current,
            if view.can_share(current) {
                1
            } else {
                occurrences
            },
        ));
    }
    Ok(output)
}

mod domain;
use domain::*;

#[cfg(test)]
mod tests;

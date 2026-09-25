// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct IntegralRangeConstraint {
    pub(super) binding: ColumnBinding,
    pub(super) domain: IntegralDomain,
    pub(super) minimum: u128,
    pub(super) maximum: u128,
    pub(super) bound: IntegralRangeBound,
    pub(super) constant: u128,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum IntegralRangeBound {
    Upper { inclusive: bool },
    Lower { inclusive: bool },
}

pub(super) fn integral_range_constraint<'a, View: PredicateView<'a>>(
    expression: View::Node,
    resolver: &View,
) -> Option<IntegralRangeConstraint> {
    let PredicateKind::Comparison(comparison, left, right) = resolver.kind(expression) else {
        return None;
    };
    if !matches!(
        comparison,
        ComparisonType::LessThan
            | ComparisonType::LessThanOrEqual
            | ComparisonType::GreaterThan
            | ComparisonType::GreaterThanOrEqual
    ) {
        return None;
    }
    let (column, constant, comparison_type) =
        column_constant_comparison(comparison, left, right, resolver)?;
    let binding = resolver.binding(column)?;
    let stats = resolver.statistics(column)?;
    let minimum = ordered_integral_value(&NumericStats::min(stats.values?)?)?;
    let maximum = ordered_integral_value(&NumericStats::max(stats.values?)?)?;
    let constant = ordered_integral_value(constant)?;
    if minimum.domain != maximum.domain
        || minimum.domain != constant.domain
        || minimum.coordinate > maximum.coordinate
    {
        return None;
    }
    let bound = match comparison_type {
        ComparisonType::LessThan => IntegralRangeBound::Upper { inclusive: false },
        ComparisonType::LessThanOrEqual => IntegralRangeBound::Upper { inclusive: true },
        ComparisonType::GreaterThan => IntegralRangeBound::Lower { inclusive: false },
        ComparisonType::GreaterThanOrEqual => IntegralRangeBound::Lower { inclusive: true },
        _ => return None,
    };
    Some(IntegralRangeConstraint {
        binding,
        domain: minimum.domain,
        minimum: minimum.coordinate,
        maximum: maximum.coordinate,
        bound,
        constant: constant.coordinate,
    })
}

#[derive(Debug)]
pub(super) struct IntegralIntervalEstimate {
    pub(super) binding: ColumnBinding,
    pub(super) domain: IntegralDomain,
    pub(super) minimum: u128,
    pub(super) maximum: u128,
    pub(super) lower: u128,
    pub(super) upper: u128,
    pub(super) empty: bool,
    pub(super) first_expression: usize,
}

impl IntegralIntervalEstimate {
    pub(super) fn new(first_expression: usize, constraint: &IntegralRangeConstraint) -> Self {
        Self {
            binding: constraint.binding,
            domain: constraint.domain,
            minimum: constraint.minimum,
            maximum: constraint.maximum,
            lower: constraint.minimum,
            upper: constraint.maximum,
            empty: false,
            first_expression,
        }
    }

    pub(super) fn intersect(&mut self, bound: IntegralRangeBound, constant: u128) {
        if self.empty {
            return;
        }
        match bound {
            IntegralRangeBound::Upper { inclusive: false } => {
                if constant <= self.minimum {
                    self.empty = true;
                } else {
                    self.upper = self.upper.min(constant - 1);
                }
            }
            IntegralRangeBound::Upper { inclusive: true } => {
                if constant < self.minimum {
                    self.empty = true;
                } else {
                    self.upper = self.upper.min(constant);
                }
            }
            IntegralRangeBound::Lower { inclusive: false } => {
                if constant >= self.maximum {
                    self.empty = true;
                } else {
                    self.lower = self.lower.max(constant + 1);
                }
            }
            IntegralRangeBound::Lower { inclusive: true } => {
                if constant > self.maximum {
                    self.empty = true;
                } else {
                    self.lower = self.lower.max(constant);
                }
            }
        }
        self.empty |= self.lower > self.upper;
    }

    pub(super) fn selectivity(&self) -> f64 {
        if self.empty {
            return 0.0;
        }
        let domain = self.maximum.saturating_sub(self.minimum).saturating_add(1) as f64;
        let matching = self.lower.abs_diff(self.upper).saturating_add(1) as f64;
        clamp_selectivity(matching.min(domain) / domain)
    }
}

/// Estimate an ordered comparison from complete-population numeric bounds.
///
/// Integral domains use their exact number of representable values so that
/// inclusive and exclusive predicates differ at the endpoints. Floating-point
/// domains use the conventional continuous uniform approximation.
pub(super) fn estimate_range_selectivity<'a>(
    stats: impl Into<ColumnPredicateEvidence<'a>>,
    constant: &Value,
    comparison_type: ComparisonType,
) -> Option<f64> {
    let stats = stats.into();
    let values = stats.values?;
    if let Some(distribution) = stats.distribution {
        let constant =
            crate::estimate::aggregate_filter::numeric_value(constant, values.get_type())
                .filter(|value| value.is_finite())?;
        return crate::estimate::aggregate_filter::normal_comparison_selectivity(
            distribution.mean(),
            distribution.variance(),
            constant,
            comparison_type,
        );
    }
    let minimum = NumericStats::min(values)?;
    let maximum = NumericStats::max(values)?;

    if let (Some(minimum), Some(maximum), Some(constant)) = (
        ordered_integral_value(&minimum),
        ordered_integral_value(&maximum),
        ordered_integral_value(constant),
    ) {
        if minimum.domain != maximum.domain || minimum.domain != constant.domain {
            return None;
        }
        let (minimum, maximum, constant) =
            (minimum.coordinate, maximum.coordinate, constant.coordinate);
        if minimum > maximum {
            return None;
        }
        let domain = maximum.saturating_sub(minimum).saturating_add(1) as f64;
        let matching = match comparison_type {
            ComparisonType::LessThan if constant <= minimum => 0,
            ComparisonType::LessThan => constant.saturating_sub(minimum),
            ComparisonType::LessThanOrEqual if constant < minimum => 0,
            ComparisonType::LessThanOrEqual => constant.saturating_sub(minimum).saturating_add(1),
            ComparisonType::GreaterThan if constant >= maximum => 0,
            ComparisonType::GreaterThan => maximum.saturating_sub(constant),
            ComparisonType::GreaterThanOrEqual if constant > maximum => 0,
            ComparisonType::GreaterThanOrEqual => {
                maximum.saturating_sub(constant).saturating_add(1)
            }
            _ => return None,
        };
        return Some(clamp_selectivity((matching as f64).min(domain) / domain));
    }

    let minimum = ordered_float_value(&minimum)?;
    let maximum = ordered_float_value(&maximum)?;
    let constant = ordered_float_value(constant)?;
    if !minimum.is_finite() || !maximum.is_finite() || !constant.is_finite() || minimum > maximum {
        return None;
    }
    if minimum == maximum {
        return Some(match comparison_type {
            ComparisonType::LessThan => {
                if minimum < constant {
                    1.0
                } else {
                    0.0
                }
            }
            ComparisonType::LessThanOrEqual => {
                if minimum <= constant {
                    1.0
                } else {
                    0.0
                }
            }
            ComparisonType::GreaterThan => {
                if minimum > constant {
                    1.0
                } else {
                    0.0
                }
            }
            ComparisonType::GreaterThanOrEqual => {
                if minimum >= constant {
                    1.0
                } else {
                    0.0
                }
            }
            _ => return None,
        });
    }
    let below = ((constant - minimum) / (maximum - minimum)).clamp(0.0, 1.0);
    Some(match comparison_type {
        ComparisonType::LessThan | ComparisonType::LessThanOrEqual => below,
        ComparisonType::GreaterThan | ComparisonType::GreaterThanOrEqual => 1.0 - below,
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum IntegralDomain {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    HugeInt,
    UTinyInt,
    USmallInt,
    UInteger,
    UBigInt,
    UHugeInt,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    Decimal { scale: u8 },
}

pub(super) struct OrderedIntegralValue {
    pub(super) domain: IntegralDomain,
    pub(super) coordinate: u128,
}

pub(super) fn ordered_integral_value(value: &Value) -> Option<OrderedIntegralValue> {
    let (domain, coordinate) = match value {
        Value::Boolean(value) => (IntegralDomain::Boolean, u128::from(*value)),
        Value::TinyInt(value) => (
            IntegralDomain::TinyInt,
            u128::from((*value as u8) ^ (1 << 7)),
        ),
        Value::SmallInt(value) => (
            IntegralDomain::SmallInt,
            u128::from((*value as u16) ^ (1 << 15)),
        ),
        Value::Integer(value) => (
            IntegralDomain::Integer,
            u128::from((*value as u32) ^ (1 << 31)),
        ),
        Value::BigInt(value) => (
            IntegralDomain::BigInt,
            u128::from((*value as u64) ^ (1 << 63)),
        ),
        Value::HugeInt(value) => (IntegralDomain::HugeInt, (*value as u128) ^ (1 << 127)),
        Value::UTinyInt(value) => (IntegralDomain::UTinyInt, u128::from(*value)),
        Value::USmallInt(value) => (IntegralDomain::USmallInt, u128::from(*value)),
        Value::UInteger(value) => (IntegralDomain::UInteger, u128::from(*value)),
        Value::UBigInt(value) => (IntegralDomain::UBigInt, u128::from(*value)),
        Value::UHugeInt(value) => (IntegralDomain::UHugeInt, *value),
        Value::Date(value) => (
            IntegralDomain::Date,
            u128::from((*value as u32) ^ (1 << 31)),
        ),
        Value::Timestamp(value) => (
            IntegralDomain::Timestamp,
            u128::from((*value as u64) ^ (1 << 63)),
        ),
        Value::TimestampTz(value) => (
            IntegralDomain::TimestampTz,
            u128::from((*value as u64) ^ (1 << 63)),
        ),
        Value::Time(value) => (
            IntegralDomain::Time,
            u128::from((*value as u64) ^ (1 << 63)),
        ),
        Value::Decimal(value, _, scale) => (
            IntegralDomain::Decimal { scale: *scale },
            (*value as u128) ^ (1 << 127),
        ),
        _ => return None,
    };
    Some(OrderedIntegralValue { domain, coordinate })
}

pub(super) fn ordered_float_value(value: &Value) -> Option<f64> {
    match value {
        Value::Float(value) => Some(*value as f64),
        Value::Double(value) => Some(*value),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LikePatternShape {
    MatchAll,
    Exact,
    Wildcard(WildcardLikePattern),
    Generic,
}

/// Semantic shape of a `%`-only wildcard pattern after consecutive wildcards
/// have been normalized away.
///
/// Wildcard count is deliberately absent: `%%needle%` and `%needle%` have the
/// same language and therefore must receive the same estimate. Anchors and
/// non-empty literal fragments are the properties that strengthen a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WildcardLikePattern {
    pub(super) anchored_start: bool,
    pub(super) anchored_end: bool,
    pub(super) literal_fragments: usize,
}

impl WildcardLikePattern {
    pub(super) fn selectivity(self, defaults: &SelectivityDefaults) -> f64 {
        // Enforce the semantic ordering here as well as in the defaults so a
        // caller cannot install a calibration that makes adding a start
        // anchor increase the estimated result set.
        let start_anchored = defaults.like_prefix.min(defaults.like_contains);
        let base = match (self.anchored_start, self.anchored_end) {
            (true, false) => start_anchored,
            (false, true) | (false, false) => defaults.like_contains,
            // An internally wildcarded pattern anchored at both ends is at
            // least as selective as either one-ended form.
            (true, true) => start_anchored,
        };

        // Ordered fragments overlap on one string value and are consequently
        // more correlated than independent same-column predicates. Give each
        // additional fragment half the previous weight, saturating at the
        // equivalent of two independent occurrences. The finite bound avoids
        // converting an arbitrarily long SQL literal into an integer exponent.
        let remaining_weight = if self.literal_fragments >= 64 {
            0.0
        } else {
            0.5f64.powi(self.literal_fragments as i32)
        };
        let exponent = 2.0 * (1.0 - remaining_weight);
        base.powf(exponent)
    }
}

pub(super) fn like_pattern_shape(pattern: Option<&Value>) -> LikePatternShape {
    let Some(Value::Varchar(value)) = pattern else {
        return LikePatternShape::Generic;
    };
    if value.contains('_') || value.contains('\\') {
        return LikePatternShape::Generic;
    }
    if !value.contains('%') {
        return LikePatternShape::Exact;
    }
    let literal_fragments = value
        .split('%')
        .filter(|fragment| !fragment.is_empty())
        .count();
    if literal_fragments == 0 {
        return LikePatternShape::MatchAll;
    }
    LikePatternShape::Wildcard(WildcardLikePattern {
        anchored_start: !value.starts_with('%'),
        anchored_end: !value.ends_with('%'),
        literal_fragments,
    })
}

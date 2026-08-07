// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn test_numeric_value_union_from_value() {
    // Test integer types
    assert!(matches!(
        NumericValueUnion::from_value(&Value::Integer(42)),
        Some(NumericValueUnion::Integer(42))
    ));
    assert!(matches!(
        NumericValueUnion::from_value(&Value::BigInt(123456789)),
        Some(NumericValueUnion::BigInt(123456789))
    ));
    assert!(matches!(
        NumericValueUnion::from_value(&Value::TinyInt(-10)),
        Some(NumericValueUnion::TinyInt(-10))
    ));

    // Test unsigned types
    assert!(matches!(
        NumericValueUnion::from_value(&Value::UInteger(100)),
        Some(NumericValueUnion::UInteger(100))
    ));

    // Test floating point
    let float_val = NumericValueUnion::from_value(&Value::Float(std::f32::consts::PI));
    assert!(matches!(float_val, Some(NumericValueUnion::Float(_))));

    let double_val = NumericValueUnion::from_value(&Value::Double(std::f64::consts::E));
    assert!(matches!(double_val, Some(NumericValueUnion::Double(_))));

    // Test boolean
    assert!(matches!(
        NumericValueUnion::from_value(&Value::Boolean(true)),
        Some(NumericValueUnion::Boolean(true))
    ));

    // Test non-numeric types return None
    assert!(NumericValueUnion::from_value(&Value::Varchar("hello".to_string())).is_none());
    assert!(NumericValueUnion::from_value(&Value::Null(LogicalType::Integer)).is_none());
}

#[test]
fn test_numeric_value_union_to_value() {
    let union = NumericValueUnion::Integer(42);
    assert_eq!(union.to_value(&LogicalType::Integer), Value::Integer(42));

    let union = NumericValueUnion::BigInt(1000);
    assert_eq!(union.to_value(&LogicalType::BigInt), Value::BigInt(1000));

    assert_eq!(
        NumericValueUnion::Integer(7).to_value(&LogicalType::Date),
        Value::Date(7)
    );
    assert_eq!(
        NumericValueUnion::BigInt(42).to_value(&LogicalType::TimestampTz),
        Value::TimestampTz(42)
    );
}

#[test]
fn test_numeric_value_union_compare() {
    let a = NumericValueUnion::Integer(10);
    let b = NumericValueUnion::Integer(20);
    let c = NumericValueUnion::Integer(10);

    assert_eq!(a.compare(&b), Ordering::Less);
    assert_eq!(b.compare(&a), Ordering::Greater);
    assert_eq!(a.compare(&c), Ordering::Equal);

    // Test floating point comparison
    let f1 = NumericValueUnion::Double(1.5);
    let f2 = NumericValueUnion::Double(2.5);
    assert_eq!(f1.compare(&f2), Ordering::Less);
}

#[test]
fn test_numeric_stats_data_new_unknown() {
    let stats = NumericStatsData::new_unknown();
    assert!(!stats.has_min());
    assert!(!stats.has_max());
    assert!(!stats.has_min_max());
}

#[test]
fn test_numeric_stats_data_new_empty() {
    let stats = NumericStatsData::new_empty();
    assert!(!stats.has_min());
    assert!(!stats.has_max());
    assert!(!stats.has_min_max());
    assert!(stats.minimum().is_none());
    assert!(stats.maximum().is_none());
}

#[test]
fn test_numeric_stats_data_update() {
    let mut stats = NumericStatsData::new_empty();

    // Update with first value
    stats.update(&Value::Integer(50));
    assert_eq!(
        stats.min_value(&LogicalType::Integer),
        Some(Value::Integer(50))
    );
    assert_eq!(
        stats.max_value(&LogicalType::Integer),
        Some(Value::Integer(50))
    );

    // Update with smaller value
    stats.update(&Value::Integer(10));
    assert_eq!(
        stats.min_value(&LogicalType::Integer),
        Some(Value::Integer(10))
    );
    assert_eq!(
        stats.max_value(&LogicalType::Integer),
        Some(Value::Integer(50))
    );

    // Update with larger value
    stats.update(&Value::Integer(100));
    assert_eq!(
        stats.min_value(&LogicalType::Integer),
        Some(Value::Integer(10))
    );
    assert_eq!(
        stats.max_value(&LogicalType::Integer),
        Some(Value::Integer(100))
    );
}

#[test]
fn test_boolean_and_temporal_stats_establish_typed_bounds() {
    let mut boolean = NumericStats::create_empty(LogicalType::Boolean);
    NumericStats::update_bool(&mut boolean, true);
    assert_eq!(NumericStats::min(&boolean), Some(Value::Boolean(true)));
    assert_eq!(NumericStats::max(&boolean), Some(Value::Boolean(true)));

    let mut date = NumericStats::create_empty(LogicalType::Date);
    NumericStats::update(&mut date, &Value::Date(10));
    NumericStats::update(&mut date, &Value::Date(-5));
    assert_eq!(NumericStats::min(&date), Some(Value::Date(-5)));
    assert_eq!(NumericStats::max(&date), Some(Value::Date(10)));

    let mut timestamp = NumericStats::create_empty(LogicalType::Timestamp);
    NumericStats::update(&mut timestamp, &Value::Timestamp(100));
    assert_eq!(
        NumericStats::guaranteed_bounds(&timestamp),
        Some((Value::Timestamp(100), Value::Timestamp(100)))
    );
}

#[test]
fn guaranteed_bound_setters_normalize_lossless_integral_casts() {
    let mut stats = NumericStats::create_unknown(LogicalType::BigInt);
    NumericStats::set_guaranteed_min(&mut stats, &Value::TinyInt(-7));
    NumericStats::set_guaranteed_max(&mut stats, &Value::UInteger(42));

    assert_eq!(
        NumericStats::guaranteed_bounds(&stats),
        Some((Value::BigInt(-7), Value::BigInt(42)))
    );

    let bytes = stats.to_bytes().expect("statistics should serialize");
    let restored = BaseStatistics::from_bytes(&bytes, LogicalType::BigInt)
        .expect("normalized statistics should deserialize");
    assert_eq!(
        NumericStats::guaranteed_bounds(&restored),
        Some((Value::BigInt(-7), Value::BigInt(42)))
    );
}

#[test]
fn guaranteed_bound_setters_discard_incompatible_physical_values() {
    let mut stats = NumericStats::create_unknown(LogicalType::Integer);
    NumericStats::set_guaranteed_min(&mut stats, &Value::Varchar("7".to_string()));
    NumericStats::set_guaranteed_max(&mut stats, &Value::UHugeInt(u128::MAX));

    assert_eq!(NumericStats::guaranteed_bounds(&stats), None);
    assert_eq!(NumericStats::min(&stats), None);
    assert_eq!(NumericStats::max(&stats), None);
}

#[test]
fn test_numeric_stats_data_merge() {
    let mut stats1 = NumericStatsData::new_empty();
    stats1.update(&Value::Integer(10));
    stats1.update(&Value::Integer(50));

    let mut stats2 = NumericStatsData::new_empty();
    stats2.update(&Value::Integer(5));
    stats2.update(&Value::Integer(30));

    stats1.merge(&stats2);

    // After merge, min should be 5 (from stats2), max should be 50 (from stats1)
    assert_eq!(
        stats1.min_value(&LogicalType::Integer),
        Some(Value::Integer(5))
    );
    assert_eq!(
        stats1.max_value(&LogicalType::Integer),
        Some(Value::Integer(50))
    );
}

#[test]
fn test_numeric_stats_data_is_constant() {
    let mut stats = NumericStatsData::new_empty();

    // Not constant when no values
    assert!(!stats.is_constant());

    // Constant when only one value
    stats.update(&Value::Integer(42));
    assert!(stats.is_constant());

    // Not constant when different values
    stats.update(&Value::Integer(43));
    assert!(!stats.is_constant());
}

#[test]
fn test_numeric_stats_data_set_min_max() {
    let mut stats = NumericStatsData::new_unknown();

    stats.set_guaranteed_min(NumericValueUnion::Integer(10));
    assert!(stats.has_min());
    assert!(!stats.has_max());
    assert_eq!(
        stats.min_value(&LogicalType::Integer),
        Some(Value::Integer(10))
    );

    stats.set_guaranteed_max(NumericValueUnion::Integer(100));
    assert!(stats.has_min());
    assert!(stats.has_max());
    assert_eq!(
        stats.max_value(&LogicalType::Integer),
        Some(Value::Integer(100))
    );
}

#[test]
fn test_numeric_stats_data_floating_point() {
    let mut stats = NumericStatsData::new_empty();

    stats.update(&Value::Double(1.5));
    stats.update(&Value::Double(std::f64::consts::PI));
    stats.update(&Value::Double(-2.5));

    let min = stats.min_value(&LogicalType::Double);
    let max = stats.max_value(&LogicalType::Double);

    assert!(matches!(min, Some(Value::Double(v)) if (v - (-2.5)).abs() < f64::EPSILON));
    assert!(matches!(
        max,
        Some(Value::Double(v)) if (v - std::f64::consts::PI).abs() < f64::EPSILON
    ));
}

// ========== NumericStats tests ==========

#[test]
fn test_numeric_stats_create_unknown() {
    let stats = NumericStats::create_unknown(LogicalType::Integer);
    assert!(stats.can_have_null());
    assert!(stats.can_have_no_null());
    assert!(!NumericStats::has_min(&stats));
    assert!(!NumericStats::has_max(&stats));
    assert!(!NumericStats::has_min_max(&stats));
}

#[test]
fn test_numeric_stats_create_empty() {
    let stats = NumericStats::create_empty(LogicalType::Integer);
    assert!(!stats.can_have_null());
    assert!(!stats.can_have_no_null());
    assert!(!NumericStats::has_min(&stats));
    assert!(!NumericStats::has_max(&stats));
    assert!(!NumericStats::has_min_max(&stats));
}

#[test]
fn test_numeric_stats_set_min_max() {
    let mut stats = NumericStats::create_unknown(LogicalType::Integer);

    NumericStats::set_guaranteed_min(&mut stats, &Value::Integer(10));
    assert!(NumericStats::has_min(&stats));
    assert_eq!(NumericStats::min(&stats), Some(Value::Integer(10)));

    NumericStats::set_guaranteed_max(&mut stats, &Value::Integer(100));
    assert!(NumericStats::has_max(&stats));
    assert_eq!(NumericStats::max(&stats), Some(Value::Integer(100)));

    assert!(NumericStats::has_min_max(&stats));
}

#[test]
fn test_numeric_stats_update() {
    let mut stats = NumericStats::create_empty(LogicalType::Integer);

    NumericStats::update(&mut stats, &Value::Integer(50));
    assert!(stats.can_have_no_null());
    assert_eq!(NumericStats::min(&stats), Some(Value::Integer(50)));
    assert_eq!(NumericStats::max(&stats), Some(Value::Integer(50)));

    NumericStats::update(&mut stats, &Value::Integer(10));
    assert_eq!(NumericStats::min(&stats), Some(Value::Integer(10)));
    assert_eq!(NumericStats::max(&stats), Some(Value::Integer(50)));

    NumericStats::update(&mut stats, &Value::Integer(100));
    assert_eq!(NumericStats::min(&stats), Some(Value::Integer(10)));
    assert_eq!(NumericStats::max(&stats), Some(Value::Integer(100)));
}

#[test]
fn test_numeric_stats_update_with_null() {
    let mut stats = NumericStats::create_empty(LogicalType::Integer);

    NumericStats::update(&mut stats, &Value::Integer(50));
    assert!(!stats.can_have_null());
    assert!(stats.can_have_no_null());

    NumericStats::update(&mut stats, &Value::Null(LogicalType::Integer));
    assert!(stats.can_have_null());
    assert!(stats.can_have_no_null());

    // Min/max should not change after null update
    assert_eq!(NumericStats::min(&stats), Some(Value::Integer(50)));
    assert_eq!(NumericStats::max(&stats), Some(Value::Integer(50)));
}

#[test]
fn test_numeric_stats_merge() {
    let mut stats1 = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats1, &Value::Integer(10));
    NumericStats::update(&mut stats1, &Value::Integer(50));

    let mut stats2 = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats2, &Value::Integer(5));
    NumericStats::update(&mut stats2, &Value::Integer(30));
    NumericStats::update(&mut stats2, &Value::Null(LogicalType::Integer));

    stats1.merge(&stats2);

    // After merge, min should be 5 (from stats2), max should be 50 (from stats1)
    assert_eq!(NumericStats::min(&stats1), Some(Value::Integer(5)));
    assert_eq!(NumericStats::max(&stats1), Some(Value::Integer(50)));
    // stats2 had null, so merged stats should have null
    assert!(stats1.can_have_null());
}

#[test]
fn test_numeric_stats_merge_propagates_unknown_bounds() {
    let mut known = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update_i32(&mut known, 10);
    NumericStats::update_i32(&mut known, 20);
    let unknown = NumericStats::create_unknown(LogicalType::Integer);

    let mut known_then_unknown = known.copy();
    known_then_unknown.merge(&unknown);
    assert_eq!(NumericStats::guaranteed_bounds(&known_then_unknown), None);

    let mut unknown_then_known = unknown;
    unknown_then_known.merge(&known);
    assert_eq!(NumericStats::guaranteed_bounds(&unknown_then_known), None);
}

#[test]
fn test_numeric_stats_is_constant() {
    let mut stats = NumericStats::create_empty(LogicalType::Integer);

    NumericStats::update(&mut stats, &Value::Integer(42));
    assert!(NumericStats::is_constant(&stats));

    NumericStats::update(&mut stats, &Value::Integer(43));
    assert!(!NumericStats::is_constant(&stats));
}

#[test]
fn test_numeric_stats_typed_update() {
    let mut stats = NumericStats::create_empty(LogicalType::Integer);

    NumericStats::update_i32(&mut stats, 50);
    assert_eq!(NumericStats::get_min_i32(&stats), Some(50));
    assert_eq!(NumericStats::get_max_i32(&stats), Some(50));

    NumericStats::update_i32(&mut stats, 10);
    assert_eq!(NumericStats::get_min_i32(&stats), Some(10));
    assert_eq!(NumericStats::get_max_i32(&stats), Some(50));

    NumericStats::update_i32(&mut stats, 100);
    assert_eq!(NumericStats::get_min_i32(&stats), Some(10));
    assert_eq!(NumericStats::get_max_i32(&stats), Some(100));
}

#[test]
fn test_numeric_stats_typed_update_i64() {
    let mut stats = NumericStats::create_empty(LogicalType::BigInt);

    NumericStats::update_i64(&mut stats, 1000000000000i64);
    assert_eq!(NumericStats::get_min_i64(&stats), Some(1000000000000i64));
    assert_eq!(NumericStats::get_max_i64(&stats), Some(1000000000000i64));

    NumericStats::update_i64(&mut stats, -1000000000000i64);
    assert_eq!(NumericStats::get_min_i64(&stats), Some(-1000000000000i64));
    assert_eq!(NumericStats::get_max_i64(&stats), Some(1000000000000i64));
}

#[test]
fn test_numeric_stats_typed_update_f64() {
    let mut stats = NumericStats::create_empty(LogicalType::Double);

    NumericStats::update_f64(&mut stats, std::f64::consts::PI);
    let min = NumericStats::get_min_f64(&stats);
    let max = NumericStats::get_max_f64(&stats);
    assert!(min.is_some());
    assert!((min.unwrap() - std::f64::consts::PI).abs() < f64::EPSILON);
    assert!(max.is_some());
    assert!((max.unwrap() - std::f64::consts::PI).abs() < f64::EPSILON);

    NumericStats::update_f64(&mut stats, -2.5);
    let min = NumericStats::get_min_f64(&stats);
    assert!(min.is_some());
    assert!((min.unwrap() - (-2.5)).abs() < f64::EPSILON);
}

#[test]
fn test_numeric_stats_to_string() {
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    let s = NumericStats::to_string(&stats);
    assert!(s.contains("Min:"));
    assert!(s.contains("Max:"));
    assert!(s.contains("10"));
    assert!(s.contains("100"));
}

#[test]
fn test_numeric_stats_min_or_null() {
    let stats = NumericStats::create_unknown(LogicalType::Integer);
    let min = NumericStats::min_or_null(&stats);
    assert!(min.is_null());

    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(42));
    let min = NumericStats::min_or_null(&stats);
    assert_eq!(min, Value::Integer(42));
}

#[test]
fn test_numeric_stats_get_data() {
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update_i32(&mut stats, 1);
    let data = NumericStats::get_data(&stats);
    assert!(data.is_some());
    assert!(data.unwrap().has_min());
    assert!(data.unwrap().has_max());

    // String stats should return None
    let string_stats = BaseStatistics::create_empty(LogicalType::Varchar);
    let data = NumericStats::get_data(&string_stats);
    assert!(data.is_none());
}

// ========== CheckZonemap tests ==========

#[test]
fn test_check_zonemap_no_min_max() {
    let stats = NumericStats::create_unknown(LogicalType::Integer);
    let result =
        NumericStats::check_zonemap(&stats, ExpressionType::CompareEqual, &[Value::Integer(50)]);
    assert_eq!(result, FilterPropagateResult::NoPruningPossible);
}

#[test]
fn test_check_zonemap_equal_in_range() {
    // Segment: min=10, max=100
    // Filter: x = 50 (in range)
    // Result: NoPruningPossible
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    let result =
        NumericStats::check_zonemap(&stats, ExpressionType::CompareEqual, &[Value::Integer(50)]);
    assert_eq!(result, FilterPropagateResult::NoPruningPossible);
}

#[test]
fn test_check_zonemap_equal_out_of_range() {
    // Segment: min=10, max=100
    // Filter: x = 200 (out of range)
    // Result: FilterAlwaysFalse (can prune)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    let result =
        NumericStats::check_zonemap(&stats, ExpressionType::CompareEqual, &[Value::Integer(200)]);
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
}

#[test]
fn test_check_zonemap_equal_constant_segment() {
    // Segment: min=50, max=50 (constant)
    // Filter: x = 50
    // Result: FilterAlwaysTrue
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(50));

    let result =
        NumericStats::check_zonemap(&stats, ExpressionType::CompareEqual, &[Value::Integer(50)]);
    assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
}

#[test]
fn test_check_zonemap_not_equal_out_of_range() {
    // Segment: min=10, max=100
    // Filter: x != 200 (out of range)
    // Result: FilterAlwaysTrue (all values satisfy)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareNotEqual,
        &[Value::Integer(200)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
}

#[test]
fn test_check_zonemap_not_equal_constant_segment() {
    // Segment: min=50, max=50 (constant)
    // Filter: x != 50
    // Result: FilterAlwaysFalse (no values satisfy)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(50));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareNotEqual,
        &[Value::Integer(50)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
}

#[test]
fn test_check_zonemap_greater_than_always_true() {
    // Segment: min=100, max=200
    // Filter: x > 50
    // Result: FilterAlwaysTrue (min > 50)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(100));
    NumericStats::update(&mut stats, &Value::Integer(200));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareGreaterThan,
        &[Value::Integer(50)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
}

#[test]
fn test_check_zonemap_greater_than_always_false() {
    // Segment: min=10, max=100
    // Filter: x > 200
    // Result: FilterAlwaysFalse (max <= 200)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareGreaterThan,
        &[Value::Integer(200)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
}

#[test]
fn test_check_zonemap_greater_than_no_pruning() {
    // Segment: min=10, max=100
    // Filter: x > 50
    // Result: NoPruningPossible (some values may satisfy)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareGreaterThan,
        &[Value::Integer(50)],
    );
    assert_eq!(result, FilterPropagateResult::NoPruningPossible);
}

#[test]
fn test_check_zonemap_less_than_always_true() {
    // Segment: min=10, max=50
    // Filter: x < 100
    // Result: FilterAlwaysTrue (max < 100)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(50));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareLessThan,
        &[Value::Integer(100)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
}

#[test]
fn test_check_zonemap_less_than_always_false() {
    // Segment: min=100, max=200
    // Filter: x < 50
    // Result: FilterAlwaysFalse (min >= 50)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(100));
    NumericStats::update(&mut stats, &Value::Integer(200));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareLessThan,
        &[Value::Integer(50)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
}

#[test]
fn test_check_zonemap_greater_than_or_equal() {
    // Segment: min=100, max=200
    // Filter: x >= 100
    // Result: FilterAlwaysTrue (min >= 100)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(100));
    NumericStats::update(&mut stats, &Value::Integer(200));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareGreaterThanOrEqualTo,
        &[Value::Integer(100)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
}

#[test]
fn test_check_zonemap_less_than_or_equal() {
    // Segment: min=10, max=100
    // Filter: x <= 100
    // Result: FilterAlwaysTrue (max <= 100)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareLessThanOrEqualTo,
        &[Value::Integer(100)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
}

#[test]
fn test_check_zonemap_multiple_constants_in() {
    // Segment: min=10, max=100
    // Filter: x IN (5, 50, 200)
    // Result: NoPruningPossible (50 is in range)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareEqual,
        &[Value::Integer(5), Value::Integer(50), Value::Integer(200)],
    );
    assert_eq!(result, FilterPropagateResult::NoPruningPossible);
}

#[test]
fn test_check_zonemap_multiple_constants_all_out() {
    // Segment: min=10, max=100
    // Filter: x IN (5, 200, 300)
    // Result: FilterAlwaysFalse (all out of range)
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareEqual,
        &[Value::Integer(5), Value::Integer(200), Value::Integer(300)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
}

#[test]
fn test_check_zonemap_floating_point() {
    // Segment: min=1.5, max=10.5
    // Filter: x > 5.0
    // Result: NoPruningPossible
    let mut stats = NumericStats::create_empty(LogicalType::Double);
    NumericStats::update(&mut stats, &Value::Double(1.5));
    NumericStats::update(&mut stats, &Value::Double(10.5));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareGreaterThan,
        &[Value::Double(5.0)],
    );
    assert_eq!(result, FilterPropagateResult::NoPruningPossible);

    // Filter: x > 20.0
    // Result: FilterAlwaysFalse
    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareGreaterThan,
        &[Value::Double(20.0)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
}

#[test]
fn test_check_zonemap_bigint() {
    // Segment: min=1000000000000, max=2000000000000
    let mut stats = NumericStats::create_empty(LogicalType::BigInt);
    NumericStats::update(&mut stats, &Value::BigInt(1000000000000i64));
    NumericStats::update(&mut stats, &Value::BigInt(2000000000000i64));

    // Filter: x < 500000000000
    // Result: FilterAlwaysFalse
    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareLessThan,
        &[Value::BigInt(500000000000i64)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);

    // Filter: x > 500000000000
    // Result: FilterAlwaysTrue
    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareGreaterThan,
        &[Value::BigInt(500000000000i64)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
}

#[test]
fn test_check_zonemap_boundary_cases() {
    // Segment: min=10, max=100
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(10));
    NumericStats::update(&mut stats, &Value::Integer(100));

    // x > 100 should be FilterAlwaysFalse (max is 100, not > 100)
    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareGreaterThan,
        &[Value::Integer(100)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);

    // x >= 100 should be NoPruningPossible (max == 100)
    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareGreaterThanOrEqualTo,
        &[Value::Integer(100)],
    );
    assert_eq!(result, FilterPropagateResult::NoPruningPossible);

    // x < 10 should be FilterAlwaysFalse (min is 10, not < 10)
    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareLessThan,
        &[Value::Integer(10)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);

    // x <= 10 should be NoPruningPossible (min == 10)
    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareLessThanOrEqualTo,
        &[Value::Integer(10)],
    );
    assert_eq!(result, FilterPropagateResult::NoPruningPossible);
}

#[test]
fn test_check_zonemap_not_distinct_from() {
    // IS NOT DISTINCT FROM behaves like = for non-null values
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(50));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareNotDistinctFrom,
        &[Value::Integer(50)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysTrue);
}

#[test]
fn test_check_zonemap_distinct_from() {
    // IS DISTINCT FROM behaves like != for non-null values
    let mut stats = NumericStats::create_empty(LogicalType::Integer);
    NumericStats::update(&mut stats, &Value::Integer(50));

    let result = NumericStats::check_zonemap(
        &stats,
        ExpressionType::CompareDistinctFrom,
        &[Value::Integer(50)],
    );
    assert_eq!(result, FilterPropagateResult::FilterAlwaysFalse);
}

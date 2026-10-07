// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cost::source::WorkSourceId;
use paro_planner::physical::{ResourceGrantClassId, SpillPolicy};

fn model() -> ScanAccessCostModel {
    ScanAccessCostModel::default()
}

fn work(units: f64) -> PhysicalCost {
    divisible_work_cost(CompactRange::point(units).unwrap()).unwrap()
}

/// A scan of `rows` rows reading `width` stored bytes, whose stream is one
/// unit per row.
fn scan_lane(id: usize, rows: f64, width: u64) -> SourceLane {
    let scan = ScanAccess {
        rows: CompactRange::point(rows).unwrap(),
        width,
        static_stage: None,
    };
    SourceLane {
        source: WorkSourceId(id),
        decode: scan.decode(&[]).unwrap(),
        scan,
        filters: Vec::new(),
        stream: work(rows),
        apply: PhysicalCost::ZERO,
        share: 1.0,
        at_scan: true,
    }
}

/// A child whose cost is exactly its scans: cursor, eager decode and stream.
fn response_with(lanes: Vec<SourceLane>) -> PhysicalResponse {
    let mut cost = PhysicalCost::ZERO;
    for lane in &lanes {
        cost = cost
            .sequential(base_table_scan_cost(lane.scan.rows, lane.scan.width).unwrap())
            .unwrap()
            .sequential(lane.stream)
            .unwrap();
    }
    PhysicalResponse {
        cost,
        hard_rows: None,
        result: requirements::ResultGuarantee::Exact,
        lanes: lanes.into_boxed_slice(),
    }
}

fn filter(column: usize, retained: f64) -> LaneFilter {
    LaneFilter {
        column,
        retained,
        key_width: 4,
    }
}

/// The single filter key, held by source column 0.
fn key(domain: RuntimeFilterProbeKeyDomain) -> RuntimeFilterSourceKey {
    RuntimeFilterSourceKey {
        key: 0,
        column: 0,
        domain,
    }
}

fn join_facts() -> ResolvedPlannerCostFacts {
    let plan =
        OwnedLogicalPlan::synthetic(paro_planner::logical::operator::LogicalOperator::DummyScan);
    let template = planner_cost_facts(&plan, &HashMap::new(), Default::default()).unwrap();
    let mut facts = resolve_cost_facts(
        &template,
        CompactRange::point(10.0).unwrap(),
        Box::new([
            CompactRange::point(100.0).unwrap(),
            CompactRange::point(10.0).unwrap(),
        ]),
        Some(1000),
        Box::new([Some(100), Some(10)]),
    )
    .unwrap();
    facts.runtime_filter_key_types = Box::new([paro_common::types::LogicalType::Integer]);
    facts.runtime_filter_build_key_distinct = Box::new([Some(10)]);
    facts.runtime_filter_probe_sources = Box::new([ResolvedRuntimeFilterSource {
        source: WorkSourceId(7),
        keys: Box::new([key(RuntimeFilterProbeKeyDomain::DeclaredUnique)]),
        rows: CompactRange::point(100.0).unwrap(),
    }]);
    facts
}

fn context<'a>(
    facts: &'a ResolvedPlannerCostFacts,
    calibration: &'a MachineCalibrationBundle,
) -> LaneContext<'a> {
    LaneContext {
        facts,
        calibration,
        tasks: 4,
    }
}

#[test]
fn a_filter_reaches_only_its_traced_source() {
    let calibration = MachineCalibrationBundle::builtin_production();
    let mut facts = join_facts();
    let lane = scan_lane(7, 100.0, 32);
    let flavor = PhysicalImplementationFlavor::HashJoinRuntimeFilter;
    let find = |facts: &ResolvedPlannerCostFacts, flavor| {
        lane_filters(&lane, facts, flavor, &context(facts, &calibration)).unwrap()
    };
    let (found, flat_apply) = find(&facts, flavor).unwrap();
    assert_eq!(found, vec![filter(0, 0.1)]);
    // The owning join charges evaluation on every source row.
    assert_eq!(
        flat_apply,
        runtime_filter_apply_cost(CompactRange::point(100.0).unwrap(), &calibration).unwrap()
    );
    assert!(find(&facts, PhysicalImplementationFlavor::HashJoin).is_none());
    facts.runtime_filter_probe_sources[0].source = WorkSourceId(8);
    assert!(find(&facts, flavor).is_none());
    // Without a key-domain estimate a row count proves nothing.
    facts.runtime_filter_probe_sources[0].source = WorkSourceId(7);
    facts.runtime_filter_probe_sources[0].keys =
        Box::new([key(RuntimeFilterProbeKeyDomain::Unknown)]);
    assert_eq!(find(&facts, flavor).unwrap().0[0].retained, 1.0);
}

#[test]
fn a_composite_filter_adds_one_condition_per_key() {
    let calibration = MachineCalibrationBundle::builtin_production();
    let mut facts = join_facts();
    facts.runtime_filter_key_types = Box::new([
        paro_common::types::LogicalType::Integer,
        paro_common::types::LogicalType::BigInt,
    ]);
    facts.runtime_filter_build_key_distinct = Box::new([Some(5), None]);
    facts.runtime_filter_probe_sources[0].keys = Box::new([
        RuntimeFilterSourceKey {
            key: 1,
            column: 2,
            domain: RuntimeFilterProbeKeyDomain::EstimatedDistinct { keys: 50 },
        },
        RuntimeFilterSourceKey {
            key: 0,
            column: 4,
            domain: RuntimeFilterProbeKeyDomain::EstimatedDistinct { keys: 20 },
        },
    ]);
    let lane = scan_lane(7, 100.0, 32);
    let (filters, flat_apply) = lane_filters(
        &lane,
        &facts,
        PhysicalImplementationFlavor::HashJoinRuntimeFilter,
        &context(&facts, &calibration),
    )
    .unwrap()
    .unwrap();
    // The second key's domain is the ten build rows; each key keeps its own
    // share of its own column, at its own width.
    assert_eq!(
        filters,
        vec![
            LaneFilter {
                column: 2,
                retained: 0.2,
                key_width: 8,
            },
            LaneFilter {
                column: 4,
                retained: 0.25,
                key_width: 4,
            },
        ]
    );
    assert_eq!(
        flat_apply,
        runtime_filter_apply_cost(CompactRange::point(200.0).unwrap(), &calibration).unwrap()
    );
    let filtered = lane.with_filters(filters, &calibration).unwrap();
    assert!((filtered.retained() - 0.05).abs() < 1e-12);
}

#[test]
fn filters_on_one_key_nest_and_filters_on_different_keys_compose() {
    let calibration = MachineCalibrationBundle::builtin_production();
    let lane = scan_lane(7, 1_000_000.0, 64);
    let date = lane.with_filters([filter(0, 0.1)], &calibration).unwrap();
    // A weaker filter on the same key column removes nothing but is still
    // evaluated on the survivors of the stronger one.
    let weaker = date.with_filters([filter(0, 0.5)], &calibration).unwrap();
    assert_eq!(weaker.retained(), date.retained());
    assert_eq!(weaker.stream, date.stream);
    assert_eq!(weaker.decode, date.decode);
    let survivors = runtime_filter_apply_cost(
        CompactRange::new(100_000.0, 100_000.0, 1_000_000.0).unwrap(),
        &calibration,
    )
    .unwrap();
    assert!(
        (weaker.apply.score.range.expected
            - date.apply.score.range.expected
            - survivors.score.range.expected)
            .abs()
            < 1e-6
    );
    let stronger = date.with_filters([filter(0, 0.01)], &calibration).unwrap();
    assert!((stronger.retained() - 0.01).abs() < 1e-12);
    // A filter on another key column removes rows independently.
    let customer = date.with_filters([filter(1, 0.2)], &calibration).unwrap();
    assert!((customer.retained() - 0.02).abs() < 1e-12);
    assert!(
        (customer.stream.score.range.expected - 1_000_000.0 * 0.02).abs() < 1.0,
        "{}",
        customer.stream.score.range.expected
    );
}

#[test]
fn retention_keeps_cursor_key_decode_and_memory() {
    let calibration = MachineCalibrationBundle::builtin_production();
    let lane = scan_lane(7, 1_000_000.0, 64);
    let mut total = response_with(vec![lane.clone()]).cost;
    total.minimum_memory_bytes = 20;
    total.peak_memory_upper = 100;
    for retained in [0.1, 1.0e-5] {
        let filtered = lane
            .with_filters([filter(0, retained)], &calibration)
            .unwrap();
        let flat_apply = filtered.apply;
        let changed = lane
            .revise(total.sequential(flat_apply).unwrap(), &filtered, flat_apply)
            .unwrap();
        let cursor = 1_000_000.0;
        let key = 1_000_000.0 * 4.0 / 32.0;
        let deferred = 1_000_000.0 * 60.0 / 32.0 * model().deferred_decode_fraction(retained);
        let stream = 1_000_000.0 * retained;
        let expected = cursor + key + deferred + stream + flat_apply.score.range.expected;
        assert!(
            (changed.score.range.expected - expected).abs() < 1.0,
            "{retained}: {} != {expected}",
            changed.score.range.expected
        );
        // A filter can keep every row: the risk is never below eager.
        assert!(changed.score.range.upper >= total.score.range.upper);
        assert_eq!(changed.minimum_memory_bytes, 20);
        assert_eq!(changed.peak_memory_upper, 100);
    }
}

#[test]
fn key_only_scans_have_no_deferred_decode_to_retain() {
    let calibration = MachineCalibrationBundle::builtin_production();
    let lane = scan_lane(7, 1_000.0, 4);
    let filtered = lane.with_filters([filter(0, 0.1)], &calibration).unwrap();
    assert_eq!(filtered.decode, lane.decode);
    assert!(filtered.stream.score.range.expected < lane.stream.score.range.expected);
}

/// A dimension scan of 73,049 rows whose 4-byte static predicate keeps 356,
/// propagated through that filter; and the filter's evaluation work.
fn filtered_dimension() -> (Box<[SourceLane]>, PhysicalCost, PhysicalCost) {
    let mut facts = join_facts();
    facts.child_rows = Box::new([CompactRange::point(73_049.0).unwrap()]);
    facts.output_rows = CompactRange::point(356.0).unwrap();
    facts.predicate_width = Some(4);
    let calibration = MachineCalibrationBundle::builtin_production();
    let evaluation = filter_evaluation_cost(&facts).unwrap().unwrap();
    let dimension = response_with(vec![scan_lane(14, 73_049.0, 24)]);
    let local = work(356.0).sequential(evaluation).unwrap();
    let cost = dimension.cost.sequential(local).unwrap();
    let (lanes, cost) = propagate_lanes(
        StreamShape::Filter,
        PhysicalImplementationFlavor::Structural,
        local,
        cost,
        &[&dimension],
        None,
        context(&facts, &calibration),
    )
    .unwrap();
    (lanes, cost, evaluation)
}

#[test]
fn a_filter_above_a_scan_becomes_its_static_stage() {
    let (lanes, _, evaluation) = filtered_dimension();
    let lane = &lanes[0];
    let selectivity = 356.0 / 73_049.0;
    // Output vectors of rejected rows are a static cost, not retainable;
    // the filter's own survivor work streams on.
    assert!((lane.stream.score.range.expected - (73_049.0 * selectivity + 356.0)).abs() < 1.0);
    assert_eq!(lane.static_charge().unwrap(), evaluation.work_only());
    assert!(!lane.at_scan);
}

#[test]
fn static_stages_price_the_scan_decode_as_staged_access() {
    let (lanes, cost, _) = filtered_dimension();
    let eager = 73_049.0 * 24.0 / 32.0;
    let staged = 73_049.0
        * model().staged_decode_bytes(
            24,
            &[ScanPredicateStage {
                selectivity: 356.0 / 73_049.0,
                width: 4,
                runtime: false,
            }],
        )
        / 32.0;
    assert!((lanes[0].decode.score.range.expected - staged).abs() < 1e-6);
    let unstaged = response_with(vec![scan_lane(14, 73_049.0, 24)])
        .cost
        .score
        .range
        .expected
        + work(356.0).score.range.expected
        + filter_evaluation_cost(&{
            let mut facts = join_facts();
            facts.child_rows = Box::new([CompactRange::point(73_049.0).unwrap()]);
            facts.predicate_width = Some(4);
            facts
        })
        .unwrap()
        .unwrap()
        .score
        .range
        .expected;
    assert!((cost.score.range.expected - (unstaged - eager + staged)).abs() < 1e-3);
}

#[test]
fn weaker_runtime_filters_run_after_the_static_stage() {
    let calibration = MachineCalibrationBundle::builtin_production();
    let (lanes, _, _) = filtered_dimension();
    let lane = &lanes[0];
    let weaker = lane.with_filters([filter(0, 0.5)], &calibration).unwrap();
    assert_eq!(
        weaker.static_charge().unwrap(),
        lane.static_charge().unwrap()
    );
    // The filter itself only sees the static survivors.
    let static_survivors = runtime_filter_apply_cost(
        CompactRange::new(356.0, 356.0, 73_049.0).unwrap(),
        &calibration,
    )
    .unwrap();
    assert!(
        (weaker.apply.score.range.expected - static_survivors.score.range.expected).abs() < 1e-6
    );
    // Static survivors are already too dense to skip blocks; halving them
    // barely changes decode.
    let (before, after) = (
        lane.decode.score.range.expected,
        weaker.decode.score.range.expected,
    );
    assert!(
        (after - before).abs() < before * 0.01,
        "{before} -> {after}"
    );
}

#[test]
fn more_selective_runtime_filters_run_before_the_static_stage() {
    let calibration = MachineCalibrationBundle::builtin_production();
    let (lanes, _, evaluation) = filtered_dimension();
    let lane = &lanes[0];
    let first = lane
        .with_filters([filter(0, 1.0e-6)], &calibration)
        .unwrap();
    assert!(first.static_charge().unwrap().score.range.expected < 1.0);
    let total = response_with(vec![scan_lane(14, 73_049.0, 24)])
        .cost
        .sequential(evaluation)
        .unwrap();
    let revised = lane.revise(total, &first, PhysicalCost::ZERO).unwrap();
    // Ordering removes almost all static evaluation, but the RF lookup itself
    // need not be cheaper than that evaluation under every machine calibration.
    assert!(
        total.score.range.expected - revised.score.range.expected
            + first.apply.score.range.expected
            > evaluation.score.range.expected * 0.99
    );
}

#[test]
fn streaming_operators_are_attributed_by_share_and_build_sides_end() {
    let calibration = MachineCalibrationBundle::builtin_production();
    let facts = join_facts();
    let left = response_with(vec![scan_lane(1, 100.0, 32)]);
    let right = response_with(vec![scan_lane(2, 10.0, 32)]);
    let propagate = |stream,
                     implementation,
                     facts: &ResolvedPlannerCostFacts,
                     children: &[&PhysicalResponse]| {
        propagate_lanes(
            stream,
            implementation,
            work(110.0),
            PhysicalCost::ZERO,
            children,
            None,
            context(facts, &calibration),
        )
        .unwrap()
        .0
    };
    let union = propagate(
        StreamShape::Concatenate,
        PhysicalImplementationFlavor::Structural,
        &facts,
        &[&left, &right],
    );
    // Facts carry 100 and 10 child rows: the branches own 100/110 and
    // 10/110 of every later operator in the concatenated stream.
    assert!((union[0].share - 100.0 / 110.0).abs() < 1e-12);
    assert!((union[0].stream.score.range.expected - 200.0).abs() < 1e-3);
    assert!((union[1].stream.score.range.expected - 20.0).abs() < 1e-3);

    let mut join = facts.clone();
    join.child_row_widths = Box::new([16, 8]);
    let joined = propagate(
        StreamShape::Join,
        PhysicalImplementationFlavor::HashJoin,
        &join,
        &[&left, &right],
    );
    let probe = hash_join_probe_stream_cost(
        &join,
        PhysicalImplementationFlavor::HashJoin,
        &calibration,
        4,
    )
    .unwrap()
    .unwrap();
    assert_eq!(joined[0].share, 1.0);
    assert!(
        (joined[0].stream.score.range.expected
            - (left.lanes[0].stream.score.range.expected + probe.score.range.expected))
            .abs()
            < 1e-6
    );
    assert_eq!(joined[1].share, 0.0);
    assert_eq!(joined[1].stream, right.lanes[0].stream);
    let opaque = propagate(
        StreamShape::Opaque,
        PhysicalImplementationFlavor::CrossProductInMemory,
        &facts,
        &[&left],
    );
    assert_eq!(opaque[0].share, 0.0);
    assert!(propagate(
        StreamShape::Closed,
        PhysicalImplementationFlavor::HashAggregate,
        &facts,
        &[&left],
    )
    .is_empty());
}

#[test]
fn runtime_filters_pay_for_themselves_through_intervening_streaming_work() {
    let session = crate::tests::catalog::setup_session();
    let calibration = MachineCalibrationBundle::builtin_production();
    let mut facts = join_facts();
    // A selective filter between the fact scan and this join leaves only
    // 50k of its 1M source rows to probe here.
    facts.child_rows = Box::new([
        CompactRange::point(50_000.0).unwrap(),
        CompactRange::point(100.0).unwrap(),
    ]);
    facts.child_rows_hard_upper = Box::new([None, Some(100)]);
    facts.child_row_widths = Box::new([16, 8]);
    facts.output_rows = CompactRange::point(5_000.0).unwrap();
    facts.runtime_filter_build_key_distinct = Box::new([Some(100)]);
    // 100 build keys contained in 200 probe keys: a weak filter that keeps
    // half of the source.
    facts.runtime_filter_probe_sources = Box::new([ResolvedRuntimeFilterSource {
        source: WorkSourceId(7),
        keys: Box::new([key(RuntimeFilterProbeKeyDomain::EstimatedDistinct {
            keys: 200,
        })]),
        rows: CompactRange::point(1_000_000.0).unwrap(),
    }]);
    let implementations = PlannerImplementationSet {
        baseline: PhysicalImplementationFlavor::HashJoin,
        hash_join_runtime_filter: true,
        ..PlannerImplementationSet::STRUCTURAL
    };
    let model = LocalCostModel {
        operator_type: paro_planner::logical::operator::LogicalOperatorType::ComparisonJoin,
        local_cost: PhysicalCost::ZERO,
        baseline: PhysicalImplementationFlavor::HashJoin,
        resource_sensitive: true,
        spillable: true,
        perfect_hash: None,
        runtime_filter_key_types: &facts.runtime_filter_key_types,
    };
    let grant = ResourceGrantClass {
        id: ResourceGrantClassId(0),
        hard_memory_bytes: 1 << 30,
        spill_policy: SpillPolicy::Allowed,
        max_parallel_tasks: 4,
    };
    let select = |probe: &PhysicalResponse, build: &PhysicalResponse| {
        select_costed(
            &model,
            &facts,
            implementations,
            requirements::ResultGuarantee::Exact,
            StreamShape::Join,
            &[probe, build],
            SelectionEnvironment {
                grant,
                calibration: &calibration,
                session: &session,
            },
        )
        .unwrap()
        .unwrap()
    };
    let build = response_with(vec![scan_lane(9, 100.0, 8)]);
    // Without work between a narrow key-only source and this join,
    // checking 1M source rows to drop 30% of 50k probes is a loss.
    let mut narrow = scan_lane(7, 1_000_000.0, 4);
    narrow.stream = work(50_000.0);
    let direct = select(&response_with(vec![narrow.clone()]), &build);
    assert_eq!(
        direct.implementation,
        PhysicalImplementationFlavor::HashJoin
    );
    // The same stream crossing an expensive filter: only the source-side
    // filter removes that work, so pricing just this join's probe would
    // hide the gain.
    let mut through_filter = narrow;
    let apply =
        runtime_filter_apply_cost(CompactRange::point(1_000_000.0).unwrap(), &calibration).unwrap();
    // Half of this work is removable. Make its saving exceed the complete
    // lookup envelope rather than relying on a particular bootstrap price.
    let expensive_work = 4.0 * apply.score.range.upper.max(apply.work_latency.upper);
    through_filter.attribute(work(expensive_work)).unwrap();
    let chosen = select(&response_with(vec![through_filter.clone()]), &build);
    assert_eq!(
        chosen.implementation,
        PhysicalImplementationFlavor::HashJoinRuntimeFilter
    );
    assert!((chosen.response.lanes[0].retained() - 0.5).abs() < 1e-6);
    assert!(
        chosen.response.lanes[0].stream.score.range.expected
            < through_filter.stream.score.range.expected
    );
}

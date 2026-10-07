// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Source lanes: the divisible work a runtime filter evaluated at one rowset
//! source can remove.
//!
//! A runtime filter runs inside the scan of its traced source, so it removes
//! rows before every streaming operator between that source and the join that
//! owns the filter. A lane carries that work so the owner can price all of it:
//! the scan's staged decode, the static predicate pushed into the scan, and the
//! probe, filter, projection and UNION ALL work attributed by the source's
//! share of each stream. Build sides, cross products and breakers end a lane.
//!
//! The scan evaluates its static predicate and every runtime filter as
//! stages in ascending selectivity, each on the survivors of the earlier
//! ones; decode, static evaluation and filter application are all charged in
//! that order. A runtime filter adds one condition per key column. Conditions
//! on the same column have nested domains, so only the strongest one removes
//! rows; conditions on different columns are combined as independent. The scan's decode is priced by the storage access model,
//! which also owns the executor's late-materialization decision.

use paro_storage::rowset::scan_cost::{ScanAccessCostModel, ScanPredicateStage};

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::cost::source::WorkSourceId;

#[derive(Clone)]
pub(crate) struct SourceLane {
    source: WorkSourceId,
    scan: ScanAccess,
    filters: Vec<LaneFilter>,
    /// Currently charged stored-column decode.
    decode: PhysicalCost,
    /// Currently charged streaming work since the scan's static stage.
    stream: PhysicalCost,
    /// Currently charged evaluation of the runtime filters, staged.
    apply: PhysicalCost,
    /// Fraction of the current output stream that originates at this source.
    /// Zero once the source ends in a build side or a non-streaming operator.
    share: f64,
    /// No operator has consumed the scan output yet.
    at_scan: bool,
}

#[derive(Clone)]
struct ScanAccess {
    rows: CompactRange,
    width: u64,
    static_stage: Option<StaticStage>,
}

/// A filter directly above a scan, which lowering pushes into it.
#[derive(Clone)]
struct StaticStage {
    selectivity: f64,
    width: u64,
    /// Predicate evaluation over every scanned row.
    evaluation: PhysicalCost,
}

/// One key condition of a runtime filter applied at this source.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LaneFilter {
    column: usize,
    retained: f64,
    key_width: u64,
}

impl SourceLane {
    /// The lane of a base-table scan, whose own cost already charges the eager
    /// decode and the returned `stream` (its output vectors).
    pub(super) fn scan(
        facts: &ResolvedPlannerCostFacts,
        calibration: &MachineCalibrationBundle,
    ) -> Result<Option<Self>> {
        let (Some(source), Some(width)) = (facts.scan_work_source, facts.scan_access_width) else {
            return Ok(None);
        };
        let mut work = crate::cost::calibration::LocalOperatorWork::default();
        work.add(
            crate::cost::calibration::OP_TUPLE_BYTE_BLOCK,
            scaled_work(facts.output_rows, facts.output_row_width as f64 / 32.0)?,
        )?;
        let scan = ScanAccess {
            rows: facts.output_rows,
            width,
            static_stage: None,
        };
        Ok(Some(Self {
            source,
            decode: scan.decode(&[])?,
            scan,
            filters: Vec::new(),
            stream: calibration.fold(&work)?.work_only(),
            apply: PhysicalCost::ZERO,
            share: 1.0,
            at_scan: true,
        }))
    }

    pub(super) fn stream(&self) -> PhysicalCost {
        self.stream
    }

    /// Fraction of the scanned rows all runtime filters keep.
    fn retained(&self) -> f64 {
        retained(&self.filters)
    }

    /// This lane once `added` also run at the source.
    pub(super) fn with_filters(
        &self,
        added: impl IntoIterator<Item = LaneFilter>,
        calibration: &MachineCalibrationBundle,
    ) -> Result<Self> {
        let mut filters = self.filters.clone();
        filters.extend(added);
        let before = self.retained();
        let relative = if before > 0.0 {
            retained(&filters) / before
        } else {
            0.0
        };
        Ok(Self {
            decode: self.scan.decode(&filters)?,
            stream: self.stream.retain_work(ppm(relative), 1_000_000)?,
            apply: self.scan.apply(&filters, calibration)?,
            filters,
            ..self.clone()
        })
    }

    /// A filter directly above the scan becomes its static stage: it keeps
    /// `selectivity` of the rows before any runtime filter acts, so only that
    /// part of the scan's stream remains retainable, and its `evaluation`
    /// over every scanned row becomes retainable by more selective filters.
    fn with_static_stage(
        &self,
        selectivity: f64,
        width: u64,
        evaluation: PhysicalCost,
    ) -> Result<Self> {
        let selectivity = selectivity.clamp(0.0, 1.0);
        let scan = ScanAccess {
            static_stage: Some(StaticStage {
                selectivity,
                width,
                evaluation: evaluation.work_only(),
            }),
            ..self.scan.clone()
        };
        let kept = ppm(selectivity);
        Ok(Self {
            decode: scan.decode(&self.filters)?,
            stream: self.stream.retain_work(kept, kept)?,
            scan,
            ..self.clone()
        })
    }

    /// Static predicate evaluation currently charged: runtime filters more
    /// selective than the static predicate run first and leave it only their
    /// survivors.
    fn static_charge(&self) -> Result<PhysicalCost> {
        let Some(stage) = &self.scan.static_stage else {
            return Ok(PhysicalCost::ZERO);
        };
        let before = self
            .scan
            .stages(&self.filters)
            .into_iter()
            .find_map(|stage| (stage.filter.is_none()).then_some(stage.surviving))
            .unwrap_or(1.0);
        stage.evaluation.retain_work(ppm(before), 1_000_000)
    }

    /// Replace this lane's charge in `cost` with `revised`. The owning join
    /// charged `flat_apply` for evaluating its new filter on every source
    /// row; the lane charges it where the scan actually evaluates it.
    pub(super) fn revise(
        &self,
        cost: PhysicalCost,
        revised: &Self,
        flat_apply: PhysicalCost,
    ) -> Result<PhysicalCost> {
        cost.replace_work(flat_apply, PhysicalCost::ZERO)?
            .replace_work(self.apply, revised.apply)?
            .replace_work(self.decode, revised.decode)?
            .replace_work(self.stream, revised.stream)?
            .replace_work(self.static_charge()?, revised.static_charge()?)
    }

    /// Charge this source's share of streaming `work` to its lane.
    fn attribute(&mut self, work: PhysicalCost) -> Result<()> {
        if self.share <= 0.0 {
            return Ok(());
        }
        let share = ppm(self.share);
        self.stream = self
            .stream
            .sequential(work.work_only().retain_work(share, share)?)?;
        Ok(())
    }
}

/// One predicate the scan evaluates, in evaluation order.
struct Stage {
    /// Fraction of the rows reaching this stage that it keeps. A condition on
    /// a column an earlier, stronger condition already reduced keeps all.
    selectivity: f64,
    width: u64,
    /// Fraction of the scanned rows reaching this stage.
    surviving: f64,
    /// The runtime filter evaluated here; `None` is the static stage.
    filter: Option<usize>,
}

/// Fraction of rows `filters` keep: the strongest condition per column,
/// different columns independent.
fn retained(filters: &[LaneFilter]) -> f64 {
    let mut strongest = BTreeMap::<usize, f64>::new();
    for filter in filters {
        let known = strongest.entry(filter.column).or_insert(1.0);
        *known = known.min(filter.retained);
    }
    strongest.values().product()
}

impl ScanAccess {
    /// The scan's predicates in the executor's order: ascending selectivity.
    fn stages(&self, filters: &[LaneFilter]) -> Vec<Stage> {
        let mut order = (0..filters.len()).collect::<Vec<_>>();
        order.sort_by(|&left, &right| filters[left].retained.total_cmp(&filters[right].retained));
        let mut reduced = BTreeSet::<usize>::new();
        let mut stages = order
            .into_iter()
            .map(|index| {
                let filter = &filters[index];
                Stage {
                    selectivity: if reduced.insert(filter.column) {
                        filter.retained
                    } else {
                        1.0
                    },
                    width: filter.key_width,
                    surviving: 1.0,
                    filter: Some(index),
                }
            })
            .collect::<Vec<_>>();
        stages.extend(self.static_stage.as_ref().map(|stage| Stage {
            selectivity: stage.selectivity,
            width: stage.width,
            surviving: 1.0,
            filter: None,
        }));
        stages.sort_by(|left, right| left.selectivity.total_cmp(&right.selectivity));
        let mut surviving = 1.0;
        for stage in &mut stages {
            stage.surviving = surviving;
            surviving *= stage.selectivity;
        }
        stages
    }

    /// Decode charged under `filters`, priced by the executor's staged access.
    /// The risk assumes every runtime filter keeps every row.
    fn decode(&self, filters: &[LaneFilter]) -> Result<PhysicalCost> {
        let bytes = |filters: &[LaneFilter]| {
            let stages = self
                .stages(filters)
                .into_iter()
                .map(|stage| ScanPredicateStage {
                    selectivity: stage.selectivity,
                    width: stage.width as usize,
                    runtime: stage.filter.is_some(),
                })
                .collect::<Vec<_>>();
            ScanAccessCostModel::default().staged_decode_bytes(self.width as usize, &stages) / 32.0
        };
        let expected = bytes(filters);
        let unfiltered = filters
            .iter()
            .map(|filter| LaneFilter {
                retained: 1.0,
                ..filter.clone()
            })
            .collect::<Vec<_>>();
        divisible_work_cost(CompactRange::new(
            self.rows.lower * expected,
            self.rows.expected * expected,
            (self.rows.upper * bytes(&unfiltered)).max(self.rows.expected * expected),
        )?)
    }

    /// Evaluation of every runtime filter on the rows reaching its stage.
    /// The risk evaluates each on every scanned row.
    fn apply(
        &self,
        filters: &[LaneFilter],
        calibration: &MachineCalibrationBundle,
    ) -> Result<PhysicalCost> {
        let mut total = PhysicalCost::ZERO;
        for stage in self.stages(filters) {
            if stage.filter.is_some() {
                let rows = CompactRange::new(
                    self.rows.lower * stage.surviving,
                    self.rows.expected * stage.surviving,
                    self.rows.upper,
                )?;
                total = total.sequential(runtime_filter_apply_cost(rows, calibration)?)?;
            }
        }
        Ok(total)
    }
}

fn ppm(fraction: f64) -> u32 {
    (fraction.clamp(0.0, 1.0) * 1_000_000.0).ceil() as u32
}

/// The scan conditions the runtime filter `flavor` adds at `lane`'s source,
/// one per key column, and the evaluation work the owning join charges for
/// them on every source row.
pub(super) fn lane_filters(
    lane: &SourceLane,
    facts: &ResolvedPlannerCostFacts,
    flavor: PhysicalImplementationFlavor,
    context: &LaneContext<'_>,
) -> Result<Option<(Vec<LaneFilter>, PhysicalCost)>> {
    let LaneContext {
        calibration, tasks, ..
    } = *context;
    let Some(build) = runtime_filter_build(facts, flavor, tasks)? else {
        return Ok(None);
    };
    let Some(input) = build
        .sources
        .iter()
        .find(|input| input.source == lane.source)
    else {
        return Ok(None);
    };
    if input.rows.expected <= 0.0 {
        return Ok(None);
    }
    let model = ScanAccessCostModel::default();
    let filters = input
        .keys
        .iter()
        .map(|key| LaneFilter {
            column: key.column,
            retained: build.key_retained(input.rows.expected, key),
            key_width: facts
                .runtime_filter_key_types
                .get(key.key)
                .map_or(8, |ty| model.estimated_width(ty) as u64),
        })
        .collect();
    Ok(Some((
        filters,
        runtime_filter_apply_cost(
            scaled_work(input.rows, input.keys.len() as f64)?,
            calibration,
        )?,
    )))
}

/// How each child's rows continue in an operator's output stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamShape {
    /// Breaker or unsupported boundary: lanes end here.
    Closed,
    /// Projection: the only child streams through.
    Single,
    /// Filter: the only child streams through; directly above a scan it
    /// becomes the scan's static stage.
    Filter,
    /// Comparison join: the probe side streams, the build side ends.
    Join,
    /// Cross product: lanes stay reachable but stop streaming.
    Opaque,
    /// UNION ALL: each branch streams its share of the output.
    Concatenate,
}

pub(crate) fn stream_shape<Child>(
    operator: &paro_planner::logical::operator::LogicalOperator<Child>,
) -> StreamShape {
    use paro_planner::logical::operator::{Join, LogicalOperator, SetOpType};
    match operator {
        LogicalOperator::Filter(_) => StreamShape::Filter,
        LogicalOperator::Projection(_) => StreamShape::Single,
        LogicalOperator::Join(Join::Comparison(_)) => StreamShape::Join,
        LogicalOperator::Join(Join::Cross(_)) => StreamShape::Opaque,
        LogicalOperator::SetOperation(setop)
            if setop.setop_type == SetOpType::Union && setop.setop_all =>
        {
            StreamShape::Concatenate
        }
        _ => StreamShape::Closed,
    }
}

#[derive(Clone, Copy)]
pub(super) struct LaneContext<'a> {
    pub facts: &'a ResolvedPlannerCostFacts,
    pub calibration: &'a MachineCalibrationBundle,
    pub tasks: u16,
}

/// Carry the children's lanes through the selected implementation and
/// return them with the operator's final cost. Runtime filters apply first,
/// so this operator's work is attributed on the basis of its filtered input.
/// A filter directly above a scan reprices the scan decode as staged access.
pub(super) fn propagate_lanes(
    stream: StreamShape,
    implementation: PhysicalImplementationFlavor,
    local: PhysicalCost,
    mut cost: PhysicalCost,
    children: &[&PhysicalResponse],
    own_lane: Option<SourceLane>,
    context: LaneContext<'_>,
) -> Result<(Box<[SourceLane]>, PhysicalCost)> {
    use PhysicalImplementationFlavor as F;
    let LaneContext {
        facts,
        calibration,
        tasks,
        ..
    } = context;
    let mut lanes = Vec::new();
    if stream != StreamShape::Closed {
        let probe_stream = hash_join_probe_stream_cost(facts, implementation, calibration, tasks)?;
        let probe_child = match implementation {
            F::HashJoinBuildLeft | F::HashJoinBuildLeftRuntimeFilter => 1,
            _ => 0,
        };
        let total_rows = facts
            .child_rows
            .iter()
            .map(|rows| rows.expected)
            .sum::<f64>();
        for (index, child) in children.iter().enumerate() {
            for lane in child.lanes.iter() {
                let mut lane = match lane_filters(lane, facts, implementation, &context)? {
                    Some((filters, _)) => lane.with_filters(filters, calibration)?,
                    None => lane.clone(),
                };
                match stream {
                    StreamShape::Filter => {
                        let input = facts.child_rows.first().map_or(0.0, |rows| rows.expected);
                        match filter_evaluation_cost(facts)? {
                            Some(evaluation) if lane.at_scan && input > 0.0 => {
                                let staged = lane.with_static_stage(
                                    facts.output_rows.expected / input,
                                    facts.predicate_width.unwrap_or(0),
                                    evaluation,
                                )?;
                                cost = cost.replace_work(lane.decode, staged.decode)?;
                                lane = staged;
                                lane.attribute(local.replace_work(evaluation, PhysicalCost::ZERO)?)?
                            }
                            _ => lane.attribute(local)?,
                        }
                    }
                    StreamShape::Single => lane.attribute(local)?,
                    StreamShape::Join => match probe_stream {
                        Some(work) if index == probe_child => lane.attribute(work)?,
                        _ => lane.share = 0.0,
                    },
                    StreamShape::Concatenate => {
                        let rows = facts
                            .child_rows
                            .get(index)
                            .map_or(0.0, |rows| rows.expected);
                        lane.share *= if total_rows > 0.0 {
                            rows / total_rows
                        } else {
                            0.0
                        };
                        lane.attribute(local)?;
                    }
                    StreamShape::Opaque => lane.share = 0.0,
                    StreamShape::Closed => unreachable!(),
                }
                lane.at_scan = false;
                lanes.push(lane);
            }
        }
    }
    lanes.extend(own_lane);
    Ok((lanes.into_boxed_slice(), cost))
}

#[cfg(test)]
mod tests;

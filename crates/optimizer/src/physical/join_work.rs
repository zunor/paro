// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Join work units shared by region enumeration and physical implementation.
//!
//! Expected work ranks alternatives. Statistical risk and executable memory
//! floors are separate contracts; neither changes the amount of expected work.

use crate::cascades::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, OP_HASH_KEY_BYTE_BLOCK, OP_TUPLE_BYTE_BLOCK,
};
use crate::cascades::ids::OpClassId;
use crate::physical::cost::CompactRange;
use paro_common::error::Result;

pub(crate) const HASH_BUILD: OpClassId = OpClassId(1);
pub(crate) const HASH_PROBE: OpClassId = OpClassId(2);

/// A linear work description, not a memory requirement or a row-count proof.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HashJoinWork {
    pub build_rows: f64,
    pub probe_rows: f64,
    pub output_rows: f64,
    pub build_width: f64,
    pub probe_width: f64,
    pub output_width: f64,
    pub key_width: f64,
}

impl HashJoinWork {
    fn units(self) -> [f64; 4] {
        [
            self.build_rows,
            self.probe_rows + self.output_rows,
            // Reading the input stream and writing retained hash rows are
            // different work. Charging only the former makes swapping two
            // inputs width-insensitive, despite a very different build table.
            (self.build_rows
                * (self.build_width + hash_build_width(self.build_width, self.key_width))
                + self.probe_rows * self.probe_width
                + self.output_rows * self.output_width)
                / 32.0,
            (self.build_rows + self.probe_rows) * (self.key_width - 8.0).max(0.0) / 32.0,
        ]
    }
}

pub(crate) fn hash_build_width(payload: f64, keys: f64) -> f64 {
    crate::join::build_probe_side::estimate_hash_build_row_width(payload as usize, keys as usize)
        as f64
}

/// Frozen statement calibration. DP pricing is a dot product, not a call into
/// physical selection, allocation of a recipe, or construction of a plan.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JoinWorkPricing {
    latency: [f64; 4],
    pair_latency: f64,
    range_latency: f64,
}

impl JoinWorkPricing {
    pub(crate) fn new(calibration: &MachineCalibrationBundle) -> Result<Self> {
        let mut latency = [0.0; 4];
        for (index, class) in [
            HASH_BUILD,
            HASH_PROBE,
            OP_TUPLE_BYTE_BLOCK,
            OP_HASH_KEY_BYTE_BLOCK,
        ]
        .into_iter()
        .enumerate()
        {
            let mut work = LocalOperatorWork::default();
            work.add(class, CompactRange::point(1.0)?)?;
            latency[index] = calibration.fold(&work)?.work_latency.expected;
        }
        let mut pairs = LocalOperatorWork::default();
        pairs.add(OpClassId(3), CompactRange::point(1.0)?)?;
        let mut accepted = LocalOperatorWork::default();
        accepted.add(OpClassId(5), CompactRange::point(1.0)?)?;
        Ok(Self {
            latency,
            pair_latency: calibration.fold(&pairs)?.work_latency.expected,
            range_latency: calibration.fold(&accepted)?.work_latency.expected,
        })
    }

    pub(crate) fn price(self, work: HashJoinWork) -> f64 {
        work.units()
            .into_iter()
            .zip(self.latency)
            .map(|(units, price)| units * price)
            .sum()
    }

    pub(crate) fn non_hash(self, work: HashJoinWork, compares_pairs: bool) -> f64 {
        let bytes = work.build_rows * work.build_width
            + work.probe_rows * work.probe_width
            + work.output_rows * work.output_width;
        bytes / 32.0 * self.latency[2]
            + if compares_pairs {
                work.build_rows * work.probe_rows * self.pair_latency
                    + work.output_rows * self.range_latency
            } else {
                0.0
            }
    }
}

/// The physical kernel evaluates the same units at lower/expected/risk points.
/// Input risks must already be resolved by the caller; this function cannot
/// promote a statistical interval into a hard resource bound.
pub(crate) fn add_hash_join_work(
    work: &mut LocalOperatorWork,
    points: [HashJoinWork; 3],
) -> Result<()> {
    let [lower, expected, upper] = points.map(HashJoinWork::units);
    for (index, class) in [
        HASH_BUILD,
        HASH_PROBE,
        OP_TUPLE_BYTE_BLOCK,
        OP_HASH_KEY_BYTE_BLOCK,
    ]
    .into_iter()
    .enumerate()
    {
        work.add(
            class,
            CompactRange::new(lower[index], expected[index], upper[index])?,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regional_and_physical_hash_work_use_identical_units_and_calibration() {
        let calibration = MachineCalibrationBundle::builtin_production();
        let pricing = JoinWorkPricing::new(&calibration).unwrap();
        for rows in [0.0, 1.0, 73_000.0] {
            for width in [4.0, 32.0, 256.0] {
                let input = HashJoinWork {
                    build_rows: rows,
                    probe_rows: 100_000.0,
                    output_rows: 23_000.0,
                    build_width: width,
                    probe_width: 24.0,
                    output_width: width + 24.0,
                    key_width: 16.0,
                };
                let mut work = LocalOperatorWork::default();
                add_hash_join_work(&mut work, [input; 3]).unwrap();
                let physical = calibration.fold(&work).unwrap().work_latency.expected;
                assert!((pricing.price(input) - physical).abs() <= physical.abs() * 1e-12);
            }
        }
    }

    #[test]
    fn risk_does_not_replace_expected_work() {
        let calibration = MachineCalibrationBundle::builtin_production();
        let input = HashJoinWork {
            build_rows: 10.0,
            probe_rows: 100.0,
            output_rows: 50.0,
            build_width: 32.0,
            probe_width: 8.0,
            output_width: 40.0,
            key_width: 8.0,
        };
        let mut upper = input;
        upper.build_rows = 1_000_000.0;
        let mut work = LocalOperatorWork::default();
        add_hash_join_work(&mut work, [input, input, upper]).unwrap();
        let physical = calibration.fold(&work).unwrap();
        assert_eq!(
            physical.work_latency.expected,
            JoinWorkPricing::new(&calibration).unwrap().price(input)
        );
        assert!(physical.work_latency.upper > physical.work_latency.expected);
    }

    #[test]
    fn retained_build_payload_breaks_otherwise_symmetric_input_work() {
        let pricing =
            JoinWorkPricing::new(&MachineCalibrationBundle::builtin_production()).unwrap();
        let narrow_build = HashJoinWork {
            build_rows: 100_000.0,
            probe_rows: 100_000.0,
            output_rows: 100_000.0,
            build_width: 16.0,
            probe_width: 256.0,
            output_width: 272.0,
            key_width: 8.0,
        };
        let wide_build = HashJoinWork {
            build_width: 256.0,
            probe_width: 16.0,
            ..narrow_build
        };
        assert!(pricing.price(narrow_build) < pricing.price(wide_build));
    }
}

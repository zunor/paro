//! Versioned progress contracts used for row-goal costing and EXPLAIN.

use std::collections::BTreeMap;

use paro_common::error::{self as paro_error, Result};

use super::cost::CompactRange;
use super::ids::{Fingerprint, ProgressSummaryId};

#[derive(Debug, Clone, PartialEq)]
pub enum ProgressModel {
    Blocking,
    UniformStreaming,
    Piecewise(Box<[ProgressPoint]>),
    RankedProvider {
        provider_curve: Fingerprint,
    },
    ExternalBatched {
        batch_rows: u32,
        first_batch_latency: CompactRange,
        per_batch_latency: CompactRange,
        blocking: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProgressPoint {
    pub work_fraction: f64,
    pub rows_fraction: f64,
}

impl ProgressModel {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Piecewise(points) => {
                if points.is_empty() {
                    return Err(paro_error::internal("piecewise progress curve is empty"));
                }
                let mut previous = ProgressPoint {
                    work_fraction: 0.0,
                    rows_fraction: 0.0,
                };
                for point in points {
                    if !point.work_fraction.is_finite()
                        || !point.rows_fraction.is_finite()
                        || point.work_fraction < previous.work_fraction
                        || point.rows_fraction < previous.rows_fraction
                        || !(0.0..=1.0).contains(&point.work_fraction)
                        || !(0.0..=1.0).contains(&point.rows_fraction)
                    {
                        return Err(paro_error::internal(
                            "piecewise progress curve is not monotone and bounded",
                        ));
                    }
                    previous = *point;
                }
                if previous.work_fraction != 1.0 || previous.rows_fraction != 1.0 {
                    return Err(paro_error::internal(
                        "piecewise progress curve must terminate at (1, 1)",
                    ));
                }
            }
            Self::ExternalBatched {
                batch_rows,
                first_batch_latency,
                per_batch_latency,
                ..
            } => {
                if *batch_rows == 0 {
                    return Err(paro_error::internal(
                        "external progress model has zero batch size",
                    ));
                }
                CompactRange::new(
                    first_batch_latency.lower,
                    first_batch_latency.expected,
                    first_batch_latency.upper,
                )?;
                CompactRange::new(
                    per_batch_latency.lower,
                    per_batch_latency.expected,
                    per_batch_latency.upper,
                )?;
            }
            Self::Blocking | Self::UniformStreaming | Self::RankedProvider { .. } => {}
        }
        Ok(())
    }

    /// Conservative work fraction needed for a requested output fraction.
    /// Missing/provider-specific certificates return full work.
    pub fn work_fraction_for_rows(&self, requested: f64) -> f64 {
        let requested = requested.clamp(0.0, 1.0);
        match self {
            Self::Blocking => 1.0,
            Self::UniformStreaming => requested,
            Self::Piecewise(points) => points
                .iter()
                .find(|point| point.rows_fraction >= requested)
                .map(|point| point.work_fraction)
                .unwrap_or(1.0),
            Self::ExternalBatched { blocking: true, .. } | Self::RankedProvider { .. } => 1.0,
            Self::ExternalBatched {
                batch_rows,
                blocking: false,
                ..
            } => requested.max(1.0 / *batch_rows as f64).min(1.0),
        }
    }
}

#[derive(Debug, Default)]
pub struct ProgressArena {
    models: Vec<ProgressModel>,
    index: BTreeMap<Fingerprint, Vec<ProgressSummaryId>>,
}

impl ProgressArena {
    pub fn intern(
        &mut self,
        fingerprint: Fingerprint,
        model: ProgressModel,
    ) -> Result<ProgressSummaryId> {
        model.validate()?;
        if let Some(ids) = self.index.get(&fingerprint) {
            if let Some(id) = ids
                .iter()
                .copied()
                .find(|id| self.models[id.index()] == model)
            {
                return Ok(id);
            }
        }
        let id = ProgressSummaryId::new(self.models.len());
        self.models.push(model);
        self.index.entry(fingerprint).or_default().push(id);
        Ok(id)
    }

    pub fn get(&self, id: ProgressSummaryId) -> Option<&ProgressModel> {
        self.models.get(id.index())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocking_never_gets_optimistic_early_stop() {
        assert_eq!(ProgressModel::Blocking.work_fraction_for_rows(0.01), 1.0);
        assert_eq!(
            ProgressModel::UniformStreaming.work_fraction_for_rows(0.01),
            0.01
        );
    }

    #[test]
    fn malformed_piecewise_curve_is_rejected() {
        let model = ProgressModel::Piecewise(
            vec![
                ProgressPoint {
                    work_fraction: 0.8,
                    rows_fraction: 0.8,
                },
                ProgressPoint {
                    work_fraction: 0.7,
                    rows_fraction: 1.0,
                },
            ]
            .into_boxed_slice(),
        );
        assert!(model.validate().is_err());
    }
}

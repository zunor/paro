// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Read-before-write safety is an executable barrier, not a search enforcer.

use paro_common::error::{self as error, Result};
use paro_planner::physical::requirements::{
    MutationSafetyRequirement, ProvidedMutationSafety, ProvidedProperties, ProvidedReplayability,
    RequiredProperties,
};
use paro_planner::physical::{Fingerprint, MutationBarrierId, StableFingerprintBuilder};

pub(crate) fn materialize_input(
    mut provided: ProvidedProperties,
    required: &RequiredProperties,
    barrier: MutationBarrierId,
) -> Result<ProvidedProperties> {
    let MutationSafetyRequirement::StableReadBeforeWrite { targets, snapshot } =
        &required.mutation_safety
    else {
        return Err(error::internal(
            "mutation input has no read-before-write requirement",
        ));
    };
    provided.mutation_safety = ProvidedMutationSafety::MaterializedMutationInput {
        targets: targets.clone(),
        snapshot: *snapshot,
        barrier,
    };
    provided.replayability = ProvidedReplayability::Rewindable;
    provided.validate()?;
    Ok(provided)
}

pub(crate) fn identity(barrier: MutationBarrierId) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    // Preserve the typed mutation-barrier encoding, independent of module names.
    builder.write_u64(9);
    builder.write_u64(barrier.0 as u64);
    builder.finish()
}

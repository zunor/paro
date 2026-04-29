// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Catalog transaction participants and durable-record appliers.

pub mod participant;
pub mod record_applier;

pub use participant::{
    CatalogCommitParticipant, CatalogPreparedChange, CatalogPreparedCommitPart,
    CATALOG_PARTICIPANT_ID,
};
pub use record_applier::CatalogCommittedRecordApplier;

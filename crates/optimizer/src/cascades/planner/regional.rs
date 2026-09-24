// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! The relational program has an explicit, finite dependency order. It is not
//! a promise ordering for an agenda and it does not iterate to a fixed point.
//! Normalization replaces pre-normal forms after semantic verification;
//! regional exploration retains its input alongside cost alternatives. The
//! closed physical solver, not aggregate/RF counts, selects the result.

use super::super::engine::RegionalPass::{self, Explore, Normalize};
use super::*;

pub(super) const RELATIONAL_PROGRAM: &[RegionalPass] = &[
    // Local semantic reductions expose region boundaries.
    Normalize(MARK_JOIN_TO_SEMI_RULE),
    Normalize(JOIN_ELIMINATION_RULE),
    Normalize(AGGREGATE_POST_REDUCTION_RULE),
    Normalize(AGGREGATE_JOIN_SUBSUMPTION_RULE),
    Normalize(AGGREGATE_NON_NULL_INPUT_RULE),
    // Establish aggregate region alternatives before domain transport inserts
    // representation-specific wrappers across their boundaries. Both shapes
    // then receive the same demand normalization before join enumeration and
    // physical pricing. Do not require an obsolete pre-normal form to remain
    // in the cost frontier just so a later recognizer can find the region.
    Explore(AGGREGATE_DIMENSION_DEFERRAL_RULE),
    Explore(AGGREGATE_DIMENSION_SHARING_RULE),
    Explore(AGGREGATE_JOIN_PREAGGREGATION_RULE),
    // Demand and survivor domains precede cardinality-sensitive enumeration.
    Normalize(CTE_DEMAND_PUSHDOWN_RULE),
    Normalize(CTE_FILTER_PUSHDOWN_RULE),
    Normalize(PREDICATE_TRANSFER_RULE),
    Normalize(KEY_DOMAIN_TRANSFER_RULE),
    Explore(JOIN_REGION_ENUMERATION_RULE),
    // Newly constructed joins may expose new legal pushdown opportunities.
    // This is an explicit second pass, never unbounded mutual reactivation.
    Normalize(PREDICATE_TRANSFER_RULE),
    Normalize(CTE_FILTER_PUSHDOWN_RULE),
    Normalize(AGGREGATE_INPUT_MATERIALIZATION_RULE),
    Normalize(TOP_N_INTRODUCTION_RULE),
    Normalize(LIMIT_PUSHDOWN_RULE),
    Explore(LATE_PAYLOAD_FETCH_RULE),
];

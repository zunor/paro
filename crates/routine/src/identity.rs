// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

pub use crate::spec::{RoutineId, RoutineIdentity};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BuiltinIntrinsicId {
    Add,
    Subtract,
    Multiply,
    Divide,
    IntegerDivide,
    FullTextMatch,
    FullTextMatchInternal,
    Bm25,
    Bm25ScoreInternal,
    TsRank,
    TsRankCd,
    ToTsVector,
    PlainToTsQuery,
    ToTsQuery,
    PhraseToTsQuery,
    WebSearchToTsQuery,
    L2Distance,
    L1Distance,
    CosineDistance,
    NegativeInnerProduct,
    SparseDistance,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BuiltinSemanticTag {
    Deterministic,
    NoSideEffects,
    Foldable,
    SearchOptimized,
    VectorOptimized,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RoutineCallIdentity {
    Catalog {
        routine_id: RoutineId,
        generation: u64,
    },
    Builtin {
        intrinsic: BuiltinIntrinsicId,
        semantic_tags: Vec<BuiltinSemanticTag>,
    },
}

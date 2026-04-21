// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::Expression;
use paro_common::types::LogicalType;
use paro_function::scalar::{
    BoundScalarFunction, FunctionSideEffects, FunctionStability, ScalarDispatch,
};
use paro_routine::{
    BoundRoutineCallMeta, BuiltinIntrinsicId, BuiltinSemanticTag, ExecutionBoundary,
    PlacementClass, RoutineCallIdentity, RowSemantics,
};

#[derive(Debug, Clone)]
pub struct FunctionExpression {
    pub function: BoundScalarFunction,
    pub children: Vec<Expression>,
    pub return_type: LogicalType,
    pub routine_meta: Option<BoundRoutineCallMeta>,
}

impl FunctionExpression {
    pub fn new<F>(function: F, children: Vec<Expression>, return_type: LogicalType) -> Self
    where
        F: Into<BoundScalarFunction>,
    {
        let function = function.into();
        Self {
            routine_meta: Some(infer_builtin_routine_meta(&function)),
            function,
            children,
            return_type,
        }
    }

    pub fn with_routine_meta(mut self, routine_meta: BoundRoutineCallMeta) -> Self {
        self.routine_meta = Some(routine_meta);
        self
    }

    pub fn routine_meta(&self) -> Option<&BoundRoutineCallMeta> {
        self.routine_meta.as_ref()
    }

    pub fn routine_identity(&self) -> Option<&RoutineCallIdentity> {
        self.routine_meta().map(|meta| &meta.identity)
    }

    pub fn builtin_intrinsic(&self) -> Option<&BuiltinIntrinsicId> {
        match self.routine_identity()? {
            RoutineCallIdentity::Builtin { intrinsic, .. } => Some(intrinsic),
            RoutineCallIdentity::Catalog { .. } => None,
        }
    }

    pub fn has_builtin_tag(&self, expected: BuiltinSemanticTag) -> bool {
        let Some(RoutineCallIdentity::Builtin { semantic_tags, .. }) = self.routine_identity()
        else {
            return false;
        };
        semantic_tags.contains(&expected)
    }

    pub fn crosses_execution_boundary(&self) -> bool {
        matches!(
            self.routine_meta(),
            Some(meta) if meta.boundary.placement == PlacementClass::External
        )
    }

    pub fn is_foldable_native(&self) -> bool {
        !self.crosses_execution_boundary()
            && self.function.stability == FunctionStability::Consistent
            && self.function.side_effects == FunctionSideEffects::NoSideEffects
            && self.has_builtin_tag(BuiltinSemanticTag::Foldable)
    }
}

fn infer_builtin_routine_meta(function: &BoundScalarFunction) -> BoundRoutineCallMeta {
    BoundRoutineCallMeta {
        identity: RoutineCallIdentity::Builtin {
            intrinsic: infer_builtin_intrinsic(function.name.as_str()),
            semantic_tags: infer_builtin_semantic_tags(function),
        },
        semantics: infer_builtin_semantics(function),
        boundary: ExecutionBoundary {
            placement: PlacementClass::Native,
            may_block: false,
            row_semantics: RowSemantics::RowPreserving,
        },
        spec: None,
    }
}

fn infer_builtin_intrinsic(name: &str) -> BuiltinIntrinsicId {
    match name.to_ascii_lowercase().as_str() {
        "+" => BuiltinIntrinsicId::Add,
        "-" => BuiltinIntrinsicId::Subtract,
        "*" => BuiltinIntrinsicId::Multiply,
        "/" => BuiltinIntrinsicId::Divide,
        "//" => BuiltinIntrinsicId::IntegerDivide,
        "fulltext_match" => BuiltinIntrinsicId::FullTextMatch,
        "fulltext_match_internal" => BuiltinIntrinsicId::FullTextMatchInternal,
        "bm25" => BuiltinIntrinsicId::Bm25,
        "bm25_score_internal" => BuiltinIntrinsicId::Bm25ScoreInternal,
        "ts_rank" => BuiltinIntrinsicId::TsRank,
        "ts_rank_cd" => BuiltinIntrinsicId::TsRankCd,
        "to_tsvector" => BuiltinIntrinsicId::ToTsVector,
        "plainto_tsquery" => BuiltinIntrinsicId::PlainToTsQuery,
        "to_tsquery" => BuiltinIntrinsicId::ToTsQuery,
        "phraseto_tsquery" => BuiltinIntrinsicId::PhraseToTsQuery,
        "websearch_to_tsquery" => BuiltinIntrinsicId::WebSearchToTsQuery,
        "l2_distance" => BuiltinIntrinsicId::L2Distance,
        "l1_distance" => BuiltinIntrinsicId::L1Distance,
        "cosine_distance" => BuiltinIntrinsicId::CosineDistance,
        "negative_inner_product" | "neg_inner_product" => BuiltinIntrinsicId::NegativeInnerProduct,
        "sparse_distance" => BuiltinIntrinsicId::SparseDistance,
        other => BuiltinIntrinsicId::Other(other.to_string()),
    }
}

fn infer_builtin_semantic_tags(function: &BoundScalarFunction) -> Vec<BuiltinSemanticTag> {
    let mut tags = Vec::new();
    if function.stability == FunctionStability::Consistent {
        tags.push(BuiltinSemanticTag::Deterministic);
    }
    if function.side_effects == FunctionSideEffects::NoSideEffects {
        tags.push(BuiltinSemanticTag::NoSideEffects);
    }
    if function.stability == FunctionStability::Consistent
        && function.side_effects == FunctionSideEffects::NoSideEffects
        && matches!(
            function.dispatch,
            ScalarDispatch::Direct(_) | ScalarDispatch::Variadic(_)
        )
    {
        tags.push(BuiltinSemanticTag::Foldable);
    }
    match infer_builtin_intrinsic(function.name.as_str()) {
        BuiltinIntrinsicId::FullTextMatch
        | BuiltinIntrinsicId::FullTextMatchInternal
        | BuiltinIntrinsicId::Bm25
        | BuiltinIntrinsicId::Bm25ScoreInternal
        | BuiltinIntrinsicId::TsRank
        | BuiltinIntrinsicId::TsRankCd
        | BuiltinIntrinsicId::ToTsVector
        | BuiltinIntrinsicId::PlainToTsQuery
        | BuiltinIntrinsicId::ToTsQuery
        | BuiltinIntrinsicId::PhraseToTsQuery
        | BuiltinIntrinsicId::WebSearchToTsQuery => {
            tags.push(BuiltinSemanticTag::SearchOptimized);
        }
        BuiltinIntrinsicId::L2Distance
        | BuiltinIntrinsicId::L1Distance
        | BuiltinIntrinsicId::CosineDistance
        | BuiltinIntrinsicId::NegativeInnerProduct
        | BuiltinIntrinsicId::SparseDistance => {
            tags.push(BuiltinSemanticTag::VectorOptimized);
        }
        _ => {}
    }
    tags
}

fn infer_builtin_semantics(function: &BoundScalarFunction) -> paro_routine::RoutineSemantics {
    let stability = match function.stability {
        FunctionStability::Consistent => paro_routine::RoutineStability::Immutable,
        FunctionStability::ConsistentWithinQuery => paro_routine::RoutineStability::Stable,
        FunctionStability::Volatile => paro_routine::RoutineStability::Volatile,
    };
    let side_effects = match function.side_effects {
        FunctionSideEffects::NoSideEffects => paro_routine::RoutineSideEffects::None,
        FunctionSideEffects::HasSideEffects => paro_routine::RoutineSideEffects::HasSideEffects,
    };
    paro_routine::RoutineSemantics {
        stability,
        null_policy: paro_routine::RoutineNullPolicy::CalledOnNullInput,
        side_effects,
        row_semantics: RowSemantics::RowPreserving,
        may_block: false,
    }
}

//! Conservative grant-sensitivity closure and cross-class goal sharing.

use std::collections::BTreeSet;

use paro_common::error::{self as paro_error, Result};

use super::ids::{Fingerprint, GroupId, ImplementationId, StableFingerprintBuilder};
use super::memo::Memo;
use super::rules::{GrantDependencyDescriptor, ImplementationContext, ImplementationRegistry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantInvarianceProof {
    pub root: GroupId,
    pub registry_fingerprint: Fingerprint,
    pub groups: Box<[GroupId]>,
    pub implementations: Box<[ImplementationId]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantSensitivitySummary {
    Invariant(GrantInvarianceProof),
    Sensitive {
        witness_group: GroupId,
        witness_implementation: ImplementationId,
    },
}

impl GrantSensitivitySummary {
    pub fn is_sensitive(&self) -> bool {
        matches!(self, Self::Sensitive { .. })
    }
}

/// Derive sensitivity from the complete reachable logical closure. False
/// positives are allowed; a registered implementation declaring sensitivity
/// for an expression is enough to split that group by grant class.
pub fn derive_grant_sensitivity(
    memo: &Memo,
    registry: &ImplementationRegistry,
    root: GroupId,
) -> Result<GrantSensitivitySummary> {
    let root = memo.canonical_group(root);
    let mut pending = vec![root];
    let mut groups = BTreeSet::new();
    let mut implementations = BTreeSet::new();

    while let Some(group) = pending.pop() {
        let group = memo.canonical_group(group);
        if !groups.insert(group) {
            continue;
        }
        let group_ref = memo
            .group(group)
            .ok_or_else(|| paro_error::internal("grant proof references an unknown group"))?;
        for &logical_id in group_ref.logical_exprs() {
            let logical = memo.logical_expr(logical_id).ok_or_else(|| {
                paro_error::internal("grant proof references an unknown logical expression")
            })?;
            pending.extend(logical.key.children.iter().copied());
            let context = ImplementationContext { memo, group };
            for (implementation_id, implementation) in registry.implementation_entries() {
                implementations.insert(implementation_id);
                if implementation.grant_dependency_for(logical, &context)
                    == GrantDependencyDescriptor::Sensitive
                {
                    return Ok(GrantSensitivitySummary::Sensitive {
                        witness_group: group,
                        witness_implementation: implementation_id,
                    });
                }
            }
        }
    }

    Ok(GrantSensitivitySummary::Invariant(GrantInvarianceProof {
        root,
        registry_fingerprint: registry_fingerprint(registry),
        groups: groups.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        implementations: implementations
            .into_iter()
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }))
}

pub fn verify_grant_invariance(
    memo: &Memo,
    registry: &ImplementationRegistry,
    proof: &GrantInvarianceProof,
) -> Result<()> {
    match derive_grant_sensitivity(memo, registry, proof.root)? {
        GrantSensitivitySummary::Invariant(recomputed) if &recomputed == proof => Ok(()),
        GrantSensitivitySummary::Invariant(_) => Err(paro_error::internal(
            "grant invariance proof does not match the current Memo/registry closure",
        )),
        GrantSensitivitySummary::Sensitive { .. } => Err(paro_error::internal(
            "grant invariance proof covers a grant-sensitive implementation",
        )),
    }
}

fn registry_fingerprint(registry: &ImplementationRegistry) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    for (id, implementation) in registry.implementation_entries() {
        builder.write_u64(id.0 as u64);
        builder.write_u64(match implementation.grant_dependency() {
            GrantDependencyDescriptor::Invariant => 0,
            GrantDependencyDescriptor::Sensitive => 1,
        });
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;

    use super::*;
    use crate::cascades::budget::SearchBudget;
    use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility, GroupSchema};
    use crate::cascades::ids::{ColumnId, LogicalExprId, LogicalPayloadId};
    use crate::cascades::memo::{
        EquivalenceProof, LogicalExpr, LogicalExprKey, LogicalProperties, OptimizationGoal,
    };
    use crate::cascades::rules::{PhysicalCandidate, PhysicalImplementation};

    fn seeded_memo() -> (Memo, GroupId) {
        let mut memo = Memo::new(SearchBudget::default());
        let group = memo.create_group(
            GroupSchema::new([ColumnDesc {
                id: ColumnId(0),
                logical_type: LogicalType::Integer,
                nullable: false,
                origin: ColumnOrigin::Derived {
                    key: Fingerprint(1),
                },
                visibility: ColumnVisibility::Visible,
                name_hint: None,
            }])
            .unwrap(),
            LogicalProperties::default(),
        );
        memo.insert_logical(
            group,
            LogicalExprKey {
                operator: Fingerprint(7),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId(0),
            EquivalenceProof::Initial,
        )
        .unwrap();
        (memo, group)
    }

    struct SensitiveImplementation;

    impl PhysicalImplementation for SensitiveImplementation {
        fn id(&self) -> ImplementationId {
            ImplementationId(9)
        }

        fn grant_dependency(&self) -> GrantDependencyDescriptor {
            GrantDependencyDescriptor::Sensitive
        }

        fn matches(
            &self,
            _expr: &LogicalExpr,
            _goal: OptimizationGoal,
            _ctx: &ImplementationContext<'_>,
        ) -> bool {
            true
        }

        fn candidates(
            &self,
            _expr: LogicalExprId,
            _goal: OptimizationGoal,
            _ctx: &ImplementationContext<'_>,
        ) -> Result<Box<[PhysicalCandidate]>> {
            Ok(Box::new([]))
        }
    }

    #[test]
    fn invariant_proof_is_recomputed_against_registry_closure() {
        let (memo, root) = seeded_memo();
        let registry = ImplementationRegistry::default();
        let GrantSensitivitySummary::Invariant(proof) =
            derive_grant_sensitivity(&memo, &registry, root).unwrap()
        else {
            panic!("expected invariant closure")
        };
        verify_grant_invariance(&memo, &registry, &proof).unwrap();
    }

    #[test]
    fn sensitive_implementation_is_a_deterministic_witness() {
        let (memo, root) = seeded_memo();
        let mut registry = ImplementationRegistry::default();
        registry
            .register_implementation(SensitiveImplementation)
            .unwrap();
        assert_eq!(
            derive_grant_sensitivity(&memo, &registry, root).unwrap(),
            GrantSensitivitySummary::Sensitive {
                witness_group: root,
                witness_implementation: ImplementationId(9),
            }
        );
    }
}

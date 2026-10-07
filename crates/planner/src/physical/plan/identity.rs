// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Canonical graph traversal and artifact/structure identity.

use super::encoding::{
    edge_kind_key, write_canonical_kind, write_hashed, write_hashed_slice,
    write_optional_fingerprint, write_row_type,
};
use super::PhysicalPlan;
use crate::physical::identity::{Fingerprint, StableFingerprintBuilder};
use crate::physical::ids::PhysicalPlanNodeId;
use crate::physical::properties::PhysicalGrantContract;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalIdentityError {
    InvalidRoot,
    InvalidEdge,
    InvalidChild,
    Cycle,
    MissingAuxiliaryDependency {
        node: PhysicalPlanNodeId,
        dependency: u32,
    },
    /// No typed identity schema has been declared for this implementation.
    /// Identity generation must fail closed instead of assigning a shared
    /// placeholder to semantically different physical payloads.
    UnsupportedKind {
        kind: &'static str,
    },
    /// Two auxiliary producers have the same local key but no canonical
    /// ordering proof. Refuse to manufacture a cross-run identity from arena
    /// allocation order.
    AmbiguousAuxiliaryOrder,
}

impl fmt::Display for PhysicalIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRoot => formatter.write_str("physical identity has an invalid root"),
            Self::InvalidEdge => formatter.write_str("physical identity has an invalid edge"),
            Self::InvalidChild => formatter.write_str("physical identity has an invalid child"),
            Self::Cycle => formatter.write_str("physical identity graph contains a cycle"),
            Self::MissingAuxiliaryDependency { node, dependency } => write!(
                formatter,
                "physical identity node {node:?} references missing auxiliary dependency {dependency}"
            ),
            Self::UnsupportedKind { kind } => write!(
                formatter,
                "physical identity has no typed canonical encoder for {kind}"
            ),
            Self::AmbiguousAuxiliaryOrder => formatter.write_str(
                "physical identity has ambiguous auxiliary producer ordering",
            ),
        }
    }
}

impl PhysicalPlan {
    /// Artifact identity includes the selected implementation's resource
    /// contract, not merely its operator shape. Identical operators compiled
    /// for different task or memory requirements are not interchangeable.
    ///
    /// Admission is the intersection of every node's contract. Canonicalize
    /// that conjunction independently of arena ids, node order and duplicate
    /// constraints. Auxiliary producers are included: extraction has already
    /// compacted the complete executable arena before this method is called.
    pub fn artifact_fingerprint(
        &self,
        structural: Fingerprint,
    ) -> paro_common::error::Result<Fingerprint> {
        let mut contracts = BTreeSet::new();
        for node in self.nodes.iter() {
            let properties = self.properties.get(node.id).ok_or_else(|| {
                paro_common::error::internal("artifact identity requires every node grant contract")
            })?;
            if properties.grant_contract != PhysicalGrantContract::Invariant {
                contracts.insert(properties.grant_contract);
            }
        }
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_bytes(b"paro.physical.portfolio-admission.v1");
        fingerprint.write_fingerprint(structural);
        fingerprint.write_u64(contracts.len() as u64);
        for contract in contracts {
            match contract {
                PhysicalGrantContract::Invariant => {}
                PhysicalGrantContract::Parallelism { tasks } => {
                    fingerprint.write_u64(1);
                    fingerprint.write_u64(u64::from(tasks));
                }
                PhysicalGrantContract::Class(class) => {
                    fingerprint.write_u64(2);
                    fingerprint.write_u64(u64::from(class.0));
                }
            }
        }
        Ok(fingerprint.finish())
    }

    /// Stable identity of the executable physical structure.  This is an
    /// identity for receipt correlation, not a claim of SQL equivalence: the
    /// explain representation carries operator payloads while the tree
    /// carries child topology and output layout.  Keep the domain/version
    /// explicit so a consumer never treats a later encoding as compatible.
    pub fn structural_identity_fingerprint(&self) -> Result<Fingerprint, PhysicalIdentityError> {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_bytes(b"paro.physical-plan-structure.v6.typed-canonical");

        // The traversal is iterative on purpose.  Physical plans can contain
        // long unary spines and a fingerprint must not depend on recursion
        // depth or arena allocation order.
        let order = self.canonical_postorder()?;
        for (node, properties) in self.properties.iter() {
            for dependency in &properties.auxiliary_dependencies {
                if self
                    .edges
                    .get(crate::physical::edges::PhysicalEdgeId(*dependency))
                    .is_none()
                {
                    return Err(PhysicalIdentityError::MissingAuxiliaryDependency {
                        node,
                        dependency: *dependency,
                    });
                }
            }
        }
        let canonical_ids = order
            .iter()
            .enumerate()
            .map(|(index, id)| (*id, index as u64))
            .collect::<BTreeMap<_, _>>();
        builder.write_u64(order.len() as u64);
        builder.write_u64(*canonical_ids.get(&self.root).unwrap_or(&u64::MAX));

        for (canonical, id) in order.iter().enumerate() {
            let node = self.node(*id);
            builder.write_u64(canonical as u64);
            builder.write_bytes(node.kind.name().as_bytes());
            write_canonical_kind(self, *id, &mut builder)?;
            write_row_type(&mut builder, &node.output);
            let children = self.child_ids(&node.children);
            builder.write_u64(children.len() as u64);
            for child in children {
                builder.write_u64(*canonical_ids.get(child).unwrap_or(&u64::MAX));
            }
            if let Some(properties) = self.properties.get(*id) {
                builder.write_u64(1);
                write_hashed(
                    &mut builder,
                    b"required-properties",
                    &properties.required_from_parent,
                );
                write_hashed(&mut builder, b"provided-properties", &properties.provided);
                write_hashed(
                    &mut builder,
                    b"characteristics",
                    &properties.characteristics,
                );
                write_optional_fingerprint(&mut builder, properties.region_owner);
                write_hashed_slice(
                    &mut builder,
                    b"owned-artifacts",
                    &properties.owned_artifacts,
                );
                let mut dependencies = properties
                    .auxiliary_dependencies
                    .iter()
                    .filter_map(|edge| {
                        self.edges
                            .get(crate::physical::edges::PhysicalEdgeId(*edge))
                    })
                    .filter_map(|edge| {
                        Some((
                            *canonical_ids.get(&edge.producer)?,
                            *canonical_ids.get(&edge.consumer)?,
                            edge_kind_key(edge.kind),
                        ))
                    })
                    .collect::<Vec<_>>();
                dependencies.sort_unstable();
                builder.write_u64(dependencies.len() as u64);
                for (producer, consumer, (kind, fingerprint)) in dependencies {
                    builder.write_u64(producer);
                    builder.write_u64(consumer);
                    builder.write_u64(kind);
                    if let Some(fingerprint) = fingerprint {
                        builder.write_fingerprint(fingerprint);
                    }
                }
            } else {
                builder.write_u64(0);
            }
        }

        let mut edges = self
            .edges
            .iter()
            .filter_map(|edge| {
                Some((
                    *canonical_ids.get(&edge.producer)?,
                    *canonical_ids.get(&edge.consumer)?,
                    edge.kind,
                ))
            })
            .collect::<Vec<_>>();
        edges.sort_unstable_by_key(|(producer, consumer, kind)| {
            (*consumer, *producer, edge_kind_key(*kind))
        });
        builder.write_u64(edges.len() as u64);
        for (producer, consumer, kind) in edges {
            builder.write_u64(producer);
            builder.write_u64(consumer);
            let (tag, fingerprint) = edge_kind_key(kind);
            builder.write_u64(tag);
            if let Some(fingerprint) = fingerprint {
                builder.write_fingerprint(fingerprint);
            }
        }
        write_hashed(&mut builder, b"plan-dependencies", &self.dependencies);
        Ok(builder.finish())
    }

    fn canonical_postorder(&self) -> Result<Vec<PhysicalPlanNodeId>, PhysicalIdentityError> {
        if self.root == PhysicalPlanNodeId::INVALID || self.nodes.get(self.root).is_none() {
            return Err(PhysicalIdentityError::InvalidRoot);
        }
        if self.edges.iter().any(|edge| {
            edge.producer == PhysicalPlanNodeId::INVALID
                || edge.consumer == PhysicalPlanNodeId::INVALID
                || self.nodes.get(edge.producer).is_none()
                || self.nodes.get(edge.consumer).is_none()
        }) {
            return Err(PhysicalIdentityError::InvalidEdge);
        }
        let mut order = Vec::new();
        let mut visited = BTreeSet::new();
        let mut visiting = BTreeSet::new();
        let mut stack = vec![(self.root, false)];
        while let Some((id, expanded)) = stack.pop() {
            if id == PhysicalPlanNodeId::INVALID || self.nodes.get(id).is_none() {
                return Err(PhysicalIdentityError::InvalidChild);
            }
            if expanded {
                visiting.remove(&id);
                order.push(id);
                continue;
            }
            if visiting.contains(&id) {
                return Err(PhysicalIdentityError::Cycle);
            }
            if !visited.insert(id) {
                continue;
            }
            visiting.insert(id);
            stack.push((id, true));
            let node = self.node(id);
            if self.child_ids(&node.children).iter().any(|child| {
                *child == PhysicalPlanNodeId::INVALID || self.nodes.get(*child).is_none()
            }) {
                return Err(PhysicalIdentityError::InvalidChild);
            }
            for child in self.child_ids(&node.children).iter().rev() {
                stack.push((*child, false));
            }
            let mut producers = self
                .edges
                .iter()
                .filter(|edge| edge.consumer == id)
                .map(|edge| {
                    Ok((
                        edge_kind_key(edge.kind),
                        self.local_identity_key(edge.producer)?,
                        edge.producer,
                    ))
                })
                .collect::<Result<Vec<_>, PhysicalIdentityError>>()?;
            // Do not use the arena id as a tie breaker. Equal typed payloads
            // are interchangeable; an arena id would make otherwise equal
            // plans differ across extraction runs.
            producers.sort_unstable_by_key(|(kind, local, _)| (*kind, *local));
            if producers
                .windows(2)
                .any(|pair| pair[0].0 == pair[1].0 && pair[0].1 == pair[1].1)
            {
                return Err(PhysicalIdentityError::AmbiguousAuxiliaryOrder);
            }
            for (_, _, producer) in producers.into_iter().rev() {
                stack.push((producer, false));
            }
        }
        if !visiting.is_empty() {
            return Err(PhysicalIdentityError::Cycle);
        }
        Ok(order)
    }

    fn local_identity_key(
        &self,
        id: PhysicalPlanNodeId,
    ) -> Result<Fingerprint, PhysicalIdentityError> {
        let node = self.node(id);
        let mut builder = StableFingerprintBuilder::default();
        builder.write_bytes(node.kind.name().as_bytes());
        write_canonical_kind(self, id, &mut builder)?;
        write_row_type(&mut builder, &node.output);
        Ok(builder.finish())
    }
}

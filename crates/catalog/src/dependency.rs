// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::entry::{CatalogObjectId, CatalogObjectRef, DependencyList, DependencyType};
use paro_common::error::{self as paro_error, Result};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::RwLock;

pub trait DependencyExtractor {
    fn extract_dependencies(&self) -> DependencyList;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DependencyEdgeKey {
    pub dependent_id: CatalogObjectId,
    pub subject_id: CatalogObjectId,
    pub dependency_type: DependencyType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DependencyEdge {
    pub dependent_id: CatalogObjectId,
    pub subject_id: CatalogObjectId,
    pub dependency_type: DependencyType,
}

impl DependencyEdge {
    pub fn new(
        dependent_id: CatalogObjectId,
        subject_id: CatalogObjectId,
        dependency_type: DependencyType,
    ) -> Self {
        Self {
            dependent_id,
            subject_id,
            dependency_type,
        }
    }

    pub fn key(&self) -> DependencyEdgeKey {
        DependencyEdgeKey {
            dependent_id: self.dependent_id,
            subject_id: self.subject_id,
            dependency_type: self.dependency_type,
        }
    }
}

impl From<DependencyEdgeKey> for DependencyEdge {
    fn from(value: DependencyEdgeKey) -> Self {
        Self::new(value.dependent_id, value.subject_id, value.dependency_type)
    }
}

#[derive(Debug, Clone, Default)]
pub struct DependencyDelta {
    pub added_edges: Vec<DependencyEdge>,
    pub removed_edges: Vec<DependencyEdgeKey>,
    pub ref_updates: Vec<CatalogObjectRef>,
    pub removed_objects: Vec<CatalogObjectId>,
}

impl DependencyDelta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.added_edges.is_empty()
            && self.removed_edges.is_empty()
            && self.ref_updates.is_empty()
            && self.removed_objects.is_empty()
    }

    pub fn add_object(&mut self, object_ref: CatalogObjectRef) {
        self.ref_updates.push(object_ref);
    }

    pub fn add_dependency(
        &mut self,
        dependent_id: CatalogObjectId,
        subject_id: CatalogObjectId,
        dependency_type: DependencyType,
    ) {
        self.added_edges.push(DependencyEdge::new(
            dependent_id,
            subject_id,
            dependency_type,
        ));
    }

    pub fn add_dependencies(
        &mut self,
        dependent_id: CatalogObjectId,
        dependencies: &DependencyList,
    ) {
        for dependency in dependencies.iter() {
            self.add_dependency(
                dependent_id,
                dependency.entry.id,
                dependency.dependency_type,
            );
        }
    }

    pub fn remove_object(&mut self, object_id: CatalogObjectId) {
        self.removed_objects.push(object_id);
    }

    pub fn remove_edge(&mut self, edge: DependencyEdgeKey) {
        self.removed_edges.push(edge);
    }

    pub fn publish(self, graph: &DependencyGraph) -> Result<()> {
        graph.apply_delta(self)
    }

    pub fn discard(self) {}
}

#[derive(Debug, Default)]
pub struct DependencyGraph {
    refs: RwLock<HashMap<CatalogObjectId, CatalogObjectRef>>,
    outgoing: RwLock<HashMap<CatalogObjectId, BTreeSet<DependencyEdgeKey>>>,
    incoming: RwLock<HashMap<CatalogObjectId, BTreeSet<DependencyEdgeKey>>>,
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn object_ids(&self) -> Vec<CatalogObjectId> {
        let mut ids = self
            .refs
            .read()
            .map(|refs| refs.keys().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        ids.sort_unstable();
        ids
    }

    pub fn contains_object(&self, object_id: CatalogObjectId) -> bool {
        self.refs
            .read()
            .map(|refs| refs.contains_key(&object_id))
            .unwrap_or(false)
    }

    pub fn object_ref(&self, object_id: CatalogObjectId) -> Option<CatalogObjectRef> {
        self.refs
            .read()
            .ok()
            .and_then(|refs| refs.get(&object_id).cloned())
    }

    pub fn edge_count(&self) -> usize {
        self.outgoing
            .read()
            .map(|outgoing| outgoing.values().map(BTreeSet::len).sum())
            .unwrap_or(0)
    }

    pub fn dependents_of(&self, subject_id: CatalogObjectId) -> Vec<DependencyEdge> {
        let incoming = self.incoming.read().unwrap();
        let mut edges = incoming
            .get(&subject_id)
            .into_iter()
            .flat_map(|edges| edges.iter().copied())
            .map(DependencyEdge::from)
            .collect::<Vec<_>>();
        edges.sort_by_key(|edge| (edge.dependent_id, edge.subject_id, edge.dependency_type));
        edges
    }

    pub fn incident_edges_of(&self, object_id: CatalogObjectId) -> Vec<DependencyEdgeKey> {
        let outgoing = self.outgoing.read().unwrap();
        let incoming = self.incoming.read().unwrap();
        let mut edges = BTreeSet::new();
        if let Some(outgoing_edges) = outgoing.get(&object_id) {
            edges.extend(outgoing_edges.iter().copied());
        }
        if let Some(incoming_edges) = incoming.get(&object_id) {
            edges.extend(incoming_edges.iter().copied());
        }
        edges.into_iter().collect()
    }

    pub fn drop_delta(&self, object_id: CatalogObjectId) -> DependencyDelta {
        let mut delta = DependencyDelta::new();
        for edge in self.incident_edges_of(object_id) {
            delta.remove_edge(edge);
        }
        delta.remove_object(object_id);
        delta
    }

    pub fn overwrite_with(&self, other: &DependencyGraph) {
        if let (Ok(mut refs), Ok(other_refs)) = (self.refs.write(), other.refs.read()) {
            *refs = other_refs.clone();
        }
        if let (Ok(mut outgoing), Ok(other_outgoing)) =
            (self.outgoing.write(), other.outgoing.read())
        {
            *outgoing = other_outgoing.clone();
        }
        if let (Ok(mut incoming), Ok(other_incoming)) =
            (self.incoming.write(), other.incoming.read())
        {
            *incoming = other_incoming.clone();
        }
    }

    pub fn plan_drop(
        &self,
        root_id: CatalogObjectId,
        cascade: bool,
    ) -> Result<Vec<CatalogObjectRef>> {
        let refs = self
            .refs
            .read()
            .map_err(|_| paro_error::internal("dependency graph poisoned"))?
            .clone();
        let incoming = self
            .incoming
            .read()
            .map_err(|_| paro_error::internal("dependency graph poisoned"))?
            .clone();

        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut visiting = HashSet::new();
        self.plan_drop_recursive(
            root_id,
            cascade,
            &refs,
            &incoming,
            &mut visited,
            &mut visiting,
            &mut result,
        )?;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_drop_recursive(
        &self,
        object_id: CatalogObjectId,
        cascade: bool,
        refs: &HashMap<CatalogObjectId, CatalogObjectRef>,
        incoming: &HashMap<CatalogObjectId, BTreeSet<DependencyEdgeKey>>,
        visited: &mut HashSet<CatalogObjectId>,
        visiting: &mut HashSet<CatalogObjectId>,
        result: &mut Vec<CatalogObjectRef>,
    ) -> Result<()> {
        if !visiting.insert(object_id) {
            return Ok(());
        }

        let mut dependents = incoming
            .get(&object_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        dependents.sort();
        for edge in dependents {
            if visited.contains(&edge.dependent_id) {
                continue;
            }
            let can_drop = matches!(edge.dependency_type, DependencyType::Automatic) || cascade;
            if !can_drop {
                let subject = refs
                    .get(&edge.subject_id)
                    .cloned()
                    .unwrap_or_else(|| unknown_object_ref(edge.subject_id));
                let dependent = refs
                    .get(&edge.dependent_id)
                    .cloned()
                    .unwrap_or_else(|| unknown_object_ref(edge.dependent_id));
                return Err(paro_error::dependent_objects_still_exist(format!(
                    "cannot drop {} \"{}\": {} \"{}\" depends on it",
                    subject.kind.as_str().to_ascii_lowercase(),
                    subject.display_name(),
                    dependent.kind.as_str().to_ascii_lowercase(),
                    dependent.display_name()
                )));
            }

            self.plan_drop_recursive(
                edge.dependent_id,
                cascade,
                refs,
                incoming,
                visited,
                visiting,
                result,
            )?;
        }

        visiting.remove(&object_id);
        if visited.insert(object_id) {
            result.push(
                refs.get(&object_id)
                    .cloned()
                    .unwrap_or_else(|| unknown_object_ref(object_id)),
            );
        }
        Ok(())
    }

    fn apply_delta(&self, delta: DependencyDelta) -> Result<()> {
        let mut refs = self
            .refs
            .write()
            .map_err(|_| paro_error::internal("dependency graph poisoned"))?;
        let mut outgoing = self
            .outgoing
            .write()
            .map_err(|_| paro_error::internal("dependency graph poisoned"))?;
        let mut incoming = self
            .incoming
            .write()
            .map_err(|_| paro_error::internal("dependency graph poisoned"))?;

        for object_ref in delta.ref_updates {
            refs.insert(object_ref.id, object_ref);
        }

        for edge in delta.added_edges {
            let key = edge.key();
            outgoing.entry(key.dependent_id).or_default().insert(key);
            incoming.entry(key.subject_id).or_default().insert(key);
        }

        for edge in delta.removed_edges {
            if let Some(edges) = outgoing.get_mut(&edge.dependent_id) {
                edges.remove(&edge);
                if edges.is_empty() {
                    outgoing.remove(&edge.dependent_id);
                }
            }
            if let Some(edges) = incoming.get_mut(&edge.subject_id) {
                edges.remove(&edge);
                if edges.is_empty() {
                    incoming.remove(&edge.subject_id);
                }
            }
        }

        for object_id in delta.removed_objects {
            refs.remove(&object_id);
            outgoing.remove(&object_id);
            incoming.remove(&object_id);
        }

        Ok(())
    }
}

fn unknown_object_ref(id: CatalogObjectId) -> CatalogObjectRef {
    CatalogObjectRef::new(
        id,
        crate::entry::CatalogType::Invalid,
        String::new(),
        None,
        None,
        format!("object_{}", id.raw()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{CatalogType, DependencyType};

    fn schema_ref(id: u64, name: &str) -> CatalogObjectRef {
        CatalogObjectRef::schema(
            CatalogObjectId::from_raw(id),
            "main".to_string(),
            name.to_string(),
        )
    }

    fn table_ref(id: u64, schema_id: u64, name: &str) -> CatalogObjectRef {
        CatalogObjectRef::in_schema(
            CatalogObjectId::from_raw(id),
            CatalogType::Table,
            "main".to_string(),
            Some(CatalogObjectId::from_raw(schema_id)),
            "public".to_string(),
            name.to_string(),
        )
    }

    fn view_ref(id: u64, schema_id: u64, name: &str) -> CatalogObjectRef {
        CatalogObjectRef::in_schema(
            CatalogObjectId::from_raw(id),
            CatalogType::View,
            "main".to_string(),
            Some(CatalogObjectId::from_raw(schema_id)),
            "public".to_string(),
            name.to_string(),
        )
    }

    fn index_ref(id: u64, schema_id: u64, name: &str) -> CatalogObjectRef {
        CatalogObjectRef::in_schema(
            CatalogObjectId::from_raw(id),
            CatalogType::Index,
            "main".to_string(),
            Some(CatalogObjectId::from_raw(schema_id)),
            "public".to_string(),
            name.to_string(),
        )
    }

    #[test]
    fn automatic_dependencies_drop_without_cascade() {
        let graph = DependencyGraph::new();
        let mut delta = DependencyDelta::new();
        delta.add_object(schema_ref(1, "public"));
        delta.add_object(table_ref(10, 1, "users"));
        delta.add_object(index_ref(11, 1, "users_idx"));
        delta.add_dependency(
            CatalogObjectId::from_raw(10),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        delta.add_dependency(
            CatalogObjectId::from_raw(11),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        delta.add_dependency(
            CatalogObjectId::from_raw(11),
            CatalogObjectId::from_raw(10),
            DependencyType::Automatic,
        );
        delta.publish(&graph).unwrap();

        let planned = graph
            .plan_drop(CatalogObjectId::from_raw(10), false)
            .unwrap();
        assert_eq!(
            planned
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["users_idx", "users"]
        );
    }

    #[test]
    fn regular_dependencies_require_cascade() {
        let graph = DependencyGraph::new();
        let mut delta = DependencyDelta::new();
        delta.add_object(schema_ref(1, "public"));
        delta.add_object(table_ref(10, 1, "users"));
        delta.add_object(view_ref(12, 1, "users_view"));
        delta.add_dependency(
            CatalogObjectId::from_raw(10),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        delta.add_dependency(
            CatalogObjectId::from_raw(12),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        delta.add_dependency(
            CatalogObjectId::from_raw(12),
            CatalogObjectId::from_raw(10),
            DependencyType::Regular,
        );
        delta.publish(&graph).unwrap();

        let error = graph
            .plan_drop(CatalogObjectId::from_raw(10), false)
            .unwrap_err();
        assert!(error.to_string().contains("users_view"));

        let planned = graph
            .plan_drop(CatalogObjectId::from_raw(10), true)
            .unwrap();
        assert_eq!(
            planned
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["users_view", "users"]
        );
    }

    #[test]
    fn ownership_dependencies_require_cascade() {
        let graph = DependencyGraph::new();
        let mut delta = DependencyDelta::new();
        delta.add_object(schema_ref(1, "public"));
        delta.add_object(table_ref(10, 1, "users"));
        delta.add_dependency(
            CatalogObjectId::from_raw(10),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        delta.publish(&graph).unwrap();

        let error = graph
            .plan_drop(CatalogObjectId::from_raw(1), false)
            .unwrap_err();
        assert!(error.to_string().contains("users"));

        let planned = graph.plan_drop(CatalogObjectId::from_raw(1), true).unwrap();
        assert_eq!(
            planned
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["users", "public"]
        );
    }

    #[test]
    fn delta_publish_and_drop_delta_roundtrip() {
        let graph = DependencyGraph::new();
        let mut delta = DependencyDelta::new();
        delta.add_object(schema_ref(1, "public"));
        delta.add_object(table_ref(10, 1, "users"));
        delta.add_dependency(
            CatalogObjectId::from_raw(10),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        delta.publish(&graph).unwrap();
        assert!(graph.contains_object(CatalogObjectId::from_raw(10)));
        assert_eq!(graph.edge_count(), 1);

        graph
            .drop_delta(CatalogObjectId::from_raw(10))
            .publish(&graph)
            .unwrap();
        assert!(!graph.contains_object(CatalogObjectId::from_raw(10)));
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn rename_and_move_keep_dependency_identity_stable() {
        let graph = DependencyGraph::new();
        let mut delta = DependencyDelta::new();
        delta.add_object(schema_ref(1, "public"));
        delta.add_object(schema_ref(2, "analytics"));
        delta.add_object(table_ref(10, 1, "users"));
        delta.add_object(view_ref(12, 1, "users_view"));
        delta.add_dependency(
            CatalogObjectId::from_raw(10),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        delta.add_dependency(
            CatalogObjectId::from_raw(12),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        delta.add_dependency(
            CatalogObjectId::from_raw(12),
            CatalogObjectId::from_raw(10),
            DependencyType::Regular,
        );
        delta.publish(&graph).unwrap();

        let mut rename = DependencyDelta::new();
        rename.add_object(CatalogObjectRef::in_schema(
            CatalogObjectId::from_raw(10),
            CatalogType::Table,
            "main".to_string(),
            Some(CatalogObjectId::from_raw(2)),
            "analytics".to_string(),
            "users_archive".to_string(),
        ));
        rename.remove_edge(DependencyEdgeKey {
            dependent_id: CatalogObjectId::from_raw(10),
            subject_id: CatalogObjectId::from_raw(1),
            dependency_type: DependencyType::OwnedBy,
        });
        rename.add_dependency(
            CatalogObjectId::from_raw(10),
            CatalogObjectId::from_raw(2),
            DependencyType::OwnedBy,
        );
        rename.add_object(CatalogObjectRef::in_schema(
            CatalogObjectId::from_raw(12),
            CatalogType::View,
            "main".to_string(),
            Some(CatalogObjectId::from_raw(2)),
            "analytics".to_string(),
            "users_archive_view".to_string(),
        ));
        rename.remove_edge(DependencyEdgeKey {
            dependent_id: CatalogObjectId::from_raw(12),
            subject_id: CatalogObjectId::from_raw(1),
            dependency_type: DependencyType::OwnedBy,
        });
        rename.add_dependency(
            CatalogObjectId::from_raw(12),
            CatalogObjectId::from_raw(2),
            DependencyType::OwnedBy,
        );
        rename.publish(&graph).unwrap();

        let renamed_table = graph.object_ref(CatalogObjectId::from_raw(10)).unwrap();
        assert_eq!(renamed_table.schema_name.as_deref(), Some("analytics"));
        assert_eq!(renamed_table.name, "users_archive");

        let renamed_view = graph.object_ref(CatalogObjectId::from_raw(12)).unwrap();
        assert_eq!(renamed_view.schema_name.as_deref(), Some("analytics"));
        assert_eq!(renamed_view.name, "users_archive_view");

        let error = graph
            .plan_drop(CatalogObjectId::from_raw(10), false)
            .unwrap_err();
        assert!(error.to_string().contains("users_archive_view"));

        let planned = graph.plan_drop(CatalogObjectId::from_raw(2), true).unwrap();
        assert_eq!(
            planned
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["users_archive_view", "users_archive", "analytics"]
        );
    }

    #[test]
    fn discard_delta_leaves_graph_unchanged() {
        let graph = DependencyGraph::new();
        let mut delta = DependencyDelta::new();
        delta.add_object(schema_ref(1, "public"));
        delta.add_object(table_ref(10, 1, "users"));
        delta.add_dependency(
            CatalogObjectId::from_raw(10),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        delta.publish(&graph).unwrap();

        let mut staged = DependencyDelta::new();
        staged.add_object(index_ref(11, 1, "users_idx"));
        staged.add_dependency(
            CatalogObjectId::from_raw(11),
            CatalogObjectId::from_raw(1),
            DependencyType::OwnedBy,
        );
        staged.add_dependency(
            CatalogObjectId::from_raw(11),
            CatalogObjectId::from_raw(10),
            DependencyType::Automatic,
        );
        staged.discard();

        assert!(graph.contains_object(CatalogObjectId::from_raw(10)));
        assert!(!graph.contains_object(CatalogObjectId::from_raw(11)));
        assert_eq!(graph.edge_count(), 1);
    }
}

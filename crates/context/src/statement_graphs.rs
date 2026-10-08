// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Statement-owned graph generations shared by planning and every runtime
//! operator. Publication changes future statements, never an in-flight read.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use paro_common::identity::GraphId;
use paro_storage::index::graph::GraphReadSnapshot;

use crate::GraphIndexProvider;

#[derive(Debug, Clone, Default)]
pub struct StatementGraphSnapshots {
    snapshots: Arc<Mutex<HashMap<GraphId, GraphReadSnapshot>>>,
}

impl StatementGraphSnapshots {
    pub(crate) fn read(
        &self,
        provider: &dyn GraphIndexProvider,
        id: &GraphId,
    ) -> Option<GraphReadSnapshot> {
        let mut snapshots = self
            .snapshots
            .lock()
            .expect("statement graph snapshot mutex poisoned");
        if let Some(snapshot) = snapshots.get(id) {
            return Some(snapshot.clone());
        }
        // Serialize first acquisition as well as publication to this map:
        // concurrent pipeline initialization must not acquire two generations.
        // Absence is not a generation and does not create a pin.
        let snapshot = provider.snapshot(id)?;
        snapshots.insert(id.clone(), snapshot.clone());
        Some(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_storage::index::graph::{
        GraphBuildInput, GraphManifest, GraphProjectionIndex, GraphRuntimeHandle, GraphState,
        GraphStorageGeneration,
    };

    struct Provider(GraphRuntimeHandle);

    impl GraphIndexProvider for Provider {
        fn snapshot(&self, _id: &GraphId) -> Option<GraphReadSnapshot> {
            Some(self.0.snapshot())
        }
    }

    fn generation(id: u64) -> GraphStorageGeneration {
        let index = GraphProjectionIndex::build(&GraphBuildInput {
            graph_name: "g".into(),
            vertex_tables: vec![],
            edge_tables: vec![],
            build_backward_adjacency: true,
        })
        .unwrap();
        GraphStorageGeneration::from_index(
            index,
            GraphManifest::new("g".into(), GraphState::Ready, "schema".into()),
            id,
        )
    }

    #[test]
    fn compilation_admission_and_operators_share_a_generation_until_statement_end() {
        let provider = Provider(GraphRuntimeHandle::new(generation(1)));
        let graph = GraphId::new("db", "public", "g");
        let statement = StatementGraphSnapshots::default();
        let compilation = statement.read(&provider, &graph).unwrap();
        let previous = Arc::downgrade(compilation.generation());
        provider.0.publish(generation(2));
        let execution = statement.clone();
        for _ in 0..3 {
            let snapshot = execution.read(&provider, &graph).unwrap();
            assert!(Arc::ptr_eq(snapshot.generation(), compilation.generation()));
        }
        let next_statement = StatementGraphSnapshots::default();
        assert_eq!(
            next_statement
                .read(&provider, &graph)
                .unwrap()
                .generation_id(),
            2
        );
        drop(compilation);
        drop(statement);
        assert!(previous.upgrade().is_some());
        drop(execution);
        assert!(
            previous.upgrade().is_none(),
            "pins must not outlive their statement"
        );
    }

    #[test]
    fn graph_identity_separates_statement_pins() {
        let provider = Provider(GraphRuntimeHandle::new(generation(1)));
        let first = GraphId::new("db", "public", "a");
        let second = GraphId::new("db", "public", "b");
        let statement = StatementGraphSnapshots::default();
        assert_eq!(
            statement.read(&provider, &first).unwrap().generation_id(),
            1
        );
        provider.0.publish(generation(2));
        assert_eq!(
            statement.read(&provider, &second).unwrap().generation_id(),
            2
        );
        assert_eq!(
            statement.read(&provider, &first).unwrap().generation_id(),
            1
        );
    }
}

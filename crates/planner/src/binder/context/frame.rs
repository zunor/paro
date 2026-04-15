use crate::binder::ir::CTEBindState;
use paro_common::types::LogicalType;
use std::collections::HashMap;
use std::sync::Arc;

/// A binding represents a table or subquery available in the current scope.
#[derive(Debug, Clone)]
pub struct Binding {
    /// The alias or table name used to refer to this binding.
    pub alias: String,
    /// Unique index assigned to this binding for column resolution.
    pub index: usize,
    /// The names of the columns in this binding.
    pub column_names: Vec<String>,
    /// The types of the columns in this binding.
    pub column_types: Vec<LogicalType>,
}

/// Binding and CTE relations that belong to one lexical scope frame.
#[derive(Debug, Clone, Default)]
struct ScopeRelations {
    by_alias: HashMap<String, Binding>,
}

impl ScopeRelations {
    fn values(&self) -> impl Iterator<Item = &Binding> {
        self.by_alias.values()
    }

    fn iter(&self) -> impl Iterator<Item = (&String, &Binding)> {
        self.by_alias.iter()
    }

    fn get(&self, alias: &str) -> Option<&Binding> {
        self.by_alias.get(alias)
    }

    fn insert(&mut self, binding: Binding) {
        self.by_alias.insert(binding.alias.clone(), binding);
    }

    fn remove(&mut self, alias: &str) -> Option<Binding> {
        self.by_alias.remove(alias)
    }

    fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&String, &mut Binding) -> bool,
    {
        self.by_alias.retain(f);
    }
}

/// Mutable current-scope frame for a binder.
#[derive(Debug, Clone, Default)]
pub struct ScopeFrame {
    relations: Arc<ScopeRelations>,
    ctes: Arc<HashMap<String, Arc<CTEBindState>>>,
}

impl ScopeFrame {
    pub(crate) fn bindings(&self) -> impl Iterator<Item = &Binding> {
        self.relations.values()
    }

    pub(crate) fn binding_entries(&self) -> impl Iterator<Item = (&String, &Binding)> {
        self.relations.iter()
    }

    pub(crate) fn get_binding(&self, alias: &str) -> Option<&Binding> {
        self.relations.get(alias)
    }

    pub(crate) fn upsert_binding(&mut self, binding: Binding) {
        Arc::make_mut(&mut self.relations).insert(binding);
    }

    pub(crate) fn remove_binding(&mut self, alias: &str) -> Option<Binding> {
        Arc::make_mut(&mut self.relations).remove(alias)
    }

    pub(crate) fn retain_bindings<F>(&mut self, f: F)
    where
        F: FnMut(&String, &mut Binding) -> bool,
    {
        Arc::make_mut(&mut self.relations).retain(f);
    }

    pub(crate) fn cte_entries(&self) -> impl Iterator<Item = (&String, &Arc<CTEBindState>)> {
        self.ctes.iter()
    }

    pub(crate) fn get_cte(&self, name: &str) -> Option<&Arc<CTEBindState>> {
        self.ctes.get(name)
    }

    pub(crate) fn insert_cte(&mut self, name: String, state: Arc<CTEBindState>) {
        Arc::make_mut(&mut self.ctes).insert(name, state);
    }
}

/// Type of attached database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum DatabaseType {
    /// Normal read-write database.
    #[default]
    ReadWrite,
    /// Read-only database (cannot be modified).
    ReadOnly,
    /// System database (internal, stores system catalog).
    System,
    /// Temporary database (in-memory, session-scoped).
    Temp,
}

impl DatabaseType {
    /// Check if this is a system database.
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System)
    }

    /// Check if this is a temporary database.
    pub fn is_temporary(&self) -> bool {
        matches!(self, Self::Temp)
    }

    /// Check if this is a read-only database.
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

/// Fully-qualified identity of a property graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraphId {
    pub catalog: String,
    pub schema: String,
    pub graph_name: String,
}

impl GraphId {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        graph_name: impl Into<String>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            graph_name: graph_name.into(),
        }
    }

    pub fn runtime_key(&self) -> String {
        format!("{}\x1f{}\x1f{}", self.catalog, self.schema, self.graph_name)
    }
}

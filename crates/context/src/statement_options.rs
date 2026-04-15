#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExplainOutputType {
    All,
    Optimized,
    #[default]
    PhysicalOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StatementSource {
    #[default]
    SimpleQuery,
    PreparedSql,
    ExtendedQuery,
    Internal,
}

#[derive(Debug, Clone, Default)]
pub struct StatementOptions {
    pub statement_format: Option<String>,
    pub explain_output: Option<ExplainOutputType>,
    pub source: StatementSource,
}

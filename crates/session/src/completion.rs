//! Statement completion metadata used by the session/front-end layer.

use paro_parser::ast::DiscardTarget;

/// DISCARDS that should surface as distinct completion semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardCommand {
    All,
    Plans,
    Temp,
    Sequences,
}

impl From<DiscardTarget> for DiscardCommand {
    fn from(value: DiscardTarget) -> Self {
        match value {
            DiscardTarget::All => Self::All,
            DiscardTarget::Plans => Self::Plans,
            DiscardTarget::Temp => Self::Temp,
            DiscardTarget::Sequences => Self::Sequences,
        }
    }
}

impl DiscardCommand {
    fn to_command_complete(self) -> &'static str {
        match self {
            Self::All => "DISCARD ALL",
            Self::Plans => "DISCARD PLANS",
            Self::Temp => "DISCARD TEMP",
            Self::Sequences => "DISCARD SEQUENCES",
        }
    }
}

/// Session/front-end level completion semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementCompletion {
    Select { rows: usize },
    Insert { rows: usize },
    Update { rows: usize },
    Delete { rows: usize },
    Merge { rows: usize },
    Copy { rows: usize },
    Explain,
    Prepare,
    Execute,
    Deallocate { all: bool },
    DeclareCursor,
    Fetch { rows: usize },
    Move { rows: usize },
    CloseCursor { all: bool },
    Begin,
    StartTransaction,
    Commit,
    Rollback,
    Savepoint,
    Release,
    RollbackTo,
    Set,
    Reset,
    Show,
    SetConstraints,
    Discard(DiscardCommand),
    Checkpoint,
    CreateDatabase,
    DropDatabase,
    Empty,
    Custom(String),
}

impl StatementCompletion {
    /// Rows carried explicitly by the completion payload, if any.
    pub fn row_count(&self) -> Option<usize> {
        match self {
            Self::Select { rows }
            | Self::Insert { rows }
            | Self::Update { rows }
            | Self::Delete { rows }
            | Self::Merge { rows }
            | Self::Copy { rows }
            | Self::Fetch { rows }
            | Self::Move { rows } => Some(*rows),
            _ => None,
        }
    }

    pub fn is_transaction_control(&self) -> bool {
        matches!(
            self,
            Self::Begin
                | Self::StartTransaction
                | Self::Commit
                | Self::Rollback
                | Self::Savepoint
                | Self::Release
                | Self::RollbackTo
        )
    }

    /// Render to PostgreSQL `CommandComplete` payload text.
    pub fn to_command_complete(&self) -> String {
        match self {
            Self::Select { rows } => format!("SELECT {rows}"),
            Self::Insert { rows } => format!("INSERT 0 {rows}"),
            Self::Update { rows } => format!("UPDATE {rows}"),
            Self::Delete { rows } => format!("DELETE {rows}"),
            Self::Merge { rows } => format!("MERGE {rows}"),
            Self::Copy { rows } => format!("COPY {rows}"),
            Self::Explain => "EXPLAIN".to_string(),
            Self::Prepare => "PREPARE".to_string(),
            Self::Execute => "EXECUTE".to_string(),
            Self::Deallocate { all } => {
                if *all {
                    "DEALLOCATE ALL".to_string()
                } else {
                    "DEALLOCATE".to_string()
                }
            }
            Self::DeclareCursor => "DECLARE CURSOR".to_string(),
            Self::Fetch { rows } => format!("FETCH {rows}"),
            Self::Move { rows } => format!("MOVE {rows}"),
            Self::CloseCursor { all } => {
                if *all {
                    "CLOSE ALL".to_string()
                } else {
                    "CLOSE CURSOR".to_string()
                }
            }
            Self::Begin => "BEGIN".to_string(),
            Self::StartTransaction => "START TRANSACTION".to_string(),
            Self::Commit => "COMMIT".to_string(),
            Self::Rollback => "ROLLBACK".to_string(),
            Self::Savepoint => "SAVEPOINT".to_string(),
            Self::Release => "RELEASE".to_string(),
            Self::RollbackTo => "ROLLBACK TO".to_string(),
            Self::Set => "SET".to_string(),
            Self::Reset => "RESET".to_string(),
            Self::Show => "SHOW".to_string(),
            Self::SetConstraints => "SET CONSTRAINTS".to_string(),
            Self::Discard(target) => target.to_command_complete().to_string(),
            Self::Checkpoint => "CHECKPOINT".to_string(),
            Self::CreateDatabase => "CREATE DATABASE".to_string(),
            Self::DropDatabase => "DROP DATABASE".to_string(),
            Self::Empty => String::new(),
            Self::Custom(value) => value.clone(),
        }
    }
}

impl std::fmt::Display for StatementCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_command_complete())
    }
}

#[cfg(test)]
mod tests {
    use super::{DiscardCommand, StatementCompletion};

    #[test]
    fn renders_row_count_completions() {
        assert_eq!(
            StatementCompletion::Select { rows: 100 }.to_string(),
            "SELECT 100"
        );
        assert_eq!(
            StatementCompletion::Insert { rows: 5 }.to_string(),
            "INSERT 0 5"
        );
        assert_eq!(
            StatementCompletion::Copy { rows: 12 }.to_string(),
            "COPY 12"
        );
    }

    #[test]
    fn renders_frontend_specific_completions() {
        assert_eq!(StatementCompletion::Prepare.to_string(), "PREPARE");
        assert_eq!(
            StatementCompletion::Deallocate { all: true }.to_string(),
            "DEALLOCATE ALL"
        );
        assert_eq!(
            StatementCompletion::Discard(DiscardCommand::Plans).to_string(),
            "DISCARD PLANS"
        );
    }

    #[test]
    fn exposes_explicit_row_count_only_when_present() {
        assert_eq!(StatementCompletion::Show.row_count(), None);
        assert_eq!(StatementCompletion::Fetch { rows: 7 }.row_count(), Some(7));
    }
}

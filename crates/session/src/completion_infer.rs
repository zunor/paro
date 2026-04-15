//! Infer front-end completion semantics from AST + execution row counts.

use crate::completion::{DiscardCommand, StatementCompletion};
use paro_parser::ast::{Statement, TransactionKind, VariableSetKind};

fn custom(name: &str) -> StatementCompletion {
    StatementCompletion::Custom(name.to_string())
}

/// Infer the completion payload for a statement after execution.
pub fn infer_statement_completion(stmt: &Statement, rows: usize) -> StatementCompletion {
    match stmt {
        Statement::Transaction(stmt) => match &stmt.kind {
            TransactionKind::Begin => StatementCompletion::Begin,
            TransactionKind::Start => StatementCompletion::StartTransaction,
            TransactionKind::Commit => StatementCompletion::Commit,
            TransactionKind::Rollback => StatementCompletion::Rollback,
            TransactionKind::Savepoint(_) => StatementCompletion::Savepoint,
            TransactionKind::ReleaseSavepoint(_) => StatementCompletion::Release,
            TransactionKind::RollbackToSavepoint(_) => StatementCompletion::RollbackTo,
            TransactionKind::PrepareTransaction(_) => custom("PREPARE TRANSACTION"),
            TransactionKind::CommitPrepared(_) => custom("COMMIT PREPARED"),
            TransactionKind::RollbackPrepared(_) => custom("ROLLBACK PREPARED"),
        },
        Statement::VariableSet(stmt) => match stmt.kind {
            VariableSetKind::Set => StatementCompletion::Set,
            VariableSetKind::Reset | VariableSetKind::ResetAll => StatementCompletion::Reset,
        },
        Statement::VariableShow(_) => StatementCompletion::Show,
        Statement::Prepare(_) => StatementCompletion::Prepare,
        Statement::Execute(_) => StatementCompletion::Execute,
        Statement::Deallocate(stmt) => StatementCompletion::Deallocate {
            all: stmt.name.is_none(),
        },
        Statement::DeclareCursor(_) => StatementCompletion::DeclareCursor,
        Statement::Fetch(fetch) => {
            if fetch.ismove {
                StatementCompletion::Move { rows }
            } else {
                StatementCompletion::Fetch { rows }
            }
        }
        Statement::CloseCursor(stmt) => StatementCompletion::CloseCursor {
            all: stmt.name.is_none(),
        },
        Statement::Discard(stmt) => StatementCompletion::Discard(DiscardCommand::from(stmt.target)),
        Statement::Checkpoint(_) => StatementCompletion::Checkpoint,

        Statement::Query(_) => StatementCompletion::Select { rows },
        Statement::Insert(_) | Statement::InsertMultiTable(_) | Statement::Replace(_) => {
            StatementCompletion::Insert { rows }
        }
        Statement::Update(_) => StatementCompletion::Update { rows },
        Statement::Delete(_) => StatementCompletion::Delete { rows },
        Statement::MergeInto(_) => StatementCompletion::Merge { rows },

        Statement::CreateDatabase(_) => StatementCompletion::CreateDatabase,
        Statement::DropDatabase(_) => StatementCompletion::DropDatabase,
        Statement::UseDatabase { .. } => custom("USE"),
        Statement::ConnectTo(_) => custom("CONNECT"),

        Statement::CreateSchema(_) => custom("CREATE SCHEMA"),
        Statement::DropSchema(_) => custom("DROP SCHEMA"),
        Statement::AlterSchema(_) => custom("ALTER SCHEMA"),
        Statement::UndropSchema(_) => custom("UNDROP SCHEMA"),
        Statement::UseSchema { .. } => StatementCompletion::Set,

        Statement::CreateTable(_) | Statement::CreateDynamicTable(_) => custom("CREATE TABLE"),
        Statement::DropTable(_) => custom("DROP TABLE"),
        Statement::AlterTable(_) | Statement::RenameTable(_) => custom("ALTER TABLE"),
        Statement::TruncateTable(_) => custom("TRUNCATE TABLE"),
        Statement::UndropTable(_) => custom("UNDROP TABLE"),
        Statement::AttachTable(_) => custom("ATTACH TABLE"),
        Statement::OptimizeTable(_) => custom("OPTIMIZE TABLE"),
        Statement::VacuumTable(_) => custom("VACUUM TABLE"),
        Statement::VacuumDropTable(_) => custom("VACUUM DROP TABLE"),
        Statement::VacuumTemporaryFiles(_) => custom("VACUUM TEMPORARY FILES"),
        Statement::AnalyzeTable(_) => custom("ANALYZE TABLE"),
        Statement::ExistsTable(_) => StatementCompletion::Show,
        Statement::ShowStatistics(_) => StatementCompletion::Show,

        Statement::CreateView(_) => custom("CREATE VIEW"),
        Statement::DropView(_) => custom("DROP VIEW"),
        Statement::AlterView(_) => custom("ALTER VIEW"),
        Statement::ShowViews(_) | Statement::DescribeView(_) => StatementCompletion::Show,

        Statement::CreateAggregatingIndex(_) | Statement::CreateIndex(_) => custom("CREATE INDEX"),
        Statement::DropIndex(_) | Statement::DropIndexOnTable(_) => custom("DROP INDEX"),
        Statement::RefreshAggregatingIndex(_) | Statement::RefreshIndexOnTable(_) => {
            custom("REFRESH INDEX")
        }
        Statement::CreatePropertyGraph(_) => custom("CREATE PROPERTY GRAPH"),
        Statement::DropPropertyGraph(_) => custom("DROP PROPERTY GRAPH"),
        Statement::RefreshPropertyGraph(_) => custom("REFRESH PROPERTY GRAPH"),

        Statement::CreateStream(_) => custom("CREATE STREAM"),
        Statement::DropStream(_) => custom("DROP STREAM"),
        Statement::ShowStreams(_) | Statement::DescribeStream(_) => StatementCompletion::Show,

        Statement::RefreshVirtualColumn(_) => custom("REFRESH VIRTUAL COLUMN"),
        Statement::ShowVirtualColumns(_) => StatementCompletion::Show,

        Statement::CreateDictionary(_) => custom("CREATE DICTIONARY"),
        Statement::DropDictionary(_) => custom("DROP DICTIONARY"),
        Statement::RenameDictionary(_) => custom("RENAME DICTIONARY"),
        Statement::ShowCreateDictionary(_) | Statement::ShowDictionaries(_) => {
            StatementCompletion::Show
        }

        Statement::ShowColumns(_) => StatementCompletion::Show,

        Statement::CreateSequence(_) => custom("CREATE SEQUENCE"),
        Statement::DropSequence(_) => custom("DROP SEQUENCE"),
        Statement::ShowSequences { .. } | Statement::DescSequence { .. } => {
            StatementCompletion::Show
        }

        Statement::CreateStage(_) => custom("CREATE STAGE"),
        Statement::DropStage { .. } => custom("DROP STAGE"),
        Statement::RemoveStage { .. } => custom("REMOVE STAGE"),
        Statement::ShowStages { .. }
        | Statement::DescribeStage { .. }
        | Statement::ListStage { .. } => StatementCompletion::Show,

        Statement::CreateConnection(_) => custom("CREATE CONNECTION"),
        Statement::DropConnection(_) => custom("DROP CONNECTION"),
        Statement::DescribeConnection(_) | Statement::ShowConnections(_) => {
            StatementCompletion::Show
        }

        Statement::CreateFileFormat { .. } => custom("CREATE FILE FORMAT"),
        Statement::DropFileFormat { .. } => custom("DROP FILE FORMAT"),
        Statement::ShowFileFormats => StatementCompletion::Show,
        Statement::Presign(_) => custom("PRESIGN"),

        Statement::CreateUDF(_) => custom("CREATE FUNCTION"),
        Statement::DropUDF { .. } => custom("DROP FUNCTION"),
        Statement::AlterUDF(_) => custom("ALTER FUNCTION"),

        Statement::CreateUser(_) => custom("CREATE USER"),
        Statement::DropUser { .. } => custom("DROP USER"),
        Statement::AlterUser(_) => custom("ALTER USER"),
        Statement::ShowUsers { .. } | Statement::DescribeUser { .. } => StatementCompletion::Show,

        Statement::CreateRole { .. } => custom("CREATE ROLE"),
        Statement::DropRole { .. } => custom("DROP ROLE"),
        Statement::AlterRole(_) => custom("ALTER ROLE"),
        Statement::Grant(_) => custom("GRANT"),
        Statement::Revoke(_) => custom("REVOKE"),
        Statement::ShowRoles { .. }
        | Statement::ShowGrants { .. }
        | Statement::ShowObjectPrivileges(_)
        | Statement::ShowGrantsOfRole(_) => StatementCompletion::Show,

        Statement::CreateDatamaskPolicy(_) => custom("CREATE DATAMASK POLICY"),
        Statement::DropDatamaskPolicy(_) => custom("DROP DATAMASK POLICY"),
        Statement::DescDatamaskPolicy(_) => StatementCompletion::Show,

        Statement::CreateNetworkPolicy(_) => custom("CREATE NETWORK POLICY"),
        Statement::AlterNetworkPolicy(_) => custom("ALTER NETWORK POLICY"),
        Statement::DropNetworkPolicy(_) => custom("DROP NETWORK POLICY"),
        Statement::DescNetworkPolicy(_) | Statement::ShowNetworkPolicies => {
            StatementCompletion::Show
        }

        Statement::CreatePasswordPolicy(_) => custom("CREATE PASSWORD POLICY"),
        Statement::AlterPasswordPolicy(_) => custom("ALTER PASSWORD POLICY"),
        Statement::DropPasswordPolicy(_) => custom("DROP PASSWORD POLICY"),
        Statement::DescPasswordPolicy(_) | Statement::ShowPasswordPolicies { .. } => {
            StatementCompletion::Show
        }

        Statement::CreateRowAccessPolicy(_) => custom("CREATE ROW ACCESS POLICY"),
        Statement::DropRowAccessPolicy(_) => custom("DROP ROW ACCESS POLICY"),
        Statement::DescRowAccessPolicy(_) => StatementCompletion::Show,

        Statement::CreateTag(_) => custom("CREATE TAG"),
        Statement::DropTag(_) => custom("DROP TAG"),
        Statement::ShowTags(_) => StatementCompletion::Show,

        Statement::CreateTask(_) => custom("CREATE TASK"),
        Statement::AlterTask(_) => custom("ALTER TASK"),
        Statement::DropTask(_) => custom("DROP TASK"),
        Statement::ExecuteTask(_) => custom("EXECUTE TASK"),
        Statement::DescribeTask(_) | Statement::ShowTasks(_) => StatementCompletion::Show,

        Statement::CreatePipe(_) => custom("CREATE PIPE"),
        Statement::DropPipe(_) => custom("DROP PIPE"),
        Statement::AlterPipe(_) => custom("ALTER PIPE"),
        Statement::DescribePipe(_) => StatementCompletion::Show,

        Statement::CreateNotification(_) => custom("CREATE NOTIFICATION"),
        Statement::AlterNotification(_) => custom("ALTER NOTIFICATION"),
        Statement::DropNotification(_) => custom("DROP NOTIFICATION"),
        Statement::DescribeNotification(_) => StatementCompletion::Show,

        Statement::ExecuteImmediate(_) => custom("EXECUTE IMMEDIATE"),
        Statement::CreateProcedure(_) => custom("CREATE PROCEDURE"),
        Statement::DropProcedure(_) => custom("DROP PROCEDURE"),
        Statement::CallProcedure(_) => custom("CALL"),
        Statement::ShowProcedures { .. } | Statement::DescProcedure(_) => StatementCompletion::Show,

        Statement::UseWarehouse(_) => custom("USE WAREHOUSE"),
        Statement::ShowOnlineNodes(_)
        | Statement::ShowWarehouses(_)
        | Statement::InspectWarehouse(_) => StatementCompletion::Show,
        Statement::DropWarehouse(_) => custom("DROP WAREHOUSE"),
        Statement::CreateWarehouse(_) => custom("CREATE WAREHOUSE"),
        Statement::RenameWarehouse(_) => custom("RENAME WAREHOUSE"),
        Statement::ResumeWarehouse(_) => custom("RESUME WAREHOUSE"),
        Statement::SuspendWarehouse(_) => custom("SUSPEND WAREHOUSE"),
        Statement::AddWarehouseCluster(_) => custom("ADD WAREHOUSE CLUSTER"),
        Statement::DropWarehouseCluster(_) => custom("DROP WAREHOUSE CLUSTER"),
        Statement::RenameWarehouseCluster(_) => custom("RENAME WAREHOUSE CLUSTER"),
        Statement::AssignWarehouseNodes(_) => custom("ASSIGN WAREHOUSE NODES"),
        Statement::UnassignWarehouseNodes(_) => custom("UNASSIGN WAREHOUSE NODES"),

        Statement::ShowWorkloadGroups(_) => StatementCompletion::Show,
        Statement::CreateWorkloadGroup(_) => custom("CREATE WORKLOAD GROUP"),
        Statement::DropWorkloadGroup(_) => custom("DROP WORKLOAD GROUP"),
        Statement::RenameWorkloadGroup(_) => custom("RENAME WORKLOAD GROUP"),
        Statement::SetWorkloadQuotasGroup(_) => custom("SET WORKLOAD QUOTAS"),
        Statement::UnsetWorkloadQuotasGroup(_) => custom("UNSET WORKLOAD QUOTAS"),

        Statement::Begin => StatementCompletion::Begin,
        Statement::Commit => StatementCompletion::Commit,
        Statement::Abort => StatementCompletion::Rollback,

        Statement::SetStmt { .. }
        | Statement::SetRole { .. }
        | Statement::SetSecondaryRoles { .. }
        | Statement::SetPriority { .. } => StatementCompletion::Set,
        Statement::UnSetStmt { .. } => StatementCompletion::Reset,

        Statement::ShowSettings { .. }
        | Statement::ShowProcessList { .. }
        | Statement::ShowMetrics { .. }
        | Statement::ShowEngines { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowUserFunctions { .. }
        | Statement::ShowTableFunctions { .. }
        | Statement::ShowIndexes { .. }
        | Statement::ShowLocks(_)
        | Statement::ShowVariables { .. }
        | Statement::ShowDatabases(_)
        | Statement::ShowCreateDatabase(_)
        | Statement::ShowSchemas(_)
        | Statement::ShowDropSchemas(_)
        | Statement::ShowCreateSchema(_)
        | Statement::ShowTables(_)
        | Statement::ShowCreateTable(_)
        | Statement::ShowTablesStatus(_)
        | Statement::ShowDropTables(_)
        | Statement::DescribeTable(_) => StatementCompletion::Show,

        Statement::Explain { .. } | Statement::ExplainAnalyze { .. } => {
            StatementCompletion::Explain
        }
        Statement::Copy(_) => StatementCompletion::Copy { rows },
        Statement::Call(_) => custom("CALL"),
        Statement::KillStmt { .. } => custom("KILL"),
        Statement::System(_) => custom("SYSTEM"),
        Statement::ReportIssue(_) => custom("REPORT ISSUE"),

        Statement::StatementWithSettings { stmt, .. } => infer_statement_completion(stmt, rows),
    }
}

/// Infer the initial completion before execution.
pub fn initial_statement_completion(stmt: &Statement) -> StatementCompletion {
    infer_statement_completion(stmt, 0)
}

#[cfg(test)]
mod tests {
    use super::{infer_statement_completion, initial_statement_completion};
    use crate::completion::{DiscardCommand, StatementCompletion};
    use paro_parser::ast::{
        DiscardStmt, DiscardTarget, ExecuteStmt, FetchDirection, FetchStmt, Identifier, Statement,
    };

    #[test]
    fn infers_frontend_specific_completions() {
        let stmt = Statement::Execute(ExecuteStmt {
            name: Identifier::from_name(None, "my_stmt"),
            args: vec![],
        });
        assert_eq!(
            initial_statement_completion(&stmt),
            StatementCompletion::Execute
        );
    }

    #[test]
    fn infers_move_vs_fetch() {
        let fetch = Statement::Fetch(FetchStmt {
            ismove: true,
            direction: FetchDirection::Count(3),
            cursor: Identifier::from_name(None, "c"),
        });
        assert_eq!(
            infer_statement_completion(&fetch, 3),
            StatementCompletion::Move { rows: 3 }
        );
    }

    #[test]
    fn infers_discard_target() {
        let stmt = Statement::Discard(DiscardStmt {
            target: DiscardTarget::Plans,
        });
        assert_eq!(
            initial_statement_completion(&stmt),
            StatementCompletion::Discard(DiscardCommand::Plans)
        );
    }
}

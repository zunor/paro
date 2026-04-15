// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Classification of parsed statements into query, prepared, and utility routes.

use paro_parser::ast::{
    CheckpointStmt, CloseCursorStmt, CreateDatabaseStmt, DeallocateStmt, DeclareCursorStmt,
    DiscardStmt, DropDatabaseStmt, ExecuteStmt, FetchStmt, PrepareStmt, Statement, TransactionKind,
    TransactionStmt, VariableSetStmt, VariableShowStmt,
};

#[derive(Debug, Clone)]
pub enum PreparedCommand {
    Prepare(PrepareStmt),
    Execute(ExecuteStmt),
    Deallocate(DeallocateStmt),
    DeclareCursor(DeclareCursorStmt),
    Fetch(FetchStmt),
    Move(FetchStmt),
    CloseCursor(CloseCursorStmt),
}

#[derive(Debug, Clone)]
pub enum UtilityCommand {
    Transaction(TransactionStmt),
    VariableSet(VariableSetStmt),
    VariableShow(VariableShowStmt),
    Discard(DiscardStmt),
    Checkpoint(CheckpointStmt),
    CreateDatabase(CreateDatabaseStmt),
    DropDatabase(DropDatabaseStmt),
}

impl UtilityCommand {
    pub fn starts_explicit_transaction(&self) -> bool {
        matches!(
            self,
            Self::Transaction(TransactionStmt {
                kind: TransactionKind::Begin | TransactionKind::Start,
            })
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementClass {
    Query,
    Prepared,
    Utility,
}

#[derive(Debug, Clone)]
pub enum FrontendRoute {
    Query(Box<Statement>),
    Prepared(Box<PreparedCommand>),
    Utility(Box<UtilityCommand>),
}

impl FrontendRoute {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Query(_) => "query",
            Self::Prepared(_) => "prepared",
            Self::Utility(_) => "utility",
        }
    }
}

pub fn classify_statement(stmt: &Statement) -> StatementClass {
    match stmt {
        Statement::Prepare(_)
        | Statement::Execute(_)
        | Statement::Deallocate(_)
        | Statement::DeclareCursor(_)
        | Statement::Fetch(_)
        | Statement::CloseCursor(_) => StatementClass::Prepared,
        Statement::Transaction(_)
        | Statement::VariableSet(_)
        | Statement::VariableShow(_)
        | Statement::Discard(_)
        | Statement::Checkpoint(_)
        | Statement::CreateDatabase(_)
        | Statement::DropDatabase(_)
        | Statement::Begin
        | Statement::Commit
        | Statement::Abort => StatementClass::Utility,
        Statement::Explain { .. }
        | Statement::ExplainAnalyze { .. }
        | Statement::StatementWithSettings { .. } => StatementClass::Query,
        _ => StatementClass::Query,
    }
}

pub fn utility_command_from_statement(stmt: Statement) -> UtilityCommand {
    match stmt {
        Statement::Transaction(stmt) => UtilityCommand::Transaction(stmt),
        Statement::VariableSet(stmt) => UtilityCommand::VariableSet(stmt),
        Statement::VariableShow(stmt) => UtilityCommand::VariableShow(stmt),
        Statement::Discard(stmt) => UtilityCommand::Discard(stmt),
        Statement::Checkpoint(stmt) => UtilityCommand::Checkpoint(stmt),
        Statement::CreateDatabase(stmt) => UtilityCommand::CreateDatabase(stmt),
        Statement::DropDatabase(stmt) => UtilityCommand::DropDatabase(stmt),
        Statement::Begin => UtilityCommand::Transaction(TransactionStmt {
            kind: TransactionKind::Begin,
        }),
        Statement::Commit => UtilityCommand::Transaction(TransactionStmt {
            kind: TransactionKind::Commit,
        }),
        Statement::Abort => UtilityCommand::Transaction(TransactionStmt {
            kind: TransactionKind::Rollback,
        }),
        other => unreachable!("utility extractor received non-utility statement: {other:?}"),
    }
}

/// Classify a parsed statement into the canonical front-end route used by `execute.rs`.
pub fn dispatch_statement(stmt: Statement) -> FrontendRoute {
    match stmt {
        Statement::Prepare(stmt) => {
            FrontendRoute::Prepared(Box::new(PreparedCommand::Prepare(stmt)))
        }
        Statement::Execute(stmt) => {
            FrontendRoute::Prepared(Box::new(PreparedCommand::Execute(stmt)))
        }
        Statement::Deallocate(stmt) => {
            FrontendRoute::Prepared(Box::new(PreparedCommand::Deallocate(stmt)))
        }
        Statement::DeclareCursor(stmt) => {
            FrontendRoute::Prepared(Box::new(PreparedCommand::DeclareCursor(stmt)))
        }
        Statement::Fetch(stmt) => {
            if stmt.ismove {
                FrontendRoute::Prepared(Box::new(PreparedCommand::Move(stmt)))
            } else {
                FrontendRoute::Prepared(Box::new(PreparedCommand::Fetch(stmt)))
            }
        }
        Statement::CloseCursor(stmt) => {
            FrontendRoute::Prepared(Box::new(PreparedCommand::CloseCursor(stmt)))
        }
        stmt if matches!(classify_statement(&stmt), StatementClass::Utility) => {
            FrontendRoute::Utility(Box::new(utility_command_from_statement(stmt)))
        }
        stmt => FrontendRoute::Query(Box::new(stmt)),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        classify_statement, dispatch_statement, utility_command_from_statement, FrontendRoute,
        PreparedCommand, StatementClass, UtilityCommand,
    };
    use paro_parser::ast::{Statement, TransactionKind};

    fn parse_stmt(sql: &str) -> Statement {
        paro_parser::parse(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .stmt
    }

    #[test]
    fn routes_prepare_into_prepared() {
        match dispatch_statement(parse_stmt("PREPARE p AS SELECT 1")) {
            FrontendRoute::Prepared(cmd) => {
                assert!(matches!(*cmd, PreparedCommand::Prepare(_)));
            }
            route => panic!("unexpected route: {route:?}"),
        }
    }

    #[test]
    fn routes_move_into_prepared_move() {
        match dispatch_statement(parse_stmt("MOVE 5 FROM c")) {
            FrontendRoute::Prepared(cmd) => {
                assert!(matches!(*cmd, PreparedCommand::Move(_)));
            }
            route => panic!("unexpected route: {route:?}"),
        }
    }

    #[test]
    fn routes_show_into_utility() {
        match dispatch_statement(parse_stmt("SHOW ALL")) {
            FrontendRoute::Utility(cmd) => {
                assert!(matches!(*cmd, UtilityCommand::VariableShow(_)));
            }
            route => panic!("unexpected route: {route:?}"),
        }
    }

    #[test]
    fn routes_begin_into_transaction_utility() {
        match dispatch_statement(parse_stmt("BEGIN")) {
            FrontendRoute::Utility(cmd) => match *cmd {
                UtilityCommand::Transaction(stmt) => {
                    assert!(matches!(stmt.kind, TransactionKind::Begin));
                }
                other => panic!("unexpected utility command: {other:?}"),
            },
            route => panic!("unexpected route: {route:?}"),
        }
    }

    #[test]
    fn keeps_explain_execute_on_query_route() {
        match dispatch_statement(parse_stmt("EXPLAIN EXECUTE p")) {
            FrontendRoute::Query(_) => {}
            route => panic!("unexpected route: {route:?}"),
        }
    }

    #[test]
    fn classify_and_extract_utility_remain_consistent() {
        for sql in [
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SHOW ALL",
            "SET application_name = 'x'",
        ] {
            let stmt = parse_stmt(sql);
            assert_eq!(classify_statement(&stmt), StatementClass::Utility);
            let _ = utility_command_from_statement(stmt);
        }

        assert_eq!(
            classify_statement(&parse_stmt("EXPLAIN SELECT 1")),
            StatementClass::Query
        );
    }
}

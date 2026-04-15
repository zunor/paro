// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_parser::ast::{Statement, TransactionKind};

/// Returns whether a statement may execute while the current transaction is failed.
///
/// PostgreSQL only permits rollback-style control statements in this state.
pub(crate) fn is_allowed_in_failed_transaction(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Abort
            | Statement::Transaction(paro_parser::ast::TransactionStmt {
                kind: TransactionKind::Rollback | TransactionKind::RollbackToSavepoint(_),
            })
    )
}

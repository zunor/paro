// Copyright 2024-2026 Zunor
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use derive_visitor::{Drive, DriveMut, Visitor, VisitorMut};

use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Statement;

#[derive(Visitor)]
#[visitor(Expr(enter), Identifier(enter))]
pub struct StatementVisitor<F: FnMut(&Expr), G: FnMut(&Identifier)> {
    visit_expr: F,
    visit_ident: G,
}

impl<F: FnMut(&Expr), G: FnMut(&Identifier)> StatementVisitor<F, G> {
    pub fn new(visit_expr: F, visit_ident: G) -> Self {
        Self {
            visit_expr,
            visit_ident,
        }
    }

    fn enter_expr(&mut self, expr: &Expr) {
        (self.visit_expr)(expr);
    }

    fn enter_identifier(&mut self, ident: &Identifier) {
        (self.visit_ident)(ident);
    }

    pub fn visit(&mut self, stmt: &Statement) {
        stmt.drive(self);
    }
}

/// used in bendsql
#[derive(VisitorMut)]
#[visitor(Expr(enter), Identifier(enter))]
pub struct StatementReplacer<F: FnMut(&mut Expr), G: FnMut(&mut Identifier)> {
    replace_expr: F,
    replace_ident: G,
}

impl<F: FnMut(&mut Expr), G: FnMut(&mut Identifier)> StatementReplacer<F, G> {
    pub fn new(replace_expr: F, replace_ident: G) -> Self {
        Self {
            replace_expr,
            replace_ident,
        }
    }

    fn enter_expr(&mut self, expr: &mut Expr) {
        (self.replace_expr)(expr);
    }

    fn enter_identifier(&mut self, ident: &mut Identifier) {
        (self.replace_ident)(ident);
    }

    pub fn visit(&mut self, stmt: &mut Statement) {
        stmt.drive_mut(self);
    }
}

#[derive(VisitorMut)]
#[visitor(Expr(enter))]
pub struct ExprRewriter<F: FnMut(&mut Expr)> {
    rewrite_expr: F,
}

impl<F: FnMut(&mut Expr)> ExprRewriter<F> {
    pub fn new(rewrite_expr: F) -> Self {
        Self { rewrite_expr }
    }

    fn enter_expr(&mut self, expr: &mut Expr) {
        (self.rewrite_expr)(expr);
    }

    pub fn visit(&mut self, expr: &mut Expr) {
        expr.drive_mut(self);
    }
}

#[cfg(test)]
mod tests {
    use super::{ExprRewriter, StatementVisitor};
    use crate::ast::{BinaryOperator, Expr, Literal};

    #[test]
    fn expr_rewriter_descends_into_replaced_expression() {
        let mut expr = Expr::ColumnRef {
            span: None,
            column: crate::ast::ColumnRef {
                schema: None,
                table: None,
                column: crate::ast::ColumnID::Name(crate::ast::Identifier::from_name(None, "x")),
            },
        };

        let mut rewriter = ExprRewriter::new(|expr| match expr {
            Expr::ColumnRef { column, .. } if column.column.name() == "x" => {
                *expr = Expr::BinaryOp {
                    span: None,
                    op: BinaryOperator::Plus,
                    left: Box::new(Expr::Literal {
                        span: None,
                        value: Literal::UInt64(1),
                    }),
                    right: Box::new(Expr::ColumnRef {
                        span: None,
                        column: crate::ast::ColumnRef {
                            schema: None,
                            table: None,
                            column: crate::ast::ColumnID::Name(crate::ast::Identifier::from_name(
                                None, "y",
                            )),
                        },
                    }),
                };
            }
            Expr::ColumnRef { column, .. } if column.column.name() == "y" => {
                *expr = Expr::Literal {
                    span: None,
                    value: Literal::UInt64(2),
                };
            }
            _ => {}
        });
        rewriter.visit(&mut expr);

        let Expr::BinaryOp { left, right, .. } = expr else {
            panic!("expected rewritten binary op");
        };
        assert!(matches!(*left, Expr::Literal { .. }));
        assert!(matches!(*right, Expr::Literal { .. }));
    }

    #[test]
    fn statement_visitor_walks_exprs_without_mutating_statement() {
        let stmt = crate::parse_one("SELECT ?, ?, x FROM t").unwrap().stmt;
        let mut placeholders = 0usize;
        let mut identifiers = 0usize;

        let mut visitor = StatementVisitor::new(
            |expr| {
                if matches!(expr, Expr::Placeholder { .. }) {
                    placeholders += 1;
                }
            },
            |_| {
                identifiers += 1;
            },
        );
        visitor.visit(&stmt);

        assert_eq!(placeholders, 2);
        assert!(identifiers >= 2);
        assert_eq!(stmt.to_string(), "SELECT ?, ?, x FROM t");
    }
}

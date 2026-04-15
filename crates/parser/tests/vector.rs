// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use paro_parser::parser_testing::expr::*;

#[path = "support/common.rs"]
mod common;

use common::check_parser;

#[test]
fn test_vector_type_parsing() {
    check_parser(type_name, "VECTOR(3)", "VECTOR(3)");
    check_parser(type_name, "VECTOR(1536)", "VECTOR(1536)");
}

#[test]
fn test_ts_type_parsing() {
    check_parser(type_name, "TSVECTOR", "TSVECTOR");
    check_parser(type_name, "TSQUERY", "TSQUERY");
}

#[test]
fn test_vector_operator_parsing() {
    // Distance operators
    check_parser(expr, "a <-> b", "a <-> b");
    check_parser(expr, "a <+> b", "a <+> b");
    check_parser(expr, "a <=> b", "a <=> b");
    check_parser(expr, "a <#> b", "a <#> b");

    // Precedence test
    // + is 30, <-> is 22. So + binds tighter.
    // a <-> b + c is parsed as a <-> (b + c), which displays as a <-> b + c
    check_parser(expr, "a <-> b + c", "a <-> b + c");
    // (a <-> b) + c is parsed as (a <-> b) + c, which displays as (a <-> b) + c
    check_parser(expr, "(a <-> b) + c", "(a <-> b) + c");
}

#[test]
fn test_vector_elementwise_parsing() {
    // Standard operators should work
    check_parser(expr, "a + b", "a + b");
    check_parser(expr, "a - b", "a - b");
    check_parser(expr, "a * b", "a * b");
}

#[test]
fn test_vector_literal_parsing() {
    // Array literals
    check_parser(expr, "[1, 2, 3]", "[1, 2, 3]");
}

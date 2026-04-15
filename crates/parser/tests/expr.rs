// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use goldenfile::Mint;
use paro_parser::ast::quote::ident_needs_quote;
use paro_parser::ast::quote::QuotedIdent;
use paro_parser::ast::Expr;
use paro_parser::parser_testing::expr::*;
use paro_parser::parser_testing::ParseMode;
#[cfg(debug_assertions)]
use paro_parser::parser_testing::{parser_stack_stats_snapshot, reset_parser_stack_stats};

#[path = "support/common.rs"]
mod common;

use common::{parse_expr_case, run_parser, run_parser_with_mode, GOLDEN_ROOT};

#[test]
fn test_expr() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile("expr.txt").unwrap();

    let cases = &[
        r#"a"#,
        r#"?"#,
        r#"'I''m who I\'m.'"#,
        r#"'\776 \n \t \u0053 \xaa'"#,
        r#"char(0xD0, 0xBF, 0xD1)"#,
        r#"[42, 3.5, 4., .001, 5e2, 1.925e-3, .38e+7, 1.e-01, 0xfff, x'deedbeef']"#,
        r#"123456789012345678901234567890"#,
        r#"$$ab123c$$"#,
        r#"x'123456789012345678901234567890'"#,
        r#"1e100000000000000"#,
        r#"100_100_000"#,
        r#"1_12200_00"#,
        r#".1"#,
        r#"-1"#,
        r#"(1)"#,
        r#"(1,)"#,
        r#"(1,2)"#,
        r#"(1,2,)"#,
        r#"[1]"#,
        r#"[1,]"#,
        r#"[[1]]"#,
        r#"[[1],[2]]"#,
        r#"[[[1,2,3],[4,5,6]],[[7,8,9]]][0][1][2]"#,
        r#"((1 = 1) or 1)"#,
        r#"typeof(1 + 2)"#,
        r#"- - + + - 1 + + - 2"#,
        r#"0XFF + 0xff + 0xa + x'ffff'"#,
        r#"1 - -(- - -1)"#,
        r#"1 + a * c.d"#,
        r#"number % 2"#,
        r#""t":k1.k2"#,
        r#""t":k1.k2.0"#,
        r#"t.0"#,
        r#"(NULL,).0"#,
        r#"col1 not between 1 and 2"#,
        r#"sum(col1)"#,
        r#""random"()"#,
        r#"random(distinct)"#,
        r#"covar_samp(number, number)"#,
        r#"CAST(col1 AS BIGINT UNSIGNED)"#,
        r#"TRY_CAST(col1 AS BIGINT UNSIGNED)"#,
        r#"TRY_CAST(col1 AS TUPLE(BIGINT UNSIGNED NULL, BOOLEAN))"#,
        r#"trim(leading 'abc' from 'def')"#,
        r#"trim('aa','bb')"#,
        r#"timestamp()"#,
        r#"extract(year from d)"#,
        r#"date_part(year, d)"#,
        r#"datepart(year, d)"#,
        r#"date_trunc(week, to_timestamp(1630812366))"#,
        r#"TIME_SLICE(to_timestamp(1630812366), 4, 'MONTH', 'START')"#,
        r#"TIME_SLICE(to_timestamp(1630812366), 4, 'MONTH', 'end')"#,
        r#"TIME_SLICE(to_timestamp(1630812366), 4, 'WEEK')"#,
        r#"trunc(to_timestamp(1630812366), week)"#,
        r#"trunc(1630812366, 999)"#,
        r#"trunc(1630812366.23)"#,
        r#"trunc(to_timestamp(1630812366), 'y')"#,
        r#"trunc(to_timestamp(1630812366), 'mm')"#,
        r#"trunc(to_timestamp(1630812366), 'Q')"#,
        r#"DATEDIFF(SECOND, to_timestamp('2024-01-01 21:01:35.423179'), to_timestamp('2023-12-31 09:38:18.165575'))"#,
        r#"last_day(to_date('2024-10-22'), week)"#,
        r#"last_day(to_date('2024-10-22'))"#,
        r#"date_sub(QUARTER, 1, to_date('2018-01-02'))"#,
        r#"datebetween(QUARTER, to_date('2018-01-02'), to_date('2018-04-02'))"#,
        r#"position('a' in str)"#,
        r#"substring(a from b for c)"#,
        r#"substring(a, b, c)"#,
        r#"col1::UInt8"#,
        r#"(arr[0]:a).b"#,
        r#"arr[4]["k"]"#,
        r#"a rlike '^11'"#,
        r#"a like '%1$%1%' escape '$'"#,
        r#"a not like '%1$%1%' escape '$'"#,
        r#"'中文'::text not in ('a', 'b')"#,
        r#"G.E.B IS NOT NULL AND col1 not between col2 and (1 + col3) DIV sum(col4)"#,
        r#"sum(CASE WHEN n2.n_name = 'GERMANY' THEN ol_amount ELSE 0 END) / CASE WHEN sum(ol_amount) = 0 THEN 1 ELSE sum(ol_amount) END"#,
        r#"p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= CAST (1 AS smallint) AND l_quantity <= CAST (1 + 10 AS smallint)
            AND p_size BETWEEN CAST (1 AS smallint) AND CAST (5 AS smallint)
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'"#,
        r#"'中文'::text LIKE ANY ('a', 'b')"#,
        r#"'中文'::text LIKE ANY ('a', 'b') ESCAPE '$'"#,
        r#"'中文'::text LIKE ANY (SELECT 'a', 'b')"#,
        r#"'中文'::text LIKE ALL (SELECT 'a', 'b')"#,
        r#"'中文'::text LIKE SOME (SELECT 'a', 'b')"#,
        r#"'中文'::text LIKE ANY (SELECT 'a', 'b') ESCAPE '$'"#,
        r#"nullif(1, 1)"#,
        r#"nullif(a, b)"#,
        r#"coalesce(1, 2, 3)"#,
        r#"coalesce(a, b, c)"#,
        r#"ifnull(1, 1)"#,
        r#"ifnull(a, b)"#,
        r#"1 is distinct from 2"#,
        r#"a is distinct from b"#,
        r#"1 is not distinct from null"#,
        r#"{'k1':1,'k2':2}"#,
        r#"LISTAGG(salary, '|') WITHIN GROUP (ORDER BY salary DESC NULLS LAST)"#,
        r#"ROW_NUMBER() OVER (ORDER BY salary DESC)"#,
        r#"SUM(salary) OVER ()"#,
        r#"AVG(salary) OVER (PARTITION BY department)"#,
        r#"SUM(salary) OVER (PARTITION BY department ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"#,
        r#"AVG(salary) OVER (PARTITION BY department ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"#,
        r#"COUNT() OVER (ORDER BY hire_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)"#,
        r#"COUNT() OVER (ORDER BY hire_date ROWS UNBOUNDED PRECEDING)"#,
        r#"COUNT() OVER (ORDER BY hire_date ROWS CURRENT ROW)"#,
        r#"COUNT() OVER (ORDER BY hire_date ROWS 3 PRECEDING)"#,
        r#"QUANTILE_CONT(0.5)(salary) OVER (PARTITION BY department ORDER BY hire_date)"#,
        r#"ARRAY_APPLY([1,2,3], x -> x + 1)"#,
        r#"ARRAY_FILTER(col, y -> y % 2 = 0)"#,
        r#"(current_timestamp, current_timestamp(), now())"#,
        r#"ARRAY_REDUCE([1,2,3], (acc,t) -> acc + t)"#,
        r#"MAP_FILTER({1:1,2:2,3:4}, (k, v) -> k > v)"#,
        r#"MAP_TRANSFORM_KEYS({1:10,2:20,3:30}, (k, v) -> k + 1)"#,
        r#"MAP_TRANSFORM_VALUES({1:10,2:20,3:30}, (k, v) -> v + 1)"#,
        r#"INTERVAL '1 YEAR'"#,
        r#"(?, ?)"#,
        r#"@test_stage/input/34"#,
        r#"pg_catalog.pg_get_userbyid(1)"#,
        r#"pg_catalog.pg_encoding_to_char(6)"#,
        r#"my_schema.my_function(a, b, c)"#,
    ];

    for case in cases {
        run_parser(file, expr, case);
    }
}

#[test]
fn test_aggregate_function_call_modifiers() {
    let distinct_expr = parse_expr_case("COUNT(DISTINCT x)");
    let Expr::FunctionCall { func, .. } = &distinct_expr else {
        panic!("expected function call");
    };
    assert!(func.distinct);
    assert!(func.filter.is_none());
    assert!(func.order_by.is_empty());
    assert!(distinct_expr.to_string().contains("DISTINCT x"));

    let filter_expr = parse_expr_case("SUM(x) FILTER (WHERE y > 0)");
    let Expr::FunctionCall { func, .. } = &filter_expr else {
        panic!("expected function call");
    };
    assert!(!func.distinct);
    assert!(func.order_by.is_empty());
    let filter = func.filter.as_ref().expect("expected aggregate filter");
    assert_eq!(filter.to_string(), "y > 0");
    assert!(filter_expr.to_string().contains("FILTER (WHERE y > 0)"));

    let ordered_expr =
        parse_expr_case("LISTAGG(salary, '|') WITHIN GROUP (ORDER BY salary DESC NULLS LAST)");
    let Expr::FunctionCall { func, .. } = &ordered_expr else {
        panic!("expected function call");
    };
    assert_eq!(func.order_by.len(), 1);
    assert!(func.filter.is_none());
    assert!(ordered_expr.to_string().contains("WITHIN GROUP"));
}

#[test]
fn test_expr_stack() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile("expr-stack.txt").unwrap();

    let cases = &[
        r#"json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert('{}'::variant, 'email_address', 'gokul', true), 'home_phone', 12345, true), 'mobile_phone', 345678, true), 'race_code', 'M', true), 'race_desc', 'm', true), 'marital_status_code', 'y', true), 'marital_status_desc', 'yu', true), 'prefix', 'hj', true), 'first_name', 'g', true), 'last_name', 'p', true), 'deceased_date', '2085-05-07', true), 'birth_date', '6789', true), 'middle_name', '89', true), 'middle_initial', '0789', true), 'gender_code', '56789', true), 'gender_desc', 'm', true)"#,
    ];

    #[cfg(debug_assertions)]
    reset_parser_stack_stats();

    for case in cases {
        run_parser(file, expr, case);
    }

    #[cfg(debug_assertions)]
    {
        let stats = parser_stack_stats_snapshot();
        assert!(
            stats.samples > 0,
            "expected stack samples for deep expr parse"
        );
        assert!(
            stats.min_remaining > 0,
            "remaining stack should stay observable"
        );
    }
}

#[test]
fn test_expr_error() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile("expr-error.txt").unwrap();

    let cases = &[
        r#"5 * (a and ) 1"#,
        r#"a + +"#,
        r#"CAST(col1 AS foo)"#,
        r#"1 a"#,
        r#"CAST(col1)"#,
        r#"a.add(b)"#,
        r#"$ abc + 3"#,
        r#"[ x * 100 FOR x in [1,2,3] if x % 2 = 0 ]"#,
        r#"
            G.E.B IS NOT NULL
            AND col1 NOT BETWEEN col2 AND
            AND 1 + col3 DIV sum(col4)
        "#,
        r#"CAST(1 AS STRING) ESCAPE '$'"#,
        r#"1 + 1 ESCAPE '$'"#,
    ];

    for case in cases {
        run_parser(file, expr, case);
    }
}

#[test]
fn test_dialect() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile("dialect.txt").unwrap();

    let cases = &[
        r#"'a'"#,
        r#""a""#,
        r#"`a`"#,
        r#"'a''b'"#,
        r#"'a""b'"#,
        r#"'a\'b'"#,
        r#"'a"b'"#,
        r#"'a`b'"#,
        r#""a''b""#,
        r#""a""b""#,
        r#""a'b""#,
        r#""a\"b""#,
        r#""a`b""#,
    ];

    for case in cases {
        run_parser_with_mode(file, expr, ParseMode::Default, case);
    }

    let cases = &[
        r#"a"#,
        r#"a.add(b)"#,
        r#"a.sub(b).add(e)"#,
        r#"a.sub(b).add(e)"#,
        r#"1 + {'k1': 4}.k1"#,
        r#"'3'.plus(4)"#,
        r#"(3).add({'k1': 4 }.k1)"#,
        r#"[ x * 100 FOR x in [1,2,3] if x % 2 = 0 ]"#,
    ];

    for case in cases {
        run_parser_with_mode(file, expr, ParseMode::Default, case);
    }
}

#[test]
fn test_quote() {
    let cases = &[
        ("a", "a"),
        ("_", "_"),
        ("_abc", "_abc"),
        ("_abc12", "_abc12"),
        ("_12a", "_12a"),
        ("12a", "\"12a\""),
        ("a\\\"b", "\"a\\\"\"b\""),
        ("12", "\"12\""),
        ("🍣", "\"🍣\""),
        ("価格", "\"価格\""),
        ("\t", "\"\t\""),
        ("complex \"string\"", "\"complex \"\"string\"\"\""),
        ("\"\"\"", "\"\"\"\"\"\"\"\""),
        ("'''", "\"'''\""),
        ("name\"with\"quote", "\"name\"\"with\"\"quote\""),
    ];

    for (input, expected) in cases {
        if ident_needs_quote(input) {
            let quoted = QuotedIdent(input, '"').to_string();
            assert_eq!(quoted, *expected);

            let QuotedIdent(ident, quote) = quoted.parse().unwrap();
            assert_eq!(ident, *input);
            assert_eq!(quote, '"');
        } else {
            assert_eq!(input, expected);
        };
    }
}

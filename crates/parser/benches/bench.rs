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

fn main() {
    divan::main()
}

// bench                  fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ╰─ dummy                             │               │               │               │         │
// ├─ deep_function_call  81.55 µs      │ 290.1 µs      │ 91.42 µs      │ 94.28 µs      │ 100     │ 100
// ├─ deep_query          237.3 µs      │ 460.7 µs      │ 250.4 µs      │ 255.7 µs      │ 100     │ 100
// ├─ large_query         150.1 µs      │ 282.1 µs      │ 163.9 µs      │ 167 µs        │ 100     │ 100
// ├─ large_statement     149.7 µs      │ 185.4 µs      │ 161.4 µs      │ 161.8 µs      │ 100     │ 100
// ╰─ wide_expr           30.73 µs      │ 45.14 µs      │ 31.18 µs      │ 31.78 µs      │ 100     │ 100

#[divan::bench_group(max_time = 0.5)]
mod dummy {
    use std::sync::OnceLock;

    use paro_parser::parse_expr_tokens;
    use paro_parser::parse_one_tokens;
    use paro_parser::parse_tokens;
    use paro_parser::tokenize_sql;

    fn build_vector_literal(dim: usize) -> String {
        let values = (0..dim)
            .map(|idx| format!("{:.9}", ((idx % 17) as f64 - 8.0) / 17.0))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{values}]::Vector({dim})")
    }

    fn wide_embedding_case() -> &'static str {
        static CASE: OnceLock<String> = OnceLock::new();
        CASE.get_or_init(|| format!("cosine_distance(embedding, {})", build_vector_literal(1536)))
            .as_str()
    }

    #[divan::bench]
    fn large_statement() {
        let case = r#"explain SELECT SUM(count) FROM (SELECT ((((((((((((true)and(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))or((('780820706')=('')))) IS NOT NULL AND ((((((((((true)AND(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))OR((('780820706')=(''))))) ::INT64)as count FROM t0) as res;"#;
        let tokens = tokenize_sql(case).unwrap();
        let stmt = parse_one_tokens(&tokens).unwrap();
        divan::black_box(stmt.stmt);
    }

    #[divan::bench]
    fn large_query() {
        let case = r#"SELECT SUM(count) FROM (SELECT ((((((((((((true)and(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))or((('780820706')=('')))) IS NOT NULL AND ((((((((((true)AND(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))OR((('780820706')=(''))))) ::INT64)as count FROM t0) as res;"#;
        let tokens = tokenize_sql(case).unwrap();
        let stmt = parse_one_tokens(&tokens).unwrap();
        divan::black_box(stmt.stmt);
    }

    #[divan::bench]
    fn deep_query() {
        let case = r#"SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers"#;
        let tokens = tokenize_sql(case).unwrap();
        let stmt = parse_one_tokens(&tokens).unwrap();
        divan::black_box(stmt.stmt);
    }

    #[divan::bench]
    fn wide_expr() {
        let case = r#"a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a"#;
        let tokens = tokenize_sql(case).unwrap();
        let expr = parse_expr_tokens(&tokens).unwrap();
        divan::black_box(expr);
    }

    #[divan::bench]
    fn deep_function_call() {
        let case = r#"json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert(json_object_insert('{}'::variant, 'email_address', 'gokul', true), 'home_phone', 12345, true), 'mobile_phone', 345678, true), 'race_code', 'M', true), 'race_desc', 'm', true), 'marital_status_code', 'y', true), 'marital_status_desc', 'yu', true), 'prefix', 'hj', true), 'first_name', 'g', true), 'last_name', 'p', true), 'deceased_date', '2085-05-07', true), 'birth_date', '6789', true), 'middle_name', '89', true), 'middle_initial', '0789', true), 'gender_code', '56789', true), 'gender_desc', 'm', true), 'home_phone_line_type', 'uyt', true), 'mobile_phone_line_type', 4, true)"#;
        let tokens = tokenize_sql(case).unwrap();
        let expr = parse_expr_tokens(&tokens).unwrap();
        divan::black_box(expr);
    }

    #[divan::bench]
    fn deep_cte() {
        let case = (0..96).fold("SELECT 1".to_string(), |inner, depth| {
            format!("WITH RECURSIVE t{depth} AS ({inner}) SELECT * FROM t{depth}")
        });
        let tokens = tokenize_sql(&case).unwrap();
        let stmt = parse_one_tokens(&tokens).unwrap();
        divan::black_box(stmt.stmt);
    }

    #[divan::bench]
    fn deep_multi_statement() {
        let case = std::iter::repeat_n("SELECT (SELECT 1)", 256)
            .collect::<Vec<_>>()
            .join("; ");
        let tokens = tokenize_sql(&case).unwrap();
        let stmts = parse_tokens(&tokens).unwrap();
        divan::black_box(stmts);
    }

    #[divan::bench]
    fn wide_embedding() {
        let case = wide_embedding_case();
        let tokens = tokenize_sql(case).unwrap();
        let expr = parse_expr_tokens(&tokens).unwrap();
        divan::black_box(expr);
    }
}

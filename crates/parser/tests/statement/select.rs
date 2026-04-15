// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::common::{write_statement_cases, write_statement_error_cases};

#[test]
fn test_query_statements() {
    let cases = &[
        r#"select current_database();"#,
        r#"select distinct a, count(*) from t where a = 1 and b - 1 < a group by a having a = 1;"#,
        r#"select * from t4;"#,
        r#"select top 2 * from t4;"#,
        r#"select * from aa.bb;"#,
        r#"from aa.bb select *;"#,
        r#"from aa.bb"#,
        r#"select * from a, b, c;"#,
        r#"select * from a, b, c order by "db"."a"."c1";"#,
        r#"select * from a join b on a.a = b.a;"#,
        r#"select * from a left outer join b on a.a = b.a;"#,
        r#"select * from a right outer join b on a.a = b.a;"#,
        r#"select * from a left semi join b on a.a = b.a;"#,
        r#"select * from a semi join b on a.a = b.a;"#,
        r#"select * from a left anti join b on a.a = b.a;"#,
        r#"select * from a anti join b on a.a = b.a;"#,
        r#"SETTINGS (max_thread=1, timezone='Asia/Shanghai') select 1;"#,
        r#"SETTINGS (max_thread=1) select * from a anti join b on a.a = b.a;"#,
        r#"select * from a right semi join b on a.a = b.a;"#,
        r#"select * from a right anti join b on a.a = b.a;"#,
        r#"select * from a full outer join b on a.a = b.a;"#,
        r#"select * FROM fuse_compat_table ignore_result;"#,
        r#"select * from a inner join b on a.a = b.a;"#,
        r#"select * from a left outer join b using(a);"#,
        r#"select * from a right outer join b using(a);"#,
        r#"select * from a full outer join b using(a);"#,
        r#"select * from a inner join b using(a);"#,
        r#"select * from a where a.a = any (select b.a from b);"#,
        r#"select * from a where a.a = all (select b.a from b);"#,
        r#"select * from a where a.a = some (select b.a from b);"#,
        r#"select * from a where a.a > (select b.a from b);"#,
        r#"select 1 from numbers(1) where ((1 = 1) or 1)"#,
        r#"select * from read_parquet('p1', 'p2', 'p3', prune_page => true, refresh_meta_cache => true);"#,
        r#"select * from @foo (pattern=>'[.]*parquet' file_format=>'tsv');"#,
        r#"select 'stringwith''quote'''"#,
        r#"select 'stringwith"doublequote'"#,
        r#"select '🦈'"#,
        r#"select * FROM t where ((a));"#,
        r#"select * FROM t where ((select 1) > 1);"#,
        r#"select ((t1.a)>=(((((t2.b)<=(t3.c))) IS NOT NULL)::INTEGER));"#,
        r#"select 33 as row, abc(33, row), def(row)"#,
        r#"SELECT func(ROW) FROM (SELECT 1 as ROW) t"#,
        r#"select * from t sample row (99);"#,
        r#"select * from t sample block (99);"#,
        r#"select * from t sample row (10 rows);"#,
        r#"select * from numbers(1000) sample row (99);"#,
        r#"select * from numbers(1000) sample block (99);"#,
        r#"select * from numbers(1000) sample row (10 rows);"#,
        r#"select * from numbers(1000) sample block (99) row (10 rows);"#,
        r#"select * from numbers(1000) sample block (99) row (10);"#,
        r#"select parse_json('{"k1": [0, 1, 2]}').k1[0];"#,
        r#"SELECT avg((number > 314)::UInt32);"#,
        r#"SELECT 1 - (2 + 3);"#,
        r#"select $abc + 3"#,
        r#"select IDENTIFIER($abc)"#,
        r#"select $1 FROM '@my_stage/my data/'"#,
        r#"
            SELECT t.c1 FROM @stage1/dir/file
                ( file_format => 'PARQUET', FILES => ('file1', 'file2')) t;
        "#,
        r#"
            select table0.c1, table1.c2 from
                @stage1/dir/file ( FILE_FORMAT => 'parquet', FILES => ('file1', 'file2')) table0
                left join table1;
        "#,
        r#"SELECT c1 FROM 's3://test/bucket' (PATTERN => '*.parquet', connection => (ENDPOINT_URL = 'xxx')) t;"#,
        r#"SELECT * FROM t GROUP BY all"#,
        r#"SELECT * FROM t GROUP BY a, b, c, d"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS (a, b, c, d)"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS (a, b, (c, d))"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ((a, b), (c), (d, e))"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ((a, b), (), (d, e))"#,
        r#"SELECT * FROM t GROUP BY CUBE (a, b, c)"#,
        r#"SELECT * FROM t GROUP BY ROLLUP (a, b, c)"#,
        r#"SELECT * FROM t GROUP BY a, ROLLUP (b, c)"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ((a, b)), a, ROLLUP (b, c)"#,
        r#"SELECT sum(d) OVER (w) FROM e;"#,
        r#"SELECT first_value(d) OVER (w) FROM e;"#,
        r#"SELECT first_value(d) ignore nulls OVER (w) FROM e;"#,
        r#"SELECT first_value(d) respect nulls OVER (w) FROM e;"#,
        r#"SELECT sum(d) IGNORE NULLS OVER (w) FROM e;"#,
        r#"SELECT sum(d) OVER w FROM e WINDOW w AS (PARTITION BY f ORDER BY g);"#,
        r#"SELECT number, rank() OVER (PARTITION BY number % 3 ORDER BY number)
  FROM numbers(10) where number > 10 and number > 9 and number > 8;"#,
        r#"
            with
            abc as (
                select
                    id, uid, eid, match_id, created_at, updated_at
                from (
                    select * from ddd.ccc where score > 0 limit 10
                )
                qualify row_number() over(partition by uid,eid order by updated_at desc) = 1
            )
            select * from abc;
        "#,
    ];

    write_statement_cases("select.txt", cases);
}

#[test]
fn test_query_statement_errors() {
    let cases = &[
        r#"SELECT c a as FROM t"#,
        r#"SELECT c a as b FROM t"#,
        r#"SELECT top -1 c a as b FROM t"#,
        r#"SELECT top c a as b FROM t"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS a, b"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ()"#,
        r#"select * from aa.bb limit 10 order by bb;"#,
        r#"select * from aa.bb offset 10 order by bb;"#,
        r#"select * from aa.bb offset 10 limit 1;"#,
        r#"select * from aa.bb order by a order by b;"#,
        r#"select * from aa.bb offset 10 offset 20;"#,
        r#"select * from aa.bb limit 10 limit 20;"#,
        r#"select * from aa.bb limit 10,2 offset 2;"#,
        r#"select * from aa.bb limit 10,2,3;"#,
        r#"with a as (select 1) with b as (select 2) select * from aa.bb;"#,
        r#"with as t2(tt) as (select a from t) select t2.tt from t2"#,
        r#"select $1 from @data/csv/books.csv (file_format => 'aa' bad_arg => 'x', pattern => 'bb')"#,
        r#"select $0 from t1"#,
    ];

    write_statement_error_cases("select-error.txt", cases);
}

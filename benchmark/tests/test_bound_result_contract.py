from decimal import Decimal
from pathlib import Path
import sys
import unittest
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
from bound_result_contract import BoundResult, Uncovered, order_values
from exact_result_value import exact_number
from tpcds_result_contract import ColumnContract as C


class BoundResultContractTests(unittest.TestCase):
    def setUp(self):
        self.parser = duckdb.connect()
        self.addCleanup(self.parser.close)
        self.catalog = {"t": [("a",64),("b",32),("s",None)], "u":[("a",64)]}

    def bind(self, sql):
        return BoundResult(sql, self.parser, self.catalog)

    def test_scoped_star_expansion_and_case_rules(self):
        b = self.bind('WITH x AS (SELECT a AS Foo,b AS "Exact" FROM t) SELECT * FROM x q')
        b.check_identity([C("q.foo","int64","20"),C("q.Exact","int32","23")], "paro")
        b.check_identity([C("Foo","int64","BIGINT"),C("Exact","int32","INTEGER")], "duckdb")
        for names in [("q.foo","q.exact"),("other.foo","q.Exact"),("q.Exact","q.foo")]:
            with self.assertRaises(ValueError):
                b.check_identity([C(n,"int64","20") for n in names], "paro")

    def test_nested_alias_and_star_follow_only_the_named_scope(self):
        b = self.bind('SELECT z.*, y.a FROM (SELECT a AS x FROM t) z, u y')
        b.check_identity([C("z.x","int64","20"),C("y.a","int64","20")], "paro")
        with self.assertRaises(ValueError):
            b.check_identity([C("y.a","int64","20"),C("z.x","int64","20")], "paro")

    def test_alias_with_dots_is_not_a_qualifier(self):
        b = self.bind('SELECT a AS "A.b" FROM t ORDER BY "A.b" DESC NULLS LAST')
        b.check_identity([C("A.b","int64","20")], "paro")
        for name in ["b", "a.b", "t.A.b"]:
            with self.assertRaises(ValueError):
                b.check_identity([C(name,"int64","20")], "paro")
        order_values([(exact_number(2),),(exact_number(1),),(None,)], b.bind_order())

    def test_alias_quote_provenance_is_not_borrowed_from_source(self):
        b = self.bind('SELECT "A" AS A FROM t')
        b.check_identity([C("a","int64","20")], "paro")
        with self.assertRaises(ValueError):
            b.check_identity([C("A","int64","20")], "paro")

    def test_integer_sum_mapping_is_expression_and_range_scoped(self):
        b = self.bind('SELECT sum(a),sum(b),sum(a)-sum(a) FROM t')
        a = [C("sum(a)","numeric","1700"),C("sum(b)","int64","20"),C("x","decimal(38,0)","1700")]
        d = [C("x","int128","HUGEINT") for _ in a]
        self.assertEqual(len(b.check_types(a,d)),3)
        for bad in [C("sum(a)","int64","20"),C("sum(a)","decimal(38,2)","1700"),C("sum(a)","numeric","20")]:
            with self.assertRaises(ValueError):
                b.check_types([bad,*a[1:]],d)
        with self.assertRaises(Uncovered):
            self.bind('SELECT a FROM t').check_types([C("a","int64","20")],[C("a","int128","HUGEINT")])

    def test_grouping_is_read_from_projection_not_inferred_from_null(self):
        b = self.bind('SELECT a,b,grouping(a)+grouping(b) AS h FROM t GROUP BY ROLLUP(a,b) ORDER BY h DESC, CASE WHEN grouping(a)+grouping(b)=0 THEN a END NULLS FIRST')
        rows = [(None,None,exact_number(2)),(None,None,exact_number(1)),(None,None,exact_number(0)),(exact_number(1),None,exact_number(0))]
        order_values(rows,b.bind_order())
        with self.assertRaises(ValueError):
            order_values(list(reversed(rows)),b.bind_order())
        with self.assertRaises(Uncovered):
            self.bind('SELECT a,b FROM t GROUP BY ROLLUP(a,b) ORDER BY grouping(a)').bind_order()

    def test_aggregate_order_binds_expression_not_display_text(self):
        b = self.bind('SELECT count(DISTINCT a) AS "n rows" FROM t ORDER BY count(DISTINCT a) DESC')
        self.assertEqual(b.bind_order()[0][0],("column",0))
        with self.assertRaises(Uncovered):
            self.bind('SELECT count(a) FROM t ORDER BY count(DISTINCT a)').bind_order()

    def test_arithmetic_peers_and_limit_keep_exact_bag_obligation(self):
        b = self.bind('SELECT a,b FROM t ORDER BY a-b,1 DESC NULLS LAST LIMIT 2')
        rows = [(exact_number(3),exact_number(3)),(exact_number(2),exact_number(2))]
        self.assertEqual(len(order_values(rows,b.bind_order())),2)
        with self.assertRaises(ValueError):
            order_values(list(reversed(rows)),b.bind_order())

    def test_ambiguous_alias_wrong_ordinal_and_hidden_key_are_rejected(self):
        for sql in ['SELECT a AS x,b AS x FROM t ORDER BY x',
                    'SELECT a FROM t ORDER BY 2', 'SELECT a FROM t ORDER BY b']:
            with self.assertRaises(Uncovered):
                self.bind(sql).bind_order()

    def test_derived_label_does_not_reassociate_or_discard_clauses(self):
        b = self.bind('SELECT (a+b)*b FROM t')
        for label in ['a+(b*b)','(a+b)*b FROM t','(a+b)*b LIMIT 1',
                      '(a+b)*b WHERE true', '(a+b)*b GROUP BY a,b',
                      '(a+b)*b HAVING true', '(a+b)*b AS wrong']:
            with self.assertRaises(ValueError):
                b.check_identity([C(label,"int64","20")],"paro")

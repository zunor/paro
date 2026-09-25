# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from decimal import Decimal
from pathlib import Path
import sys
import unittest
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"corpora"))
from exact_result_value import exact_number, ResultContractError
from order_numeric_contract import to_double
from bound_result_contract import BoundResult, order_values, evaluate, Uncovered
from tpcds_result_contract import ColumnContract as C, assert_same_multiset


class TypedOrderTests(unittest.TestCase):
    def test_duckdb_cast_chain_matches_pinned_engine_at_boundaries(self):
        self.assertEqual(duckdb.__version__, "1.5.5")
        with duckdb.connect() as db:
            for scale in [0,1,2,9,18,22]:
                for n in [0,1,-1,2**53-1,2**53,2**53+1,-2**53-1,
                          10**38-1,10**38-2,-10**38+1]:
                    value = Decimal((int(n<0),tuple(map(int,str(abs(n)))), -scale))
                    kind = f"decimal(38,{scale})"
                    got = db.execute(f"SELECT CAST(CAST(? AS {kind}) AS DOUBLE)", [str(value)]).fetchone()[0]
                    self.assertEqual(to_double(exact_number(value),kind,"duckdb").hex(),got.hex(),(kind,n))

    def test_coefficient_scaling_is_not_decimal_to_float(self):
        # This is a conversion contract, not an epsilon exception.
        value = Decimal("9007199254740993.01")
        got = to_double(exact_number(value),"decimal(38,2)","paro")
        self.assertEqual(got, float(900719925474099301)/100.0)
        self.assertEqual(exact_number(value).value.numerator,900719925474099301)

    def test_peers_secondary_keys_null_and_limit_boundary(self):
        with duckdb.connect() as parser:
            bound = BoundResult('SELECT x,y FROM t ORDER BY x-y ASC NULLS LAST,x DESC LIMIT 100',parser)
            schema = [C('x','decimal(38,0)','1700'),C('y','float64','701')]
            keys = bound.bind_order(schema,'paro')
            a,b = exact_number(2**53),exact_number(2**53+1)
            rows = [(b,0.0),(a,0.0),(None,0.0)]
            values=order_values(rows,keys)
            self.assertEqual(values[0][0],values[1][0])
            with self.assertRaises(ResultContractError):order_values([rows[1],rows[0]],keys)
            first100=[(exact_number(i),0.0) for i in range(100)]
            order_values(first100,keys)
            with self.assertRaises(ResultContractError):
                assert_same_multiset(first100,first100[:99]+[(exact_number(100),0.0)])
            huge=[(exact_number(10**38-1),0.0),(exact_number(10**38-2),0.0)]
            hv=order_values(huge,keys)
            self.assertEqual(hv[0][0],hv[1][0])
            with self.assertRaises(ResultContractError):order_values(huge[::-1],keys)
            descending=BoundResult('SELECT x,y FROM t ORDER BY x-y DESC NULLS FIRST,x',parser)
            order_values([(None,0.0),(a,0.0),(b,0.0)],descending.bind_order(schema,'paro'))

    def test_close_subtraction_and_double_overflow(self):
        with duckdb.connect() as parser:
            b=BoundResult('SELECT x,y FROM t ORDER BY x-y',parser)
            keys=b.bind_order([C('x','decimal(38,2)','1700'),C('y','float64','701')],'paro')
            result=order_values([(exact_number(Decimal('1.01')),1.0)],keys)
            self.assertEqual(result[0][0],1.01-1.0)
        with self.assertRaises(ResultContractError):
            evaluate(('*',('column',0),('column',1)),(1e308,1e308))
        with self.assertRaises(ValueError):to_double(exact_number(1),'decimal(38,23)','paro')
        with self.assertRaises(ResultContractError):to_double(exact_number(Decimal('1000')),'decimal(3,0)','paro')
        with self.assertRaises(ResultContractError):to_double(exact_number(Decimal('0.001')),'decimal(3,2)','paro')

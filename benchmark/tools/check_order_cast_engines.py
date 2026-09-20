#!/usr/bin/env python3
"""Read-only SQL cast checks against both engines; no performance evidence."""
import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
import duckdb
import _duckdb
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"corpora"))
from exact_result_value import exact_number
from order_numeric_contract import to_double


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dsn',required=True)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    if args.output.exists():raise ValueError('refuse to overwrite evidence')
    if duckdb.__version__ != '1.5.5':raise ValueError('unpinned DuckDB')
    report={'contract':'typed-order-binary64-v1','duckdb_version':duckdb.__version__,
            'extension_sha256':hashlib.sha256(Path(_duckdb.__file__).read_bytes()).hexdigest(),'cases':[]}
    with psycopg.connect(args.dsn,autocommit=True) as conn,duckdb.connect() as db:
        conn.execute('SET optimizer_verify=true')
        report['server_observed']=conn.execute("SELECT name,setting FROM pg_settings WHERE name LIKE 'paro_diagnostic/%' ORDER BY name").fetchall()
        if not report['server_observed']:raise ValueError('missing server observation')
        for scale in [0,1,2,9,18,22]:
            for n in [0,1,-1,2**53-1,2**53,2**53+1,-2**53-1,10**38-1,10**38-2,-10**38+1]:
                value=Decimal((int(n<0),tuple(map(int,str(abs(n)))),-scale))
                kind=f'decimal(38,{scale})'
                sql=f'SELECT CAST(CAST(%s AS {kind}) AS DOUBLE)'
                a=conn.execute(sql,[format(value,'f')]).fetchone()[0]
                b=db.execute(sql.replace('%s','?'),[format(value,'f')]).fetchone()[0]
                expected=[to_double(exact_number(value),kind,e) for e in ['paro','duckdb']]
                case={'type':kind,'input':str(value),'observed_hex':[a.hex(),b.hex()],
                      'expected_hex':[v.hex() for v in expected]}
                report['cases'].append(case)
                if case['observed_hex']!=case['expected_hex']:
                    report['failure']=case
                    args.output.write_text(json.dumps(report,indent=2)+'\n')
                    raise AssertionError(case)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(len(report['cases']),'cast pairs passed')


if __name__=='__main__':main()

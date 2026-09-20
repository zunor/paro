"""One SQL output binding for identity, integer mappings and peer ordering.

This is deliberately not a SQL executor: only projections, scoped expansion
and scalar ORDER expressions over returned columns are supported. GROUPING
is usable only when its expression is projected, never inferred from NULL.
The catalog is an explicit, hashed input for base-table column expansion.
"""
from dataclasses import dataclass, replace
from decimal import Decimal
import json
import re

from exact_result_value import ExactNumber, ResultContractError, exact_number


class Uncovered(ResultContractError):
    pass


def parse_query(sql, parser):
    document = json.loads(parser.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0])
    if document.get("error") or len(document.get("statements", [])) != 1:
        raise Uncovered("requires one parseable SELECT")
    return document["statements"][0]["node"]


def identity(value):
    if isinstance(value, dict):
        return tuple(sorted((k, identity(v)) for k, v in value.items()
                            if k not in {"query_location", "alias"}))
    if isinstance(value, list):
        return tuple(map(identity, value))
    return value


@dataclass(frozen=True)
class Identifier:
    spelling: str
    quoted: bool = False

    def display(self, engine):
        return self.spelling if self.quoted or engine == "duckdb" else self.spelling.lower()

    def key(self):
        return self.spelling if self.quoted else self.spelling.lower()


@dataclass(frozen=True)
class Output:
    name: Identifier | None
    expression: dict
    integer_bits: int | None = None
    integer_aggregate: bool = False
    qualifier: Identifier | None = None
    explicit_alias: bool = False
    integer_wire: str | None = None


def lexical_identifiers(sql):
    # Lexical quote provenance only; expression syntax belongs to DuckDB's
    # parser. String literals/comments are skipped, not searched for aliases.
    token = re.compile(r"--[^\n]*|/\*.*?\*/|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|[A-Za-z_][A-Za-z_0-9$]*", re.S)
    result = []
    for match in token.finditer(sql):
        raw = match[0]
        if raw.startswith(("--", "/*", "'")):
            continue
        quoted = raw.startswith('"')
        name = raw[1:-1].replace('""', '"') if quoted else raw
        result.append((match.start(), match.end(), Identifier(name, quoted)))
    return result


class BoundResult:
    def __init__(self, sql, parser, catalog=None):
        self.sql = sql
        self.parser = parser
        self.catalog = catalog or {}
        self.tokens = lexical_identifiers(sql)
        self.node = parse_query(sql, parser)
        self.outputs, self.scope = self._query(self.node, {})
        self.orders = [order for mod in self.node.get("modifiers", [])
                       if mod["type"] == "ORDER_MODIFIER" for order in mod["orders"]]

    def identifier(self, spelling, location=None, after=False):
        candidates = [(start, end, name) for start, end, name in self.tokens
                      if name.spelling == spelling]
        if location is not None:
            if after:
                candidates = [c for c in candidates if c[0] > location]
                if candidates:
                    return candidates[0][2]
            else:
                local = [c for c in candidates if c[0] <= location <= c[1]]
                if local:
                    return local[0][2]
        forms = set(c[2] for c in candidates)
        if len(forms) == 1:
            return forms.pop()
        if not candidates:
            return Identifier(spelling)
        raise Uncovered("ambiguous quoted identifier provenance: " + spelling)

    def _alias(self, expr):
        locations = []
        def walk(v):
            if isinstance(v, dict):
                loc = v.get("query_location", 2**64-1)
                if loc < len(self.sql):
                    locations.append(loc)
                for child in v.values():
                    walk(child)
            elif isinstance(v, list):
                for child in v:
                    walk(child)
        walk(expr)
        return self.identifier(expr["alias"], max(locations, default=0), after=True)

    def _query(self, node, outer_ctes):
        ctes = dict(outer_ctes)
        for entry in node.get("cte_map", {}).get("map", []):
            value = entry["value"]
            outputs, _ = self._query(value["query"]["node"], ctes)
            aliases = value.get("aliases", [])
            if aliases:
                if len(aliases) != len(outputs):
                    raise Uncovered("CTE alias arity")
                outputs = [replace(o, name=self.identifier(a)) for o, a in zip(outputs, aliases)]
            ctes[self.identifier(entry["key"]).key()] = outputs
        if node["type"] == "SET_OPERATION_NODE":
            left, scope = self._query(node["left"], ctes)
            right, _ = self._query(node["right"], ctes)
            if len(left) != len(right):
                raise Uncovered("set operation arity")
            return [replace(a, integer_bits=max(a.integer_bits, b.integer_bits)
                            if a.integer_bits and b.integer_bits else None,
                            integer_aggregate=a.integer_aggregate and b.integer_aggregate)
                    for a, b in zip(left, right)], scope
        if node["type"] != "SELECT_NODE":
            raise Uncovered("unsupported query node " + node["type"])
        scope = self._from(node["from_table"], ctes)
        outputs = []
        for expr in node["select_list"]:
            if expr["class"] == "STAR":
                if any(expr.get(k) for k in ("exclude_list", "replace_list", "columns", "rename_list", "qualified_exclude_list")):
                    raise Uncovered("modified star expansion")
                qualifier = expr.get("relation_name")
                selected = [(q, cols) for q, cols in scope if not qualifier or q.key() == self.identifier(qualifier).key()]
                if not selected:
                    raise Uncovered("star has no bound source")
                for q, cols in selected:
                    for col in cols:
                        if col.name is None:
                            raise Uncovered("unnamed star column")
                        ref = {"class": "COLUMN_REF", "type": "COLUMN_REF", "alias": "",
                               "column_names": [q.spelling, col.name.spelling]}
                        outputs.append(replace(col, expression=ref, qualifier=q, explicit_alias=False))
                continue
            name, qualifier = None, None
            if expr.get("alias"):
                name = self._alias(expr)
            elif expr["class"] == "COLUMN_REF":
                name = self.identifier(expr["column_names"][-1], expr.get("query_location"))
                if len(expr["column_names"]) > 1:
                    qualifier = self.identifier(expr["column_names"][-2])
            bits, aggregate = self._integer(expr, scope)
            outputs.append(Output(name, expr, bits, aggregate, qualifier, bool(expr.get("alias")),
                                  self._integer_wire(expr, scope, bits)))
        return outputs, scope

    def _from(self, table, ctes):
        kind = table["type"]
        if kind == "EMPTY":
            return []
        if kind == "JOIN":
            if table.get("using_columns"):
                raise Uncovered("USING star/output binding requires merged column contract")
            return self._from(table["left"], ctes) + self._from(table["right"], ctes)
        if kind == "SUBQUERY":
            cols, _ = self._query(table["subquery"]["node"], ctes)
            name = self.identifier(table["alias"])
        elif kind == "BASE_TABLE":
            source = self.identifier(table["table_name"])
            name = self.identifier(table.get("alias") or table["table_name"])
            cols = ctes.get(source.key())
            if cols is None:
                cols = [Output(Identifier(n, True), {}, bits, False) for n, bits in self.catalog.get(source.key(), [])]
        else:
            raise Uncovered("unsupported FROM binding " + kind)
        aliases = table.get("column_name_alias", [])
        if aliases:
            if len(aliases) != len(cols):
                raise Uncovered("source column alias arity")
            cols = [replace(c, name=self.identifier(a)) for c, a in zip(cols, aliases)]
        return [(name, cols)]

    def _resolve(self, expr, scope):
        parts = expr["column_names"]
        name = self.identifier(parts[-1], expr.get("query_location")).key()
        qualifier = self.identifier(parts[-2]).key() if len(parts) > 1 else None
        matches = [(q, i, col) for q, cols in scope if qualifier is None or q.key() == qualifier
                   for i, col in enumerate(cols) if col.name and col.name.key() == name]
        if len(matches) != 1:
            return None
        return matches[0]

    def _integer(self, expr, scope):
        kind = expr.get("class")
        if kind == "COLUMN_REF":
            found = self._resolve(expr, scope)
            return (found[2].integer_bits, found[2].integer_aggregate) if found else (None, False)
        if kind == "CONSTANT":
            return ({"TINYINT": 8, "SMALLINT": 16, "INTEGER": 32, "BIGINT": 64, "HUGEINT": 128}.get(expr["value"]["type"]["id"]), False)
        children = expr.get("children", [])
        if kind == "CASE":
            children = [x["then_expr"] for x in expr["case_checks"]] + [expr["else_expr"]]
        if kind == "FUNCTION" and expr["function_name"] in {"count", "count_star"}:
            return 64, False
        if kind == "FUNCTION" and expr["function_name"] == "sum" and len(children) == 1:
            bits, _ = self._integer(children[0], scope)
            return ((64 if bits <= 32 else 128), True) if bits else (None, False)
        if kind == "CASE" or (kind == "FUNCTION" and expr["function_name"] in {"+", "-", "*", "coalesce"}) or expr.get("type") == "OPERATOR_COALESCE":
            types = [self._integer(c, scope) for c in children]
            if types and all(t[0] for t in types):
                return max(t[0] for t in types), any(t[1] for t in types)
        return None, False

    def check_identity(self, schema, engine):
        if len(schema) != len(self.outputs):
            raise Uncovered("bound output arity differs from wire schema")
        for i, (column, output) in enumerate(zip(schema, self.outputs)):
            accepted = set()
            if output.name:
                accepted.add(output.name.display(engine))
                if output.qualifier and not output.explicit_alias:
                    accepted.add(output.qualifier.display(engine) + "." + output.name.display(engine))
            if column.name in accepted:
                continue
            if output.name is None:
                try:
                    label = parse_query("SELECT " + column.name, self.parser)
                    if (label.get("from_table", {}).get("type") == "EMPTY"
                            and not label.get("modifiers") and len(label["select_list"]) == 1
                            and not any(label.get(key) for key in
                                        ("where_clause", "having", "qualify", "groups", "group_sets"))
                            and not label.get("cte_map", {}).get("map")
                            and not label["select_list"][0].get("alias")
                            and identity(label["select_list"][0]) == identity(output.expression)):
                        continue
                except Uncovered:
                    pass
            raise ResultContractError(f"column {i} {engine} identity {column.name!r} not bound to {sorted(accepted)!r}")

    def check_types(self, actual, expected):
        if len(actual) != len(self.outputs) or len(expected) != len(self.outputs):
            raise Uncovered("bound type arity")
        mappings = []
        for i, (a, b, o) in enumerate(zip(actual, expected, self.outputs)):
            self._check_wire_schema(a, "paro")
            self._check_wire_schema(b, "duckdb")
            if o.integer_wire and a.logical_type != o.integer_wire:
                raise ResultContractError(f"column {i}: Paro integer expression contract requires {o.integer_wire}, got {a.logical_type}")
            if a.logical_type == b.logical_type:
                mappings.append("same logical type")
                continue
            # PostgreSQL wire has no HUGEINT OID: Paro HugeInt deliberately
            # uses unconstrained NUMERIC. This requires a bound integer SUM
            # lineage, not just equal observed numbers or an untyped OID.
            if o.integer_aggregate and o.integer_bits in {64, 128} and b.logical_type == "int128":
                required = o.integer_wire
                if a.logical_type == required and a.engine_type == ("20" if o.integer_bits == 64 else "1700"):
                    mappings.append(f"integer aggregate int{o.integer_bits} -> int128; exact range checked")
                    continue
            raise Uncovered(f"column {i}: no lossless bound mapping {a.logical_type}/{b.logical_type}; integer lineage={o.integer_bits}/{o.integer_aggregate}")
        return mappings

    def _integer_wire(self, expr, scope, bits):
        if bits is None:
            return None
        if expr.get("class") == "COLUMN_REF":
            found = self._resolve(expr, scope)
            if found and found[2].integer_wire:
                return found[2].integer_wire
        if (bits == 128 and expr.get("class") == "FUNCTION"
                and expr.get("function_name") in {"+", "-", "*"}):
            # arithmetic.rs binds HugeInt arithmetic to checked Decimal(38,0).
            return "decimal(38,0)"
        return "numeric" if bits == 128 else f"int{bits}"

    @staticmethod
    def _check_wire_schema(column, engine):
        from tpcds_result_contract import PARO_EXACT_TYPES, duckdb_schema
        if engine == "duckdb":
            derived = duckdb_schema([(column.name, column.engine_type)])[0].logical_type
            if derived != column.logical_type:
                raise ResultContractError("DuckDB logical/wire descriptor mismatch")
        else:
            oid = int(column.engine_type)
            if oid == 1700:
                if not re.fullmatch(r"numeric|(?:decimal|numeric)\(\d+,\d+\)", column.logical_type):
                    raise ResultContractError("Paro NUMERIC wire descriptor mismatch")
            elif PARO_EXACT_TYPES.get(oid) != column.logical_type:
                raise ResultContractError("Paro logical/wire descriptor mismatch")

    def expression_key(self, expr):
        if isinstance(expr, dict):
            if expr.get("class") == "COLUMN_REF":
                resolved = self._resolve(expr, self.scope)
                if resolved:
                    return ("bound_column", resolved[0].key(), resolved[1])
            return tuple(sorted((k, self.expression_key(v)) for k, v in expr.items()
                                if k not in {"query_location", "alias"}))
        if isinstance(expr, list):
            return tuple(map(self.expression_key, expr))
        return expr

    def bind_order(self):
        return [(self._bind_scalar(o["expression"], ordinal=True),
                 o["type"] == "DESCENDING",
                 ("first" if o["type"] == "DESCENDING" else "last")
                 if o["null_order"] == "ORDER_DEFAULT" else
                 ("first" if o["null_order"] == "NULLS FIRST" else "last")) for o in self.orders]

    def canonical_rows(self, rows, schema, engine):
        from tpcds_result_contract import canonicalize_rows
        if len(schema) != len(self.outputs):
            raise Uncovered("wire output arity")
        for column in schema:
            self._check_wire_schema(column, engine)
        values = canonicalize_rows(rows, schema)
        for row in values:
            for v, out in zip(row, self.outputs):
                if v is not None and out.integer_bits is not None:
                    if not isinstance(v, ExactNumber) or v.value.denominator != 1:
                        raise ResultContractError("bound integer expression returned noninteger")
                    bits = 128 if engine == "duckdb" and out.integer_aggregate else out.integer_bits
                    if not -2**(bits-1) <= v.value.numerator < 2**(bits-1):
                        raise ResultContractError("integer expression outside bound logical width")
        return values

    def _bind_scalar(self, expr, ordinal=False):
        if ordinal and expr.get("class") == "CONSTANT" and expr["value"]["type"]["id"] in {"INTEGER", "BIGINT"}:
            index = expr["value"]["value"] - 1
            if not 0 <= index < len(self.outputs):
                raise Uncovered("ORDER ordinal out of range")
            return ("column", index)
        if expr.get("class") == "COLUMN_REF" and len(expr["column_names"]) == 1:
            name = self.identifier(expr["column_names"][0], expr.get("query_location")).key()
            matches = [i for i, out in enumerate(self.outputs) if out.name and out.name.key() == name]
            if len(matches) == 1:
                return ("column", matches[0])
            if len(matches) > 1 and len({self.expression_key(self.outputs[i].expression) for i in matches}) > 1:
                raise Uncovered("ambiguous ORDER output alias")
        matches = [i for i, out in enumerate(self.outputs)
                   if self.expression_key(expr) == self.expression_key(out.expression)]
        if matches:
            return ("column", matches[0])
        kind = expr.get("class")
        if kind == "CONSTANT":
            v = expr["value"]
            if v["is_null"]:
                return ("literal", None)
            if type(v.get("value")) is int:
                return ("literal", exact_number(v["value"]))
            if v["type"]["id"] == "VARCHAR":
                return ("literal", v["value"])
        if kind == "CASE":
            return ("case", tuple((self._bind_scalar(c["when_expr"]), self._bind_scalar(c["then_expr"]))
                                  for c in expr["case_checks"]), self._bind_scalar(expr["else_expr"]))
        if kind == "COMPARISON" and expr["type"] in {"COMPARE_EQUAL", "COMPARE_NOTEQUAL", "COMPARE_LESSTHAN", "COMPARE_GREATERTHAN"}:
            return (expr["type"], self._bind_scalar(expr["left"]), self._bind_scalar(expr["right"]))
        if kind == "FUNCTION" and expr["function_name"] in {"+", "-", "*"} and len(expr["children"]) == 2:
            return (expr["function_name"], *(self._bind_scalar(c) for c in expr["children"]))
        raise Uncovered("ORDER expression not derivable from projected values: " + str(identity(expr)))


def evaluate(expr, row):
    op = expr[0]
    if op == "column":
        return row[expr[1]]
    if op == "literal":
        return expr[1]
    if op == "case":
        for predicate, value in expr[1]:
            if evaluate(predicate, row) is True:
                return evaluate(value, row)
        return evaluate(expr[2], row)
    a, b = evaluate(expr[1], row), evaluate(expr[2], row)
    if a is None or b is None:
        return None
    if op.startswith("COMPARE_"):
        return {"COMPARE_EQUAL": lambda: a == b, "COMPARE_NOTEQUAL": lambda: a != b,
                "COMPARE_LESSTHAN": lambda: a < b, "COMPARE_GREATERTHAN": lambda: a > b}[op]()
    exact = isinstance(a, ExactNumber) and isinstance(b, ExactNumber)
    if exact:
        a, b = a.value, b.value
    else:
        a = float(a.value) if isinstance(a, ExactNumber) else a
        b = float(b.value) if isinstance(b, ExactNumber) else b
        if type(a) is not float or type(b) is not float:
            raise Uncovered("arithmetic ORDER operand types")
    value = {"+": lambda: a+b, "-": lambda: a-b, "*": lambda: a*b}[op]()
    return ExactNumber(value) if exact else value


def order_values(rows, orders):
    values = [tuple(evaluate(expr, row) for expr, _, _ in orders) for row in rows]
    for i in range(1, len(values)):
        for a, b, (_, descending, nulls) in zip(values[i-1], values[i], orders):
            if a == b:
                continue
            if a is None or b is None:
                comparison = (-1 if a is None else 1) * (1 if nulls == "first" else -1)
            else:
                comparison = (-1 if a < b else 1) * (-1 if descending else 1)
            if comparison > 0:
                raise ResultContractError(f"ORDER violated at rows {i-1}/{i}")
            break
    return values


def catalog_from_rows(metadata):
    catalog = {}
    for table, column, kind in metadata:
        catalog.setdefault(table, []).append((column, {"TINYINT":8, "SMALLINT":16,
            "INTEGER":32, "BIGINT":64, "HUGEINT":128}.get(kind)))
    return catalog


CATALOG_SQL = "SELECT table_name,column_name,data_type FROM duckdb_columns() WHERE schema_name='main' AND NOT internal ORDER BY table_name,column_index"


def result_verdicts(bound, actual_rows, actual_schema, expected_rows, expected_schema):
    """All dimensions are reported even if an earlier one fails."""
    from tpcds_result_contract import assert_same_multiset
    checks = {}
    def check(name, action):
        try:
            checks[name] = {"status": "pass", "detail": action()}
        except (AssertionError, ValueError, TypeError) as error:
            checks[name] = {"status": "Uncovered" if isinstance(error, Uncovered) else "fail", "error": str(error)}
    check("identity", lambda: [bound.check_identity(s, engine) for s, engine in
          ((actual_schema, "paro"), (expected_schema, "duckdb"))])
    check("schema", lambda: bound.check_types(actual_schema, expected_schema))
    normalized = {}
    for engine, rows, schema in (("paro",actual_rows,actual_schema),("duckdb",expected_rows,expected_schema)):
        def wire():
            normalized[engine] = bound.canonical_rows(rows, schema, engine)
            return {"rows": len(rows)}
        check(engine+"_wire_values", wire)
    if len(normalized) == 2:
        check("bag", lambda: assert_same_multiset(normalized["paro"], normalized["duckdb"]))
        def order():
            keys = bound.bind_order()
            a, b = [order_values(normalized[engine], keys) for engine in ("paro", "duckdb")]
            if a != b:
                raise ResultContractError("ordered peer-key sequences differ semantically")
            return {"keys": len(keys), "limit_contract": "exact selected bag plus ordered peer keys; no approximate boundary waiver"}
        check("order", order)
    else:
        checks.update({name:{"status":"Uncovered","error":"invalid wire values"} for name in ("bag","order")})
    return checks

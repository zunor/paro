# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Validate immutable definitions without executing user module code."""

import ast
import hashlib

from .protocol import MAX_CONTROL_BYTES, schema_from_wire


def validate(definition):
    if not isinstance(definition, dict):
        raise ValueError("TYPE_CONTRACT: definition must be an object")
    for field in ("routine_id", "generation"):
        value = definition[field]
        if type(value) is not int or not 0 <= value < 1 << 64:
            raise ValueError("TYPE_CONTRACT: invalid routine identity")
    if type(definition["strict"]) is not bool:
        raise ValueError("TYPE_CONTRACT: strict must be boolean")
    if not isinstance(definition["owner"], str) or not 0 < len(definition["owner"]) <= 256:
        raise ValueError("TYPE_CONTRACT: invalid routine owner")
    source = definition["source"]
    handler = definition["handler"]
    if not isinstance(source, str) or len(source.encode()) > MAX_CONTROL_BYTES // 2:
        raise ValueError("TYPE_CONTRACT: invalid Python source size")
    if not isinstance(handler, str) or not handler.isidentifier():
        raise ValueError("TYPE_CONTRACT: handler must be a module-level identifier")
    if hashlib.sha256(source.encode()).hexdigest() != definition["digest"]:
        raise ValueError("PROTOCOL: source digest mismatch")
    mode = definition["mode"]
    if mode not in ("scalar", "vector", "table"):
        raise ValueError("TYPE_CONTRACT: unsupported handler mode")
    inputs = schema_from_wire(definition["input_schema"])
    outputs = schema_from_wire(definition["output_schema"])
    if mode != "table" and len(outputs) != 1:
        raise ValueError("TYPE_CONTRACT: scalar/vector routines return one column")
    try:
        tree = ast.parse(source)
        compile(tree, "<paro-udf>", "exec")
    except (SyntaxError, ValueError, RecursionError) as error:
        raise ValueError(f"TYPE_CONTRACT: invalid Python source: {error}") from error
    candidates = [
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == handler
    ]
    if len(candidates) != 1 or isinstance(candidates[0], ast.AsyncFunctionDef):
        raise ValueError("TYPE_CONTRACT: expected one synchronous module-level handler")
    function = candidates[0]
    args = function.args
    if (len(args.posonlyargs) + len(args.args) != len(inputs) + 1
            or args.vararg or args.kwarg or args.kwonlyargs or args.defaults
            or function.decorator_list
            or not (args.posonlyargs + args.args)
            or (args.posonlyargs + args.args)[0].arg != "ctx"):
        raise ValueError("TYPE_CONTRACT: handler signature must be (ctx, arg...)")
    pending = list(function.body)
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.Yield, ast.YieldFrom)):
            raise ValueError("TYPE_CONTRACT: generator handlers are unsupported")
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            pending.extend(ast.iter_child_nodes(node))
    return inputs, outputs

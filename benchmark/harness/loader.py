# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Workload discovery and manifest loading."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
import re
from string import Template
import tomllib
from typing import Any, Mapping, Sequence


VALIDATE_MODES = {
    "none",
    "scalar_equals",
    "scalar_gte",
    "row_count",
    "ordered_rows",
    "ordered_digest",
    "text_contains_all",
}


@dataclass(frozen=True)
class QueryDef:
    id: str
    file: Path
    sql: str
    setup_sql: str | None = None
    teardown_sql: str | None = None
    validate: str = "none"
    expected: Any = None
    plan_contains: tuple[str, ...] = ()
    collect_explain_profile: bool = False
    allow_reexecute: bool = False


@dataclass(frozen=True)
class WorkloadDef:
    name: str
    description: str
    run_order: int
    minimum_server_memory_bytes: int
    root: Path
    params: dict[str, Any]
    setup_sql: str
    teardown_sql: str
    build_sql: str | None
    queries: tuple[QueryDef, ...]


def discover_workload_files(workloads_dir: Path) -> list[Path]:
    if not workloads_dir.exists():
        return []
    return sorted(p for p in workloads_dir.glob("*/workload.toml") if p.is_file())


def load_workloads(
    workloads_dir: Path,
    *,
    workload_name: str | None = None,
    query_filter: str | None = None,
    param_overrides: Mapping[str, Any] | None = None,
    default_collect_explain_profile: bool = False,
) -> list[WorkloadDef]:
    selected: list[WorkloadDef] = []
    normalized_filter = query_filter.lower() if query_filter else None
    workload_files = discover_workload_files(workloads_dir)

    for manifest_path in workload_files:
        workload = load_workload(
            manifest_path,
            param_overrides=param_overrides,
            default_collect_explain_profile=default_collect_explain_profile,
        )
        if workload_name and workload.name != workload_name and workload.root.name != workload_name:
            continue
        if normalized_filter:
            filtered = tuple(q for q in workload.queries if normalized_filter in q.id.lower())
            if not filtered:
                continue
            workload = replace(workload, queries=filtered)
        selected.append(workload)

    selected.sort(key=lambda workload: (workload.run_order, workload.root.name))
    if workload_name and not selected:
        raise ValueError(f"workload not found: {workload_name}")
    return selected


def load_named_workload(
    workloads_dir: Path,
    workload_name: str,
    *,
    param_overrides: Mapping[str, Any] | None = None,
    default_collect_explain_profile: bool = False,
) -> WorkloadDef:
    for manifest_path in discover_workload_files(workloads_dir):
        workload = load_workload(
            manifest_path,
            param_overrides=param_overrides,
            default_collect_explain_profile=default_collect_explain_profile,
        )
        if workload.name == workload_name or workload.root.name == workload_name:
            return workload
    raise ValueError(f"workload not found: {workload_name}")


def select_queries_exact(workload: WorkloadDef, query_ids: Sequence[str]) -> WorkloadDef:
    query_index = {query.id: query for query in workload.queries}
    selected: list[QueryDef] = []
    seen: set[str] = set()

    for query_id in query_ids:
        if query_id in seen:
            continue
        query = query_index.get(query_id)
        if query is None:
            raise ValueError(
                f"workload '{workload.name}' missing query id '{query_id}'"
            )
        selected.append(query)
        seen.add(query_id)

    if not selected:
        raise ValueError(f"workload '{workload.name}' selected zero queries")

    return replace(workload, queries=tuple(selected))


def load_workload(
    manifest_path: Path,
    *,
    param_overrides: Mapping[str, Any] | None = None,
    default_collect_explain_profile: bool = False,
) -> WorkloadDef:
    data = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    workload_root = manifest_path.parent

    meta = _get_table(data, "meta")
    setup = _get_table(data, "setup")
    params_table = _get_table(data, "params", required=False)
    params: dict[str, Any] = dict(params_table)
    if param_overrides:
        params.update(dict(param_overrides))

    name = str(meta.get("name") or workload_root.name)
    description = str(meta.get("description", ""))
    run_order = _optional_int(meta, "run_order", default=100, manifest_path=manifest_path)
    minimum_server_memory_bytes = _parse_byte_size(
        meta.get("minimum_server_memory", 0),
        manifest_path=manifest_path,
        field_name="meta.minimum_server_memory",
    )

    setup_file = _require_str(setup, "file", table_name="setup")
    teardown_file = _require_str(setup, "teardown", table_name="setup")
    setup_sql = _load_sql(workload_root, setup_file, params)
    teardown_sql = _load_sql(workload_root, teardown_file, params)

    build_sql: str | None = None
    build = _get_table(data, "build", required=False)
    if build:
        build_file = _require_str(build, "file", table_name="build")
        build_sql = _load_sql(workload_root, build_file, params)

    query_items = data.get("query")
    if not isinstance(query_items, list) or not query_items:
        raise ValueError(f"{manifest_path}: [[query]] is required")

    queries: list[QueryDef] = []
    for i, item in enumerate(query_items, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"{manifest_path}: query #{i} must be a table")
        query_id = str(item.get("id", "")).strip()
        if not query_id:
            raise ValueError(f"{manifest_path}: query #{i} missing id")
        query_file = str(item.get("file", "")).strip()
        if not query_file:
            raise ValueError(f"{manifest_path}: query '{query_id}' missing file")
        validate = str(item.get("validate", "none")).strip() or "none"
        if validate not in VALIDATE_MODES:
            raise ValueError(
                f"{manifest_path}: query '{query_id}' has invalid validate mode '{validate}'"
            )
        plan_contains = _coerce_plan_tokens(item.get("plan_contains"), manifest_path, query_id)
        collect_explain_profile = _coerce_bool(
            item.get("collect_explain_profile", default_collect_explain_profile),
            manifest_path,
            query_id,
            field_name="collect_explain_profile",
        )
        allow_reexecute = _coerce_bool(
            item.get("allow_reexecute", False),
            manifest_path,
            query_id,
            field_name="allow_reexecute",
        )
        sql = _load_sql(workload_root, query_file, params)
        query_setup_sql = _load_optional_query_sql(
            workload_root,
            item,
            key="setup",
            params=params,
            manifest_path=manifest_path,
            query_id=query_id,
        )
        query_teardown_sql = _load_optional_query_sql(
            workload_root,
            item,
            key="teardown",
            params=params,
            manifest_path=manifest_path,
            query_id=query_id,
        )
        queries.append(
            QueryDef(
                id=query_id,
                file=workload_root / query_file,
                sql=sql,
                setup_sql=query_setup_sql,
                teardown_sql=query_teardown_sql,
                validate=validate,
                expected=item.get("expected"),
                plan_contains=plan_contains,
                collect_explain_profile=collect_explain_profile,
                allow_reexecute=allow_reexecute,
            )
        )

    return WorkloadDef(
        name=name,
        description=description,
        run_order=run_order,
        minimum_server_memory_bytes=minimum_server_memory_bytes,
        root=workload_root,
        params=params,
        setup_sql=setup_sql,
        teardown_sql=teardown_sql,
        build_sql=build_sql,
        queries=tuple(queries),
    )


_BYTE_SIZE_PATTERN = re.compile(r"^([0-9]+)\s*([kmgt]?i?b)?$", re.IGNORECASE)
_BYTE_SIZE_MULTIPLIERS = {
    "": 1,
    "b": 1,
    "kb": 1_000,
    "mb": 1_000_000,
    "gb": 1_000_000_000,
    "tb": 1_000_000_000_000,
    "kib": 1 << 10,
    "mib": 1 << 20,
    "gib": 1 << 30,
    "tib": 1 << 40,
}


def _parse_byte_size(value: Any, *, manifest_path: Path, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{manifest_path}: {field_name} must be a byte size")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{manifest_path}: {field_name} must not be negative")
        return value
    if not isinstance(value, str):
        raise ValueError(f"{manifest_path}: {field_name} must be a byte size")
    match = _BYTE_SIZE_PATTERN.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"{manifest_path}: invalid {field_name} byte size {value!r}")
    amount = int(match.group(1))
    unit = (match.group(2) or "").lower()
    return amount * _BYTE_SIZE_MULTIPLIERS[unit]


def _load_sql(workload_root: Path, relative_path: str, params: Mapping[str, Any]) -> str:
    sql_path = workload_root / relative_path
    if not sql_path.exists():
        raise ValueError(f"missing SQL file: {sql_path}")
    template = Template(sql_path.read_text(encoding="utf-8"))
    mapping = _template_params(params)
    try:
        return template.substitute(mapping).strip()
    except KeyError as exc:
        missing = exc.args[0]
        raise ValueError(f"{sql_path}: missing template parameter '{missing}'") from exc


def _load_optional_query_sql(
    workload_root: Path,
    item: Mapping[str, Any],
    *,
    key: str,
    params: Mapping[str, Any],
    manifest_path: Path,
    query_id: str,
) -> str | None:
    raw_path = item.get(key)
    if raw_path is None:
        return None
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError(f"{manifest_path}: query '{query_id}' field '{key}' must be a SQL file path")
    return _load_sql(workload_root, raw_path.strip(), params)


def _template_params(params: Mapping[str, Any]) -> dict[str, str]:
    rendered: dict[str, str] = {}
    for key, value in params.items():
        if isinstance(value, bool):
            rendered[key] = "true" if value else "false"
        else:
            rendered[key] = str(value)

    vertices = params.get("vertices")
    edges = params.get("edges")
    if isinstance(vertices, int):
        rendered.setdefault("vertices_minus_one", str(max(vertices - 1, 0)))
    if isinstance(vertices, int) and isinstance(edges, int):
        chain_edges = max(vertices - 1, 0)
        rendered.setdefault("remaining_edges", str(max(edges - chain_edges, 0)))
    return rendered


def _get_table(
    data: Mapping[str, Any],
    key: str,
    *,
    required: bool = True,
) -> Mapping[str, Any]:
    raw = data.get(key)
    if raw is None:
        if required:
            raise ValueError(f"missing [{key}] section")
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"[{key}] must be a table")
    return raw


def _require_str(section: Mapping[str, Any], key: str, *, table_name: str) -> str:
    value = section.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"[{table_name}].{key} must be a non-empty string")
    return value.strip()


def _optional_int(
    section: Mapping[str, Any],
    key: str,
    *,
    default: int,
    manifest_path: Path,
) -> int:
    value = section.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{manifest_path}: [meta].{key} must be an integer")
    return value


def _coerce_plan_tokens(value: Any, manifest_path: Path, query_id: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(
            f"{manifest_path}: query '{query_id}' plan_contains must be an array of strings"
        )
    tokens = [item.strip() for item in value if item.strip()]
    return tuple(tokens)


def _coerce_bool(
    value: Any,
    manifest_path: Path,
    query_id: str,
    *,
    field_name: str,
) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{manifest_path}: query '{query_id}' {field_name} must be a boolean")

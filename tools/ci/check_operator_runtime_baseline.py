# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""BASELINE structural guards for operator-runtime performance work."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import re
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]

# HASH-JOIN must ratchet these numbers down to zero. The guard fails both when
# new production debt appears and when this threshold is stale after debt drops.
HASH_JOIN_PRODUCTION_DEFAULT_HASHER_DEBT = 0
HASH_JOIN_PRODUCTION_VALUE_HASH_DEBT = 0
AGGREGATE_PRODUCTION_DEFAULT_HASHER_DEBT = 0
AGGREGATE_PRODUCTION_VALUE_HASH_DEBT = 0
AGGREGATE_BOXED_DISTINCT_HASHSET_DEBT = 0
AGGREGATE_BOXED_DISTINCT_VALUE_ROW_DEBT = 0
AGGREGATE_ORDERED_VEC_VALUE_ARENA_DEBT = 0
HASH_JOIN_RUNTIME_FILTER_BUILD_RESCAN_DEBT = 0
HASH_JOIN_RUNTIME_FILTER_BOXED_KEY_DEBT = 0
SORT_MERGE_LINEAR_RUN_SCAN_DEBT = 0
SORT_MERGE_PER_BATCH_CURSOR_DEBT = 4
SORT_RANGE_JOIN_VALUE_ACCESS_DEBT = 7
CLASSIC_IE_JOIN_OUTPUT_VEC_DEBT = 0
NESTED_LOOP_JOIN_VALUE_ACCESS_DEBT = 36
SCHEDULER_PRODUCTION_WIRING_DEADLINE = date(2026, 6, 8)


def main() -> int:
    errors: list[str] = []
    errors.extend(_check_hash_join_hashing_debt())
    errors.extend(_check_aggregate_hashing_debt())
    errors.extend(_check_hash_join_runtime_filter_sketch_guard())
    errors.extend(_check_hash_join_reclaimer_guard())
    errors.extend(_check_aggregate_reclaimer_guard())
    errors.extend(_check_sort_merge_guard())
    errors.extend(_check_inequality_join_guard())
    errors.extend(_check_expression_program_guard())
    errors.extend(_check_join_value_access_debt())
    errors.extend(_check_rowstore_boundary_guard())
    errors.extend(_check_rowset_pushdown_consumption())
    errors.extend(_check_scheduler_wiring_guard())
    errors.extend(_check_explain_profile_schema_guard())
    if errors:
        print("operator runtime BASELINE guard failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("operator runtime BASELINE guard passed")
    return 0


def _check_hash_join_hashing_debt() -> list[str]:
    source = _strip_test_sections(_read("crates/execution/src/join_hashtable/table.rs"))
    default_hasher_count = source.count("DefaultHasher")
    value_hash_count = len(re.findall(r"\.hash\(&mut hasher\)", source))
    errors = []
    if default_hasher_count > HASH_JOIN_PRODUCTION_DEFAULT_HASHER_DEBT:
        errors.append(
            "hash join hot path added DefaultHasher usage "
            f"({default_hasher_count}>{HASH_JOIN_PRODUCTION_DEFAULT_HASHER_DEBT}); "
            "use the typed/aHash kernel path"
        )
    if default_hasher_count < HASH_JOIN_PRODUCTION_DEFAULT_HASHER_DEBT:
        errors.append(
            "hash join DefaultHasher debt dropped; ratchet "
            f"HASH_JOIN_PRODUCTION_DEFAULT_HASHER_DEBT from {HASH_JOIN_PRODUCTION_DEFAULT_HASHER_DEBT} "
            f"to {default_hasher_count}"
        )
    if value_hash_count > HASH_JOIN_PRODUCTION_VALUE_HASH_DEBT:
        errors.append(
            "hash join hot path added Value::hash-style row hashing "
            f"({value_hash_count}>{HASH_JOIN_PRODUCTION_VALUE_HASH_DEBT}); use vector-buffer hashing"
        )
    if value_hash_count < HASH_JOIN_PRODUCTION_VALUE_HASH_DEBT:
        errors.append(
            "hash join Value::hash debt dropped; ratchet "
            f"HASH_JOIN_PRODUCTION_VALUE_HASH_DEBT from {HASH_JOIN_PRODUCTION_VALUE_HASH_DEBT} "
            f"to {value_hash_count}"
        )
    return errors


def _check_expression_program_guard() -> list[str]:
    errors = []
    program = _read("crates/execution/src/expression_executor/program.rs")
    executor = _strip_test_sections(_read("crates/execution/src/expression_executor/executor.rs"))
    if "PhysicalExpressionProgram" not in program:
        errors.append("EXPR-PROGRAM physical expression program lost PhysicalExpressionProgram")
    dead_patterns = {
        "KernelOp": r"\bKernelOp\b",
        "ScratchLayout": r"\bScratchLayout\b",
        "VectorKernelAbi": r"\bVectorKernelAbi\b",
        "KernelEvalContext": r"\bKernelEvalContext\b",
    }
    for dead_symbol, pattern in dead_patterns.items():
        if re.search(pattern, program) or re.search(pattern, executor):
            errors.append(f"EXPR-PROGRAM reintroduced unused expression abstraction {dead_symbol}")
    if 'format!("{expr' in program or 'format!("{exprs' in program:
        errors.append("EXPR-PROGRAM expression program cache/CSE must not key on Debug formatting")
    if re.search(r"expressions\s*:\s*Vec\s*<\s*Expression\s*>", executor):
        errors.append(
            "ExpressionExecutor reintroduced planner Expression storage in the compiled program"
        )
    if "VectorTreeV1" not in program or "ExpressionBackend::Jit" in program:
        errors.append("EXPR-PROGRAM v1 must stay vector-tree based; JIT/bytecode backend is out of scope")
    if (
        "ExpressionProgramCache" not in program
        or "settings_fingerprint" not in program
        or "expression_fingerprint" not in program
    ):
        errors.append(
            "EXPR-PROGRAM program cache/versioning guard lost cache, settings fingerprint, or structured expression fingerprint"
        )
    if "clear()" in program or ".clear();" in executor:
        errors.append("EXPR-PROGRAM program cache must evict incrementally, not clear on overflow")
    if "fn touch(" in program or "iter().position" in program:
        errors.append("EXPR-PROGRAM program cache hit path must not linearly scan the LRU queue")
    if "CachedProgramEntry" not in program or "access_epoch" not in program:
        errors.append("EXPR-PROGRAM program cache must use epoch/lazy LRU metadata, not O(N) touch")
    if (
        "ExpressionScratchLayout" not in program
        or "PhysicalSharedExpression" not in program
        or "shared_expression_count" not in program
        or "SharedEvaluation" not in executor
        or "shared_slots" not in executor
        or "selection_hash" not in executor
    ):
        errors.append(
            "EXPR-PROGRAM subexpression CSE must keep executor-consumed shared nodes and scratch layout"
        )
    if (
        "execute_all_into_with_input" in executor
        or "execute_into_with_input" in executor
        or "select_into_with_input" in executor
    ):
        errors.append("EXPR-PROGRAM production ABI should use VectorKernelInput, not legacy *_with_input entrypoints")

    hot_paths = [
        "crates/execution/src/operators/transform/filter.rs",
        "crates/execution/src/operators/transform/project.rs",
        "crates/execution/src/operators/join/hash/build.rs",
        "crates/execution/src/operators/join/hash/probe.rs",
        "crates/execution/src/operators/join/hash/replay.rs",
        "crates/execution/src/join_hashtable/hash_kernel.rs",
    ]
    value_access = []
    for rel in hot_paths:
        source = _strip_test_sections(_read(rel))
        for lineno, line in enumerate(source.splitlines(), start=1):
            if ".get_value(" in line or ".set_value(" in line:
                if rel.endswith("join_hashtable/hash_kernel.rs") and (
                    "value_fallback_hash" in line or "read_value" in line
                ):
                    continue
                value_access.append(f"{rel}:{lineno}: {line.strip()}")
    if value_access:
        errors.append(
            "filter/project/hash-join key hot paths must not use Value get/set boxing:\n    "
            + "\n    ".join(value_access)
        )

    required_versioned_sites = [
        "crates/execution/src/operators/transform/filter.rs",
        "crates/execution/src/operators/transform/project.rs",
        "crates/execution/src/operators/join/hash/build.rs",
        "crates/execution/src/operators/join/hash/probe.rs",
        "crates/execution/src/operators/join/hash/replay.rs",
    ]
    for rel in required_versioned_sites:
        if "with_expressions_for_session" not in _read(rel):
            errors.append(f"{rel} no longer compiles expressions with session versioning")

    required_kernel_sites = {
        "crates/execution/src/operators/transform/filter.rs": "select_kernel",
        "crates/execution/src/operators/transform/project.rs": "execute_all_kernel",
        "crates/execution/src/operators/join/hash/keys.rs": "execute_kernel_into",
        "crates/execution/src/operators/aggregate/build_helpers.rs": "execute_all_kernel",
        "crates/execution/src/operators/sort/topn_build.rs": "execute_all_kernel",
    }
    for rel, call in required_kernel_sites.items():
        source = _read(rel)
        if "VectorKernelInput" not in source or call not in source:
            errors.append(f"{rel} no longer uses the EXPR-PROGRAM VectorKernelInput ABI via {call}")
    return errors


def _check_join_value_access_debt() -> list[str]:
    return (
        _check_value_access_debt(
            "crates/execution/src/operators/join/sort_range/mod.rs",
            SORT_RANGE_JOIN_VALUE_ACCESS_DEBT,
            "sort-range join Value get/set fast-path debt",
        )
        + _check_value_access_debt(
            "crates/execution/src/operators/join/nested_loop/runtime.rs",
            NESTED_LOOP_JOIN_VALUE_ACCESS_DEBT,
            "nested-loop join Value get/set fallback debt",
        )
    )


def _check_value_access_debt(rel: str, debt: int, label: str) -> list[str]:
    source = _strip_test_sections(_read(rel))
    count = source.count(".get_value(") + source.count(".set_value(")
    errors = []
    if count > debt:
        errors.append(f"{label} increased ({count}>{debt}); use typed/vector copy paths")
    if count < debt:
        errors.append(f"{label} dropped; ratchet debt from {debt} to {count}")
    return errors


def _check_aggregate_hashing_debt() -> list[str]:
    sources = "\n".join(_production_sources("crates/execution/src/operators/aggregate"))
    default_hasher_count = sources.count("DefaultHasher")
    value_hash_count = len(re.findall(r"\.hash\(&mut hasher\)", sources))
    boxed_distinct_hashset_count = sources.count("HashSet::<Box<[Value]>>::new")
    boxed_value_row_count = sources.count("AccountedValueRow") + sources.count("Box<[Value]>")
    ordered_helpers = _strip_test_sections(
        _read("crates/execution/src/operators/aggregate/ordered_helpers.rs")
    )
    ordered_vec_value_arena_count = len(re.findall(r"\bVec\s*<\s*Value\s*>", ordered_helpers))
    errors = []
    if default_hasher_count > AGGREGATE_PRODUCTION_DEFAULT_HASHER_DEBT:
        errors.append(
            "aggregate hot path added DefaultHasher usage "
            f"({default_hasher_count}>{AGGREGATE_PRODUCTION_DEFAULT_HASHER_DEBT}); "
            "use typed hash kernels or an explicit fast hasher"
        )
    if default_hasher_count < AGGREGATE_PRODUCTION_DEFAULT_HASHER_DEBT:
        errors.append(
            "aggregate DefaultHasher debt dropped; ratchet "
            f"AGGREGATE_PRODUCTION_DEFAULT_HASHER_DEBT from {AGGREGATE_PRODUCTION_DEFAULT_HASHER_DEBT} "
            f"to {default_hasher_count}"
        )
    if value_hash_count > AGGREGATE_PRODUCTION_VALUE_HASH_DEBT:
        errors.append(
            "aggregate hot path added Value::hash-style row hashing "
            f"({value_hash_count}>{AGGREGATE_PRODUCTION_VALUE_HASH_DEBT}); use typed hash kernels"
        )
    if value_hash_count < AGGREGATE_PRODUCTION_VALUE_HASH_DEBT:
        errors.append(
            "aggregate Value::hash debt dropped; ratchet "
            f"AGGREGATE_PRODUCTION_VALUE_HASH_DEBT from {AGGREGATE_PRODUCTION_VALUE_HASH_DEBT} "
            f"to {value_hash_count}"
        )
    if boxed_distinct_hashset_count > AGGREGATE_BOXED_DISTINCT_HASHSET_DEBT:
        errors.append(
            "aggregate DISTINCT/ORDERED added boxed Value-row HashSet debt "
            f"({boxed_distinct_hashset_count}>{AGGREGATE_BOXED_DISTINCT_HASHSET_DEBT}); "
            "use typed distinct key layout"
        )
    if boxed_distinct_hashset_count < AGGREGATE_BOXED_DISTINCT_HASHSET_DEBT:
        errors.append(
            "aggregate boxed DISTINCT HashSet debt dropped; ratchet "
            f"AGGREGATE_BOXED_DISTINCT_HASHSET_DEBT from {AGGREGATE_BOXED_DISTINCT_HASHSET_DEBT} "
            f"to {boxed_distinct_hashset_count}"
        )
    if boxed_value_row_count > AGGREGATE_BOXED_DISTINCT_VALUE_ROW_DEBT:
        errors.append(
            "aggregate DISTINCT/ORDERED reintroduced boxed Value row storage "
            f"({boxed_value_row_count}>{AGGREGATE_BOXED_DISTINCT_VALUE_ROW_DEBT}); "
            "use encoded distinct keys plus typed row-store replay"
        )
    if boxed_value_row_count < AGGREGATE_BOXED_DISTINCT_VALUE_ROW_DEBT:
        errors.append(
            "aggregate boxed Value row storage debt dropped; ratchet "
            f"AGGREGATE_BOXED_DISTINCT_VALUE_ROW_DEBT from {AGGREGATE_BOXED_DISTINCT_VALUE_ROW_DEBT} "
            f"to {boxed_value_row_count}"
        )
    if ordered_vec_value_arena_count > AGGREGATE_ORDERED_VEC_VALUE_ARENA_DEBT:
        errors.append(
            "ordered aggregate collector reintroduced Vec<Value> row arena debt "
            f"({ordered_vec_value_arena_count}>{AGGREGATE_ORDERED_VEC_VALUE_ARENA_DEBT}); "
            "use typed row-store collection and vector-copy replay"
        )
    if ordered_vec_value_arena_count < AGGREGATE_ORDERED_VEC_VALUE_ARENA_DEBT:
        errors.append(
            "ordered aggregate Vec<Value> arena debt dropped; ratchet "
            f"AGGREGATE_ORDERED_VEC_VALUE_ARENA_DEBT from {AGGREGATE_ORDERED_VEC_VALUE_ARENA_DEBT} "
            f"to {ordered_vec_value_arena_count}"
        )
    return errors


def _check_hash_join_runtime_filter_sketch_guard() -> list[str]:
    source = _strip_test_sections(_read("crates/execution/src/runtime/breaker/join.rs"))
    build_rescan_count = source.count("table.scan_spill_chunk(")
    boxed_key_count = source.count("key.add_value(vector.get_value(row_idx))")
    errors = []
    if build_rescan_count > HASH_JOIN_RUNTIME_FILTER_BUILD_RESCAN_DEBT:
        errors.append(
            "hash join runtime filter sketch added build-store rescan debt "
            f"({build_rescan_count}>{HASH_JOIN_RUNTIME_FILTER_BUILD_RESCAN_DEBT}); "
            "maintain a typed sketch during build instead"
        )
    if build_rescan_count < HASH_JOIN_RUNTIME_FILTER_BUILD_RESCAN_DEBT:
        errors.append(
            "hash join runtime filter build-rescan debt dropped; ratchet "
            f"HASH_JOIN_RUNTIME_FILTER_BUILD_RESCAN_DEBT from {HASH_JOIN_RUNTIME_FILTER_BUILD_RESCAN_DEBT} "
            f"to {build_rescan_count}"
        )
    if boxed_key_count > HASH_JOIN_RUNTIME_FILTER_BOXED_KEY_DEBT:
        errors.append(
            "hash join runtime filter sketch added boxed key materialization "
            f"({boxed_key_count}>{HASH_JOIN_RUNTIME_FILTER_BOXED_KEY_DEBT}); "
            "use typed key vectors in the sketch builder"
        )
    if boxed_key_count < HASH_JOIN_RUNTIME_FILTER_BOXED_KEY_DEBT:
        errors.append(
            "hash join runtime filter boxed-key debt dropped; ratchet "
            f"HASH_JOIN_RUNTIME_FILTER_BOXED_KEY_DEBT from {HASH_JOIN_RUNTIME_FILTER_BOXED_KEY_DEBT} "
            f"to {boxed_key_count}"
        )
    return errors


def _check_hash_join_reclaimer_guard() -> list[str]:
    breaker = _strip_test_sections(_read("crates/execution/src/runtime/breaker/join.rs"))
    build = _strip_test_sections(_read("crates/execution/src/operators/join/hash/build.rs"))
    replay = _strip_test_sections(_read("crates/execution/src/operators/join/hash/replay.rs"))
    state = _read("crates/execution/src/operators/join/state.rs")
    required_breaker = [
        "HashJoinBuildSpillReclaimer",
        "reclaim_build",
        "enable_build_reclaim",
        "disable_build_reclaim",
        "SpillCost::Repartition",
        "ReclaimableRowStore",
        "into_reclaimable()",
    ]
    required_build = [
        "register_reclaimer_once_by_name",
        "HashJoinBuildSpillReclaimer::new",
        "enable_build_reclaim",
        "unregister_reclaimer_by_name(&HashJoinBuildSpillReclaimer::name_for",
    ]
    required_replay = [
        "into_reclaiming_scanner",
        "build_cursor.next_chunk",
        "probe_cursor",
        "next_chunk",
    ]
    errors = []
    for symbol in required_breaker:
        if symbol not in breaker:
            errors.append(f"hash join build reclaimer lost breaker-side symbol `{symbol}`")
    for symbol in required_build:
        if symbol not in build:
            errors.append(f"hash join build reclaimer lost sink lifecycle symbol `{symbol}`")
    for symbol in required_replay:
        if symbol not in replay:
            errors.append(f"hash join spill replay lost reclaiming scan symbol `{symbol}`")
    if "ReclaimingRowScanCursor" not in state:
        errors.append("hash join spill replay local state must own a reclaiming row scanner")
    return errors


def _check_aggregate_reclaimer_guard() -> list[str]:
    breaker = _strip_test_sections(_read("crates/execution/src/runtime/breaker/aggregate.rs"))
    hash_build = _strip_test_sections(_read("crates/execution/src/operators/aggregate/hash/build.rs"))
    hash_emit = _strip_test_sections(_read("crates/execution/src/operators/aggregate/hash/emit.rs"))
    perfect_build = _strip_test_sections(
        _read("crates/execution/src/operators/aggregate/perfect_hash/build.rs")
    )
    perfect_emit = _strip_test_sections(
        _read("crates/execution/src/operators/aggregate/perfect_hash/emit.rs")
    )
    ungrouped_build = _strip_test_sections(
        _read("crates/execution/src/operators/aggregate/ungrouped/build.rs")
    )
    ungrouped_emit = _strip_test_sections(
        _read("crates/execution/src/operators/aggregate/ungrouped/emit.rs")
    )
    tables = "\n".join(
        _strip_test_sections(_read(rel))
        for rel in [
            "crates/execution/src/operators/aggregate/grouped_aggregate_hashtable.rs",
            "crates/execution/src/operators/aggregate/radix_partitioned_aggregate_hashtable.rs",
            "crates/execution/src/operators/aggregate/tuple_layout.rs",
        ]
    )
    required_breaker = [
        "AggregateFinalizedStateReclaimer",
        "reclaim_state_memory",
        "enable_state_reclaim",
        "disable_state_reclaim",
        "SpillCost::SpillToDisk",
        "spill_finalized_outputs",
        "AggregateSpilledOutput",
        "RowStoreSpillWriter",
    ]
    required_build = [
        "register_reclaimer_once_by_name",
        "AggregateFinalizedStateReclaimer::for_query",
        "enable_state_reclaim",
    ]
    required_emit = [
        "unregister_reclaimer_by_name",
        "AggregateFinalizedStateReclaimer::name_for",
    ]
    required_hash_emit = [
        "spilled_outputs",
        "RowSpillReader",
        "copy_spilled_output_rows",
    ]
    required_tables = [
        "reclaimable_finalized_memory",
        "reclaim_finalized_memory",
        "release_finalized_lookup_storage",
        "shrink_to_fit_and_refund",
        "table.destroy()?",
        "partition.destroy()?",
        "self.entries.shrink_to_fit_and_refund()",
    ]
    errors = []
    for symbol in required_breaker:
        if symbol not in breaker:
            errors.append(f"aggregate state reclaimer lost breaker-side symbol `{symbol}`")
    for source_name, source in [
        ("hash aggregate build", hash_build),
        ("perfect aggregate build", perfect_build),
        ("ungrouped aggregate build", ungrouped_build),
    ]:
        for symbol in required_build:
            if symbol not in source:
                errors.append(f"{source_name} lost aggregate reclaimer lifecycle symbol `{symbol}`")
    for source_name, source in [
        ("hash aggregate emit", hash_emit),
        ("perfect aggregate emit", perfect_emit),
        ("ungrouped aggregate emit", ungrouped_emit),
    ]:
        for symbol in required_emit:
            if symbol not in source:
                errors.append(f"{source_name} lost aggregate reclaimer lifecycle symbol `{symbol}`")
    for symbol in required_hash_emit:
        if symbol not in hash_emit:
            errors.append(f"hash aggregate emit lost spilled-output symbol `{symbol}`")
    for symbol in required_tables:
        if symbol not in tables:
            errors.append(f"aggregate table compaction lost symbol `{symbol}`")
    return errors


def _check_sort_merge_guard() -> list[str]:
    source = _strip_test_sections(_read("crates/execution/src/sorting/sorted_run_merger.rs"))
    linear_scan_count = source.count("fn select_next_run(")
    # `materialize_range` creates its cursors once and reuses them across the
    # complete range. Only constructors in the streaming source path are
    # per-batch debt.
    streaming_source = source.split("pub fn get_data(", 1)[1]
    per_batch_cursor_count = (
        streaming_source.count("cursor_on_demand(1)")
        + source.count(".map(SortedRun::external_key_cursor)")
        + source.count(".map(SortedRun::external_payload_cursor)")
    )
    errors = []
    if linear_scan_count > SORT_MERGE_LINEAR_RUN_SCAN_DEBT:
        errors.append(
            "sort merge added linear run-selection debt "
            f"({linear_scan_count}>{SORT_MERGE_LINEAR_RUN_SCAN_DEBT}); "
            "use a heap or loser tree over run cursors"
        )
    if linear_scan_count < SORT_MERGE_LINEAR_RUN_SCAN_DEBT:
        errors.append(
            "sort merge linear run-selection debt dropped; ratchet "
            f"SORT_MERGE_LINEAR_RUN_SCAN_DEBT from {SORT_MERGE_LINEAR_RUN_SCAN_DEBT} "
            f"to {linear_scan_count}"
        )
    if per_batch_cursor_count > SORT_MERGE_PER_BATCH_CURSOR_DEBT:
        errors.append(
            "sort merge added per-batch cursor construction debt "
            f"({per_batch_cursor_count}>{SORT_MERGE_PER_BATCH_CURSOR_DEBT}); "
            "keep reusable cursors/scratch in SortedRunMergerLocalState"
        )
    if per_batch_cursor_count < SORT_MERGE_PER_BATCH_CURSOR_DEBT:
        errors.append(
            "sort merge per-batch cursor debt dropped; ratchet "
            f"SORT_MERGE_PER_BATCH_CURSOR_DEBT from {SORT_MERGE_PER_BATCH_CURSOR_DEBT} "
            f"to {per_batch_cursor_count}"
        )
    return errors


def _check_inequality_join_guard() -> list[str]:
    source = _strip_test_sections(_read("crates/execution/src/operators/join/sort_range/mod.rs"))
    state = _read("crates/execution/src/operators/join/state.rs")
    required_source = [
        "SortRangeKeyKind",
        "SortRangeKeyValue",
        "sort_range_key_kind_for_condition",
        "sort_range_key_value_from_vector",
        "SORT_RANGE_CANDIDATE_CACHE_LIMIT",
        "prepare_probe_offsets",
        "fill_probe_offsets_by_sweeping_probe_permutation",
        "ProbeOffsetSweepSpec",
        "prepare_incremental_secondary_candidate_cache",
        "prepare_secondary_candidates_from_offsets",
        "secondary_positions_by_primary_pos",
        "build_secondary_positions_by_primary_pos",
        "append_candidates_by_primary_rank_scan",
        "append_candidates_by_secondary_range_scan",
        "append_cached_candidates_by_secondary_scan",
        "CachedSparsePositions",
        "ClassicIeJoinSourceExec",
        "ClassicIeJoinCursor",
        "cursor: Mutex<Option<ClassicIeJoinCursor>>",
        "build_classic_ie_join_cursor",
        "next_output_row",
        "fill_classic_ie_join_offsets",
        "ClassicIeJoinOffsetSpec",
    ]
    generator = _read("crates/execution/src/physical/generator/inequality_join_gate.rs")
    required_state = [
        "SortRangeProbeOffsets",
        "SortRangeCandidateRange",
        "cached_candidate_ranges",
        "cached_candidate_positions",
        "cached_candidates_ready",
        "primary_candidate_bitmap",
    ]
    errors = []
    for symbol in required_source:
        if symbol not in source:
            errors.append(f"sort-range join lost probe offset/incremental bitmap symbol `{symbol}`")
    classic_output_vec_count = len(
        re.findall(r"\bVec\s*<\s*ClassicIeJoinOutputRow\s*>", source)
    )
    if classic_output_vec_count > CLASSIC_IE_JOIN_OUTPUT_VEC_DEBT:
        errors.append(
            "classic IE join reintroduced full output-row materialization "
            f"({classic_output_vec_count}>{CLASSIC_IE_JOIN_OUTPUT_VEC_DEBT}); "
            "emit through ClassicIeJoinCursor batches instead"
        )
    if classic_output_vec_count < CLASSIC_IE_JOIN_OUTPUT_VEC_DEBT:
        errors.append(
            "classic IE join output Vec debt dropped; ratchet "
            f"CLASSIC_IE_JOIN_OUTPUT_VEC_DEBT from {CLASSIC_IE_JOIN_OUTPUT_VEC_DEBT} "
            f"to {classic_output_vec_count}"
        )
    if "next_output_row: AtomicUsize" in source:
        errors.append("classic IE join must not use an output Vec plus atomic row cursor")
    for symbol in required_state:
        if symbol not in state:
            errors.append(f"sort-range join local state lost cached candidate symbol `{symbol}`")
    required_generator = [
        "sort_range_join_condition_passes_gate",
        "sort_range_join_key_kind",
        "sort_range_join_cardinality_passes_gate",
        "sort_range_join_column_stats_passes_gate",
        "SortRangeUniformHistogram",
        "sort_range_column_stats_for_expr",
        "sort_range_get_column_stats",
        "sort_range_join_selectivity_limit",
        "is_classic_ie_join_candidate",
        "classic_ie_join_selectivity_passes_gate",
        "classic_ie_join_column_stats_passes_gate",
        "CLASSIC_IE_JOIN_MIN_INPUT_PAIRS",
        "SORT_RANGE_JOIN_MIN_INPUT_PAIRS",
    ]
    for symbol in required_generator:
        if symbol not in generator:
            errors.append(f"inequality join planner lost typed/selectivity gate symbol `{symbol}`")
    return errors


def _check_rowstore_boundary_guard() -> list[str]:
    errors = []
    row_mod = _read("crates/storage/src/row/mod.rs")
    row_store = _read("crates/storage/src/row/store.rs")
    row_scan = _read("crates/storage/src/row/scan.rs")
    row_format = _read("crates/storage/src/row/format.rs")

    if "Store-level ordinals are `u64`" not in row_mod:
        errors.append("ROW-BOUNDARY RowStore module docs lost the u64/segmented ordinal boundary")
    if "exceeds u32 ordinal addressability" in row_store:
        errors.append("ROW-BOUNDARY RowStore reintroduced the global u32 ordinal cap")
    if "HashMap<RowAddr, u32>" in row_store or "pub ordinal: u32" in _read("crates/storage/src/row/region.rs"):
        errors.append("ROW-BOUNDARY RowStore store-level ordinal metadata must stay u64")
    if (
        "pub fn pin_ordinal_range(&self, start: u64, len: u64)" not in row_store
        or "pub ordinal_start: u64" not in row_scan
        or "pub ordinal_end: u64" not in row_scan
    ):
        errors.append("ROW-BOUNDARY RowStore pin/scan APIs must expose u64 ordinals")

    for operator_format in ("HashJoinRowFormat", "SortRowFormat", "AggregateGroupFormat"):
        if operator_format in row_format:
            errors.append(
                "paro_storage::row::format must not know operator-specific "
                f"format {operator_format}"
            )
    for rel, symbol in {
        "crates/execution/src/operators/join/hash/row_format.rs": "HashJoinRowFormat",
        "crates/execution/src/operators/sort/row_format.rs": "SortRowFormat",
        "crates/execution/src/operators/aggregate/row_format.rs": "AggregateGroupFormat",
    }.items():
        if symbol not in _read(rel):
            errors.append(f"ROW-BOUNDARY operator-owned row format {symbol} missing from {rel}")

    required_row_format_consumers = {
        "crates/execution/src/spill/probe_spill.rs": [
            "HashJoinRowFormat::probe_spill",
            "RowStoreSpillWriter",
            "RowStoreSpillReader",
            "RowFormatHandle::from_format",
            ".append_chunk(",
            ".read_next(",
            ".finish()",
        ],
        "crates/execution/src/sorting/sorted_run.rs": [
            "SortRowFormat::new",
            "RowStoreSpillWriter",
            ".append_chunk(",
            ".finish()",
        ],
        "crates/execution/src/join_hashtable/build_store.rs": [
            "HashJoinRowFormat::build_spill",
            "RowFormatHandle::from_format",
            ".row_format().logical_types()",
        ],
        "crates/execution/src/operators/aggregate/tuple_layout.rs": [
            "AggregateGroupFormat::new",
            "self.row_format.group_width()",
            "self.row_format.state_count()",
        ],
        "crates/execution/src/runtime/breaker/aggregate.rs": [
            "AggregateGroupFormat::finalized_output",
            "RowStoreSpillWriter",
            ".append_chunk(",
            ".finish()",
        ],
        "crates/execution/src/operators/aggregate/hash/emit.rs": [
            "RowSpillReader",
            ".read_next(",
        ],
    }
    for rel, fragments in required_row_format_consumers.items():
        text = _read(rel)
        for fragment in fragments:
            if fragment not in text:
                errors.append(
                    f"ROW-BOUNDARY row format protocol is not consumed in {rel}: missing `{fragment}`"
                )

    production = "\n".join(
        _production_sources("crates/storage/src/row")
        + _production_sources("crates/execution/src/operators")
        + _production_sources("crates/execution/src/spill")
        + _production_sources("crates/execution/src/sorting")
        + _production_sources("crates/execution/src/join_hashtable")
    )
    if "Box<dyn RowFormat" in production:
        errors.append("ROW-BOUNDARY row format hot paths must not use Box<dyn RowFormat>")
    if "self.count += key.size() as u32" in _read("crates/execution/src/sorting/sorted_run.rs"):
        errors.append("ROW-BOUNDARY sorted run append must fail fast instead of silently wrapping u32 ordinals")
    if "_partition_size" in _read("crates/execution/src/sorting/sorted_run_merger.rs") or "_external" in _read("crates/execution/src/sorting/sorted_run_merger.rs"):
        errors.append("ROW-BOUNDARY SortedRunMerger::new must not keep dead sort emit parameters")

    forbidden_files = [
        "crates/execution/src/operator_state.rs",
        "crates/execution/src/sorting/sort.rs",
        "crates/execution/src/operators/join/hash/runtime.rs",
        "crates/execution/src/physical/specs.rs",
    ]
    for rel in forbidden_files:
        if (REPO_ROOT / rel).exists():
            errors.append(f"ROW-BOUNDARY stale compatibility/monolith file still exists: {rel}")

    required_files = [
        "crates/execution/src/operators/join/hash/keys.rs",
        "crates/execution/src/operators/join/hash/hashing.rs",
        "crates/execution/src/operators/join/hash/payload.rs",
        "crates/execution/src/operators/join/hash/spill.rs",
        "crates/execution/src/operators/join/hash/probe_output.rs",
        "crates/execution/src/operators/join/hash/memory.rs",
        "crates/execution/src/pipeline/lowerer/breaker_lowering.rs",
        "crates/execution/src/pipeline/lowerer/pipeline_dispatch.rs",
        "crates/execution/src/physical/specs/mod.rs",
        "crates/execution/src/physical/specs/scan.rs",
        "crates/execution/src/physical/specs/join.rs",
        "crates/execution/src/physical/specs/aggregate.rs",
        "crates/execution/src/physical/specs/sort.rs",
        "crates/execution/src/physical/specs/window.rs",
        "crates/execution/src/physical/specs/search.rs",
        "crates/execution/src/physical/specs/graph.rs",
        "crates/execution/src/physical/specs/dml.rs",
        "crates/execution/src/physical/specs/external.rs",
        "crates/execution/src/physical/specs/utility.rs",
    ]
    for rel in required_files:
        if not (REPO_ROOT / rel).exists():
            errors.append(f"ROW-BOUNDARY required split file missing: {rel}")
    return errors


def _check_rowset_pushdown_consumption() -> list[str]:
    specs = _read("crates/execution/src/physical/specs/scan.rs")
    generator = _read("crates/execution/src/physical/generator/scan.rs")
    rowset = _read("crates/execution/src/operators/scan/rowset.rs")
    body = _struct_body(specs, "RowsetScanSpec")
    fields = set(re.findall(r"pub\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*:", body))
    errors = []

    if "predicate" in fields and "with_predicates(" not in rowset:
        errors.append("RowsetScanSpec.predicate exists but RowsetSourceExec does not call with_predicates()")
    if "late_materialize" in fields and "with_late_materialize(" not in rowset:
        errors.append(
            "RowsetScanSpec.late_materialize exists but RowsetSourceExec does not call with_late_materialize()"
        )
    if "scan_order" in fields and "reorder_segments(" not in rowset:
        errors.append("RowsetScanSpec.scan_order exists but RowsetSourceExec does not call reorder_segments()")
    if "runtime_filter_expressions" in fields and "get.runtime_filter_expressions" not in generator:
        errors.append(
            "RowsetScanSpec.runtime_filter_expressions exists but physical scan lowering does not consume Get runtime filters"
        )

    pushdown_like = {field for field in fields if field in {"predicate", "late_materialize", "scan_order"}}
    unknown_unconsumed = sorted(
        field
        for field in pushdown_like
        if field not in rowset
    )
    if unknown_unconsumed:
        errors.append(
            "new RowsetScanSpec pushdown fields are not visible in RowsetSourceExec: "
            + ", ".join(unknown_unconsumed)
        )
    return errors


def _check_scheduler_wiring_guard() -> list[str]:
    scheduling = _read("crates/execution/src/runtime/scheduling_policy.rs")
    context = _read("crates/execution/src/runtime/context.rs")
    runtime_mod = _read("crates/execution/src/runtime/mod.rs")
    errors = []
    for symbol in ("PipelineSchedulingPolicy", "PipelineReadyEvent"):
        if symbol not in scheduling:
            errors.append(f"runtime/scheduling_policy.rs lost {symbol}")
        if symbol not in runtime_mod:
            errors.append(f"runtime/mod.rs no longer exports {symbol}; production wiring guard cannot see it")
    if "QueryWakeRegistry" not in context or "take_ready_with_coalesced" not in context:
        errors.append("runtime/context.rs lost producer-side wake coalescing in QueryWakeRegistry")

    scheduler_path = REPO_ROOT / "crates/execution/src/runtime/scheduler.rs"
    if scheduler_path.exists():
        scheduler = scheduler_path.read_text(encoding="utf-8")
        for symbol in ("PipelineSchedulingPolicy", "ReadyEntry", "take_ready_with_coalesced"):
            if symbol not in scheduler:
                errors.append(f"runtime/scheduler.rs exists but does not consume {symbol}")
    production_refs = _production_symbol_refs("PipelineSchedulingPolicy")
    if not production_refs and date.today() >= SCHEDULER_PRODUCTION_WIRING_DEADLINE:
        errors.append(
            "PipelineSchedulingPolicy still has no production consumer after "
            f"{SCHEDULER_PRODUCTION_WIRING_DEADLINE.isoformat()}"
        )
    return errors


def _check_explain_profile_schema_guard() -> list[str]:
    profiler = _strip_test_sections(_read("crates/execution/src/explain/profiler.rs"))
    render = _strip_test_sections(_read("crates/execution/src/explain/analyze_render.rs"))
    types = _strip_test_sections(_read("crates/execution/src/explain/types.rs"))
    scheduler = _strip_test_sections(_read("crates/execution/src/runtime/scheduler.rs"))
    pipeline_driver = _strip_test_sections(
        _read("crates/execution/src/query_executor/pipeline_driver.rs")
    )
    program_executor = _strip_test_sections(
        _read("crates/execution/src/query_executor/program_executor.rs")
    )
    parallel_finish = _strip_test_sections(
        _read("crates/execution/src/runtime/task_executor/parallel_finish.rs")
    )
    hash_build = _strip_test_sections(_read("crates/execution/src/operators/join/hash/build.rs"))
    benchmark_executor = _read("benchmark/harness/executor.py")
    errors = []

    for symbol in (
        "PROFILE_SCHEMA_VERSION",
        "ProfileWorkerContext",
        "ProfileMorselRange",
        "ExplainProfileEvent",
        "local_events",
        "record_query_memory_stats",
        "record_blocked",
        "record_wake",
    ):
        if symbol not in profiler:
            errors.append(f"PROFILE profiler lost schema/local-buffer symbol {symbol}")

    start_body = _function_body(profiler, "start_operator")
    if ".lock()" in start_body or ".lock(" in start_body:
        errors.append("PROFILE OperatorProfiler::start_operator must stay lock-free")
    record_runtime_body = _function_body(profiler, "record_runtime")
    if "push_event" in record_runtime_body or '"runtime"' in record_runtime_body:
        errors.append(
            "PROFILE runtime stats must stay aggregate-only; do not emit a profile event for every counter update"
        )
    if "set_worker_context" in profiler:
        errors.append("PROFILE OperatorProfiler must not keep unused worker-context mutation API")
    if "shared.merge(&self.local, &self.local_events)" not in profiler:
        errors.append("PROFILE profiler must aggregate worker-local stats/events only during flush")

    for field in (
        "scheduler_ready_time_us",
        "scheduler_wait_time_us",
        "scheduler_wake_coalesce_count",
        "output_backpressure_count",
        "runtime_filter_installed_count",
        "runtime_filter_no_wait_count",
        "grant_bytes",
        "revoked_bytes",
        "yield_latency_us",
        "repartition_depth",
    ):
        if field not in types or field not in render:
            errors.append(f"PROFILE runtime profile field {field} is not rendered end-to-end")

    for fragment in (
        '"profile_schema_version"',
        '"query_id"',
        '"profile_events"',
        '"parallelism"',
        '"runtime_filters"',
        '"memory"',
        "profile_summary_for_elapsed",
    ):
        if fragment not in render:
            errors.append(f"PROFILE EXPLAIN ANALYZE render missing {fragment}")

    for rel, source in {
        "runtime/scheduler.rs": scheduler,
        "query_executor/pipeline_driver.rs": pipeline_driver,
        "runtime/task_executor/parallel_finish.rs": parallel_finish,
    }.items():
        if "new_with_context" not in source or "ProfileWorkerContext::new" not in source:
            errors.append(f"PROFILE {rel} must attach pipeline/work/thread profile context")
    if "run_bound_pipeline_runtime" not in program_executor:
        errors.append(
            "PROFILE query_executor/program_executor.rs must delegate control-region "
            "pipelines to the context-aware scheduler runtime"
        )

    if "record_query_memory_stats" not in program_executor or "runtime_stats()" not in program_executor:
        errors.append("PROFILE EXPLAIN ANALYZE must snapshot query memory stats before rendering")
    if "record_runtime_filter_installed" not in hash_build:
        errors.append("PROFILE hash join build must report runtime filter installation")
    if (
        "profile_event_count" not in benchmark_executor
        or "profile_worker_utilization" not in benchmark_executor
        or "explain_profile_overhead_ratio" not in benchmark_executor
    ):
        errors.append("PROFILE benchmark harness must flatten profile schema/overhead fields")
    return errors


def _struct_body(source: str, name: str) -> str:
    match = re.search(rf"struct\s+{re.escape(name)}\s*\{{", source)
    if match is None:
        return ""
    start = match.end()
    depth = 1
    index = start
    while index < len(source) and depth > 0:
        char = source[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        index += 1
    return source[start : index - 1]


def _function_body(source: str, name: str) -> str:
    match = re.search(rf"fn\s+{re.escape(name)}\s*\([^)]*\)\s*(?:->\s*[^\{{]+)?\{{", source)
    if match is None:
        return ""
    start = match.end()
    depth = 1
    index = start
    while index < len(source) and depth > 0:
        char = source[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        index += 1
    return source[start : index - 1]


def _production_symbol_refs(symbol: str) -> list[str]:
    refs = []
    ignored = {
        "crates/execution/src/runtime/scheduling_policy.rs",
        "crates/execution/src/runtime/mod.rs",
    }
    for path in (REPO_ROOT / "crates/execution/src").rglob("*.rs"):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel in ignored or "/tests/" in rel or rel.endswith("_tests.rs"):
            continue
        source = _strip_test_sections(path.read_text(encoding="utf-8"))
        if symbol in source:
            refs.append(rel)
    return refs


def _production_sources(root: str) -> list[str]:
    sources = []
    for path in (REPO_ROOT / root).rglob("*.rs"):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if "/tests/" in rel or rel.endswith("_tests.rs"):
            continue
        sources.append(_strip_test_sections(path.read_text(encoding="utf-8")))
    return sources


def _strip_test_sections(source: str) -> str:
    return re.sub(
        r"(?s)#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*mod\s+tests\s*\{.*\}\s*$",
        "",
        source,
    )


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())

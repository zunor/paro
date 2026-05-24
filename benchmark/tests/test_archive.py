# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness.archive.calibration import (  # noqa: E402
    ArchiveHealth,
    ArchiveHealthStatus,
    append_calibration_observation,
    append_gate_result,
    build_archive_result_payload,
    build_calibration_observation_payload,
    load_calibration_health,
    recompute_calibration_from_manifest,
)
from harness.archive import finalize as archive_finalize  # noqa: E402
from harness.archive.manifest import (  # noqa: E402
    load_manifest,
    rebuild_manifest,
    verify_manifest,
)
from harness.archive.store import (  # noqa: E402
    ArchiveError,
    ArchiveStore,
)
from harness.cli import gate_bisect  # noqa: E402
from harness.performance_gate import GateEnforcement, GateFingerprint, GateOutcome, load_policy  # noqa: E402
from harness.reporter import BenchmarkReporter  # noqa: E402


POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "operator-runtime.toml"


def payload(p50: float = 10.0) -> dict:
    return {
        "workloads": [
            {
                "name": "w",
                "params": {},
                "queries": [
                    {
                        "id": "q",
                        "samples_ms": [p50] * 20,
                        "stats": {"p50": p50, "p99": p50, "throughput_per_second": 1000.0 / p50},
                        "rss": {"peak_kb": 1024.0},
                    }
                ],
            }
        ]
    }


def measurement_for(policy, *, p50: float = 10.0):
    return SimpleNamespace(source=policy.sources[0], payload=payload(p50))


class ArchiveTests(unittest.TestCase):
    def test_append_only_archive_and_manifest_rebuild(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = load_policy(POLICY_PATH)
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            measurement = measurement_for(policy)

            result = append_gate_result(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement],
                run_id="run-1",
            )
            calibration = append_calibration_observation(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement],
                run_id="run-1",
            )

            with self.assertRaisesRegex(ArchiveError, "already exists"):
                append_gate_result(
                    store=store,
                    gate=policy.name,
                    platform="macos-arm64",
                    policy=policy,
                    fingerprint=fingerprint,
                    measurements=[measurement],
                    run_id="run-1",
                )

            rebuild_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )
            manifest = load_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )

            self.assertEqual(manifest.results[0].path, result.relative_path)
            self.assertEqual(manifest.calibrations[0].path, calibration.relative_path)
            calibration_payload = json.loads((store.root / calibration.relative_path).read_text(encoding="utf-8"))
            self.assertEqual(len(calibration_payload["observations"]), 1)
            self.assertEqual(calibration_payload["observations"][0]["sources"][0]["name"], policy.sources[0].name)
            verify_manifest(store, manifest)

    def test_calibration_health_uses_cache_when_archive_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = load_policy(POLICY_PATH)
            policy = replace(
                policy,
                calibration=replace(policy.calibration, min_clean_observations_sql_macro=1),
            )
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            measurement = measurement_for(policy)
            append_calibration_observation(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement],
                run_id="run-1",
            )
            rebuild_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )

            primary = load_calibration_health(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
            )
            self.assertEqual(primary.status, ArchiveHealthStatus.OK)
            self.assertFalse(primary.cache_hit)

            archive_root = store.root
            archive_backup = root / "archive-offline"
            archive_root.rename(archive_backup)
            cached = load_calibration_health(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
            )

            self.assertEqual(cached.status, ArchiveHealthStatus.OK)
            self.assertTrue(cached.cache_hit)

    def test_hard_gate_degrades_to_soft_when_archive_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "missing-archive", cache_root=root / "cache")
            policy = replace(load_policy(POLICY_PATH), enforcement=GateEnforcement.HARD)

            health = load_calibration_health(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
            )

            self.assertEqual(health.status, ArchiveHealthStatus.UNAVAILABLE)
            self.assertEqual(health.policy_enforcement, GateEnforcement.HARD)
            self.assertEqual(health.effective_enforcement, GateEnforcement.SOFT)
            report_path = BenchmarkReporter(root).write_gate_report(
                gate=policy.name,
                outcomes=[GateOutcome(gate=policy.name, enforcement=health.effective_enforcement, entries=())],
                archive_health=health,
            )
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["archive"]["status"], "ArchiveUnavailable")

    def test_corrupt_calibration_is_reported_when_cache_cannot_verify(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = replace(
                load_policy(POLICY_PATH),
                calibration=replace(load_policy(POLICY_PATH).calibration, min_clean_observations_sql_macro=1),
            )
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            measurement = measurement_for(policy)
            calibration = append_calibration_observation(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement],
                run_id="run-1",
            )
            rebuild_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )
            (store.root / calibration.relative_path).write_text("{broken", encoding="utf-8")

            health = load_calibration_health(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
            )

            self.assertEqual(health.status, ArchiveHealthStatus.CALIBRATION_CORRUPT)

    def test_primary_corruption_does_not_fall_back_to_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = replace(
                load_policy(POLICY_PATH),
                calibration=replace(load_policy(POLICY_PATH).calibration, min_clean_observations_sql_macro=1),
            )
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            measurement = measurement_for(policy)
            calibration = append_calibration_observation(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement],
                run_id="run-1",
            )
            rebuild_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )
            self.assertEqual(
                load_calibration_health(
                    store=store,
                    gate=policy.name,
                    platform="macos-arm64",
                    policy=policy,
                ).status,
                ArchiveHealthStatus.OK,
            )
            (store.root / calibration.relative_path).write_text("{broken", encoding="utf-8")

            health = load_calibration_health(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
            )

            self.assertEqual(health.status, ArchiveHealthStatus.CALIBRATION_CORRUPT)

    def test_latest_calibration_uses_timestamp_not_lexicographic_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = replace(
                load_policy(POLICY_PATH),
                calibration=replace(load_policy(POLICY_PATH).calibration, min_clean_observations_sql_macro=1),
            )
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            old_payload = build_calibration_observation_payload(
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[],
                run_id="old",
            ) | {"observations": []}
            new_payload = build_calibration_observation_payload(
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement_for(policy)],
                run_id="new",
            )
            old_path = f"calibrations/{policy.name}/macos-arm64/policy-v{policy.version}/recompute-20260101T000000Z.json"
            new_path = f"calibrations/{policy.name}/macos-arm64/policy-v{policy.version}/0abc-20260102T000000Z-run.json"
            store.write_json_append_only(old_path, old_payload)
            store.write_json_append_only(new_path, new_payload)
            rebuild_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )

            health = load_calibration_health(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
            )

            self.assertEqual(health.status, ArchiveHealthStatus.OK)
            self.assertEqual(health.calibration_path, new_path)

    def test_recompute_calibration_from_manifest_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = load_policy(POLICY_PATH)
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            measurement = measurement_for(policy)
            append_gate_result(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement],
                run_id="run-1",
            )
            rebuild_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )
            manifest = load_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )

            write = recompute_calibration_from_manifest(
                store=store,
                manifest=manifest,
                policy=policy,
            )
            payload_data = json.loads((store.root / write.relative_path).read_text(encoding="utf-8"))

            self.assertEqual(payload_data["kind"], "calibration_recompute")
            self.assertEqual(len(payload_data["observations"]), 1)

    def test_recompute_uses_policy_rolling_window_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = replace(
                load_policy(POLICY_PATH),
                calibration=replace(load_policy(POLICY_PATH).calibration, rolling_window_sql_macro=1),
            )
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            for run_id, p50 in (("run-1", 10.0), ("run-2", 20.0)):
                append_gate_result(
                    store=store,
                    gate=policy.name,
                    platform="macos-arm64",
                    policy=policy,
                    fingerprint=fingerprint,
                    measurements=[measurement_for(policy, p50=p50)],
                    run_id=run_id,
                )
            rebuild_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )
            manifest = load_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )

            write = recompute_calibration_from_manifest(
                store=store,
                manifest=manifest,
                policy=policy,
            )
            recomputed = json.loads((store.root / write.relative_path).read_text(encoding="utf-8"))
            stats = recomputed["observations"][0]["sources"][0]["payload"]["workloads"][0]["queries"][0]["stats"]

            self.assertEqual(len(recomputed["observations"]), 1)
            self.assertEqual(stats["p50"], 20.0)

    def test_archive_finalize_rebuilds_manifest_and_calibration_in_one_process(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = replace(
                load_policy(POLICY_PATH),
                calibration=replace(load_policy(POLICY_PATH).calibration, rolling_window_sql_macro=1),
            )
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            append_gate_result(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement_for(policy)],
                run_id="run-1",
            )

            with redirect_stdout(io.StringIO()):
                exit_code = archive_finalize.main([
                    "--archive",
                    str(store.root),
                    "--cache",
                    str(store.cache_root),
                    "--gate",
                    policy.name,
                    "--platform",
                    "macos-arm64",
                    "--policy",
                    str(POLICY_PATH),
                ])

            manifest = load_manifest(
                store,
                gate=policy.name,
                platform="macos-arm64",
                policy_version=policy.version,
            )
            self.assertEqual(exit_code, 0)
            self.assertEqual(len(manifest.results), 1)
            self.assertEqual(len(manifest.calibrations), 1)

    def test_bisect_loads_matching_archived_gate_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = load_policy(POLICY_PATH)
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            write = append_gate_result(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement_for(policy)],
                run_id="bisect-1",
            )
            commit = Path(write.relative_path).name.split("-", 1)[0]

            payload_data = gate_bisect.load_archived_gate_result(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                against=commit,
            )
            sources = gate_bisect.archived_source_payloads(payload_data)

            self.assertIn(policy.sources[0].name, sources)
            self.assertEqual(sources[policy.sources[0].name]["workloads"][0]["queries"][0]["id"], "q")

    def test_bisect_rejects_ambiguous_archive_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = load_policy(POLICY_PATH)
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            for run_id in ("bisect-a", "bisect-b"):
                append_gate_result(
                    store=store,
                    gate=policy.name,
                    platform="macos-arm64",
                    policy=policy,
                    fingerprint=fingerprint,
                    measurements=[measurement_for(policy)],
                    run_id=run_id,
                )
            prefix = store.list_json(f"results/{policy.name}/macos-arm64")[0].split("/")[-1][:8]

            with self.assertRaisesRegex(Exception, "ambiguous"):
                gate_bisect.load_archived_gate_result(
                    store=store,
                    gate=policy.name,
                    platform="macos-arm64",
                    policy=policy,
                    against=prefix,
                )

    def test_bisect_rejects_policy_sha_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = load_policy(POLICY_PATH)
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            payload_data = build_archive_result_payload(
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement_for(policy)],
                run_id="bisect-mismatch",
            )
            payload_data["policy"]["sha256"] = "0" * 64
            relative_path = f"results/{policy.name}/macos-arm64/abcdef123456-bisect-mismatch.json"
            store.write_json_append_only(relative_path, payload_data)

            with self.assertRaisesRegex(Exception, "different policy"):
                gate_bisect.load_archived_gate_result(
                    store=store,
                    gate=policy.name,
                    platform="macos-arm64",
                    policy=policy,
                    against="abcdef123456",
                )

    def test_bisect_requires_current_source_in_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = load_policy(POLICY_PATH)
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            payload_data = build_archive_result_payload(
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[],
                run_id="bisect-missing-source",
            )
            store.write_json_append_only(
                f"results/{policy.name}/macos-arm64/abcdef123456-bisect-missing-source.json",
                payload_data,
            )
            summary_path = root / "summary.md"
            summary_path.write_text("", encoding="utf-8")
            measurement = measurement_for(policy)
            measurement = SimpleNamespace(
                source=measurement.source,
                payload=measurement.payload,
                failed=False,
                summary_path=summary_path,
            )
            args = SimpleNamespace(
                gate=policy.name,
                policy="auto",
                archive="auto",
                archive_cache="auto",
                against="abcdef123456",
                pid="auto",
                include_source=[],
                skip_source=[],
            )

            with (
                mock.patch.object(gate_bisect, "load_policy_for_gate", return_value=policy),
                mock.patch.object(gate_bisect, "archive_store", return_value=store),
                mock.patch.object(gate_bisect, "platform_key", return_value="macos-arm64"),
                mock.patch.object(gate_bisect, "resolve_pid", return_value=0),
                mock.patch.object(gate_bisect, "load_staging_queries_checked", return_value={}),
                mock.patch.object(gate_bisect, "run_sources", return_value=[measurement]),
                mock.patch.object(
                    gate_bisect,
                    "load_calibration_health",
                    return_value=ArchiveHealth(
                        status=ArchiveHealthStatus.DISABLED,
                        message="disabled",
                        policy_enforcement=policy.enforcement,
                        effective_enforcement=policy.enforcement,
                    ),
                ),
            ):
                with self.assertRaisesRegex(gate_bisect.GateCommandError, "no source"):
                    gate_bisect.run_bisect(args, root_dir=root, runner_module=object())

    def test_bisect_does_not_write_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            policy = load_policy(POLICY_PATH)
            fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
            write = append_gate_result(
                store=store,
                gate=policy.name,
                platform="macos-arm64",
                policy=policy,
                fingerprint=fingerprint,
                measurements=[measurement_for(policy)],
                run_id="bisect-read-only",
            )
            commit = Path(write.relative_path).name.split("-", 1)[0]
            summary_path = root / "summary.md"
            summary_path.write_text("", encoding="utf-8")
            measurement = measurement_for(policy)
            measurement = SimpleNamespace(
                source=measurement.source,
                payload=measurement.payload,
                failed=False,
                summary_path=summary_path,
            )
            args = SimpleNamespace(
                gate=policy.name,
                policy="auto",
                archive="auto",
                archive_cache="auto",
                against=commit,
                pid="auto",
                include_source=[],
                skip_source=[],
            )
            before = store.list_json(f"results/{policy.name}/macos-arm64")

            with (
                mock.patch.object(gate_bisect, "load_policy_for_gate", return_value=policy),
                mock.patch.object(gate_bisect, "archive_store", return_value=store),
                mock.patch.object(gate_bisect, "platform_key", return_value="macos-arm64"),
                mock.patch.object(gate_bisect, "resolve_pid", return_value=0),
                mock.patch.object(gate_bisect, "load_staging_queries_checked", return_value={}),
                mock.patch.object(gate_bisect, "run_sources", return_value=[measurement]),
                mock.patch.object(
                    gate_bisect,
                    "load_calibration_health",
                    return_value=ArchiveHealth(
                        status=ArchiveHealthStatus.DISABLED,
                        message="disabled",
                        policy_enforcement=policy.enforcement,
                        effective_enforcement=policy.enforcement,
                    ),
                ),
            ):
                with redirect_stdout(io.StringIO()):
                    exit_code = gate_bisect.run_bisect(args, root_dir=root, runner_module=object())

            self.assertEqual(exit_code, 0)
            self.assertEqual(store.list_json(f"results/{policy.name}/macos-arm64"), before)

    def test_retention_plan_lists_old_archive_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArchiveStore(root=root / "archive", cache_root=root / "cache")
            path = store.root / "results" / "operator-runtime" / "macos-arm64" / "old.json"
            path.parent.mkdir(parents=True)
            path.write_text("{}", encoding="utf-8")
            old = datetime.now(timezone.utc) - timedelta(days=800)
            os.utime(path, (old.timestamp(), old.timestamp()))

            candidates = store.retention_candidates(
                "results/operator-runtime/macos-arm64",
                older_than_days=730,
            )

            self.assertEqual([candidate.relative_path for candidate in candidates], [
                "results/operator-runtime/macos-arm64/old.json"
            ])


if __name__ == "__main__":
    unittest.main()

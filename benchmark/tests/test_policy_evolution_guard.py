# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools" / "ci"))

from check_policy_evolution import _validate  # noqa: E402


class PolicyEvolutionGuardTests(unittest.TestCase):
    def test_shadow_policy_does_not_require_platform_baselines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_policy(root, gate="operator-runtime", enforcement="shadow")

            errors = _validate(
                repo_root=root,
                available_platforms=set(),
                required_platforms={"linux-amd64", "macos-arm64"},
            )

            self.assertEqual(errors, [])

    def test_non_shadow_policy_requires_all_platform_runners(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_policy(root, gate="operator-runtime", enforcement="soft")

            errors = _validate(
                repo_root=root,
                available_platforms={"linux-amd64"},
                required_platforms={"linux-amd64", "macos-arm64"},
            )

            self.assertTrue(any("macos-arm64" in error for error in errors))

    def test_non_shadow_policy_requires_matching_baseline_sha(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            policy_sha = _write_policy(root, gate="operator-runtime", enforcement="soft", version=7)
            _write_baseline(root, gate="operator-runtime", platform="linux-amd64", version=7, sha=policy_sha)
            _write_baseline(root, gate="operator-runtime", platform="macos-arm64", version=7, sha=policy_sha)
            _write_calibration(root, gate="operator-runtime", platform="linux-amd64", version=7, observations=30)
            _write_calibration(root, gate="operator-runtime", platform="macos-arm64", version=7, observations=30)

            errors = _validate(
                repo_root=root,
                available_platforms={"linux-amd64", "macos-arm64"},
                required_platforms={"linux-amd64", "macos-arm64"},
            )

            self.assertEqual(errors, [])

    def test_non_shadow_policy_rejects_low_fingerprint_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            policy_sha = _write_policy(root, gate="operator-runtime", enforcement="hard")
            _write_baseline(
                root,
                gate="operator-runtime",
                platform="linux-amd64",
                version=1,
                sha=policy_sha,
                build_fingerprint={"rust_toolchain": "unknown", "cargo": "unknown"},
            )
            _write_calibration(root, gate="operator-runtime", platform="linux-amd64", version=1, observations=30)

            errors = _validate(
                repo_root=root,
                available_platforms={"linux-amd64"},
                required_platforms={"linux-amd64"},
            )

            self.assertTrue(any("coverage" in error for error in errors), errors)

    def test_non_shadow_policy_requires_archive_observation_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            policy_sha = _write_policy(root, gate="operator-runtime", enforcement="soft")
            _write_baseline(
                root,
                gate="operator-runtime",
                platform="linux-amd64",
                version=1,
                sha=policy_sha,
            )
            _write_calibration(
                root,
                gate="operator-runtime",
                platform="linux-amd64",
                version=1,
                observations=29,
            )

            errors = _validate(
                repo_root=root,
                available_platforms={"linux-amd64"},
                required_platforms={"linux-amd64"},
            )

            self.assertTrue(any("29/30" in error for error in errors), errors)


def _write_policy(root: Path, *, gate: str, enforcement: str, version: int = 1) -> str:
    policy_dir = root / "benchmark" / "policies"
    policy_dir.mkdir(parents=True, exist_ok=True)
    payload = f"""
schema_version = 1
name = "{gate}"
version = {version}
enforcement = "{enforcement}"

[[sources]]
name = "{gate}-sql"
type = "sql_suite"
suite = "{gate}"
measurement_class = "sql_macro"

[metrics]
required = ["p50"]

[thresholds]
latency_regression_percent = 15.0

[noise_floors]
latency_ms = 1.0

[absolute_max]

[absolute_min]

[statistics]
model = "mann_whitney_rolling_p95_holm"
family_alpha_first_run = 0.05
family_alpha_confirmed = 0.01
false_positive_target = 0.02
true_positive_target = 0.95

[calibration]
source = "archive"
min_clean_observations_sql_macro = 30
min_clean_observations_rust_micro = 50
rolling_window_sql_macro = 30
rolling_window_rust_micro = 50
noise_floor_formula = "directional_rolling_p95_delta"

[coverage]
new_query_staging_days = 14
""".lstrip()
    path = policy_dir / f"{gate}.toml"
    path.write_text(payload, encoding="utf-8")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _write_baseline(
    root: Path,
    *,
    gate: str,
    platform: str,
    version: int,
    sha: str,
    build_fingerprint: dict[str, str] | None = None,
) -> None:
    baseline_dir = root / "benchmark" / "baselines" / gate
    baseline_dir.mkdir(parents=True, exist_ok=True)
    (baseline_dir / f"{platform}.json").write_text(
        json.dumps({
            "schema_version": 2,
            "gate": gate,
            "platform": platform,
            "policy": {"name": gate, "version": version, "sha256": sha},
            "system_fingerprint": {"os": platform},
            "build_fingerprint": build_fingerprint or {"rust_toolchain": "rustc-test"},
            "runtime_fingerprint": {"data_dir_mode": "fresh"},
            "measurements": {
                "sources": [
                    {
                        "name": f"{gate}-sql",
                        "type": "sql_suite",
                        "measurement_class": "sql_macro",
                        "payload": {"version": 2, "workloads": []},
                    }
                ]
            },
        }),
        encoding="utf-8",
    )


def _write_calibration(
    root: Path,
    *,
    gate: str,
    platform: str,
    version: int,
    observations: int,
) -> None:
    archive = root / ".ci" / "paro-perf-archive"
    relative = f"calibrations/{gate}/{platform}/policy-v{version}/calibration.json"
    calibration_path = archive / relative
    calibration_path.parent.mkdir(parents=True, exist_ok=True)
    calibration_bytes = json.dumps(
        {"observations": [{"sources": []} for _ in range(observations)]},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    calibration_path.write_bytes(calibration_bytes)

    manifest_path = archive / f"manifests/{gate}/{platform}/policy-v{version}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "gate": gate,
                "platform": platform,
                "policy_version": version,
                "results": [],
                "calibrations": [
                    {
                        "path": relative,
                        "sha256": hashlib.sha256(calibration_bytes).hexdigest(),
                        "size_bytes": len(calibration_bytes),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    unittest.main()

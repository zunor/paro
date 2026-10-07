# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import unittest
import tempfile
from pathlib import Path

from check_plan_boundaries import dependency_path, production_dependencies, check_optimizer_surface


class PlanBoundaryTests(unittest.TestCase):
    def test_public_api_and_fixture_feature_are_explicit(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "crates/optimizer/src"
            source.mkdir(parents=True)
            facade = source / "lib.rs"
            fixture = '#[cfg(feature = "test-support")]\npub mod test_support {}\n'
            # Match a real block declaration, not a one-line lookalike.
            fixture = fixture.replace('{}', '{\n}')
            facade.write_text(fixture)
            self.assertEqual(check_optimizer_surface(root), [])
            facade.write_text(fixture + "pub mod cost;\n")
            self.assertTrue(check_optimizer_surface(root))
            facade.write_text(fixture.replace('#[cfg(feature = "test-support")]\n', ''))
            self.assertTrue(check_optimizer_surface(root))

    def test_transitive_dependency_is_not_hidden(self):
        graph = {"execution": {"helper"}, "helper": {"optimizer"}}
        self.assertEqual(
            dependency_path(graph, "execution", "optimizer"),
            ["execution", "helper", "optimizer"],
        )

    def test_cycle_terminates(self):
        self.assertIsNone(dependency_path({"a": {"b"}, "b": {"a"}}, "a", "optimizer"))

    def test_dev_dependency_does_not_change_production_boundary(self):
        manifest = {"dev-dependencies": {"paro-optimizer": "1"}}
        self.assertEqual(list(production_dependencies(manifest, {})), [])

    def test_target_and_workspace_alias_cannot_bypass_guard(self):
        manifest = {
            "target": {"cfg(unix)": {"dependencies": {"alias": {"workspace": True}}}}
        }
        workspace = {"alias": {"package": "paro-optimizer", "version": "1"}}
        self.assertEqual(
            list(production_dependencies(manifest, workspace)), ["paro-optimizer"]
        )

    def test_optional_build_dependency_is_still_a_possible_production_edge(self):
        manifest = {
            "build-dependencies": {
                "alias": {"package": "paro-optimizer", "optional": True}
            }
        }
        self.assertEqual(
            list(production_dependencies(manifest, {})), ["paro-optimizer"]
        )


if __name__ == "__main__":
    unittest.main()

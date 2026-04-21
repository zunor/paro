import tempfile
import textwrap
import unittest
from pathlib import Path

from paro_runtime_worker.module_loader import ModuleLoader, ModuleRequest


class ModuleLoaderTests(unittest.TestCase):
    def test_module_loader_caches_modules_loaded_from_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = Path(tmp_dir) / "worker_case.py"
            module_path.write_text(
                textwrap.dedent(
                    """
                    from paro_udf import batch_udf, register_compiled_kernel, register_native_jit_kernel

                    VALUE = 41

                    def answer_fast():
                        return VALUE + 100

                    def answer_jit():
                        return VALUE + 200

                    @register_compiled_kernel(kind="numba", entrypoint="answer_fast")
                    @register_native_jit_kernel(entrypoint="answer_jit")
                    @batch_udf(return_type="int64")
                    def answer():
                        return VALUE + 1

                    @batch_udf(return_type="int64")
                    def plus_one():
                        return VALUE + 1

                    @batch_udf(return_type="int64")
                    def plus_two():
                        return VALUE + 2
                    """
                )
            )

            loader = ModuleLoader()
            request = ModuleRequest(module_path=str(module_path), handler="answer")
            first = loader.load_handler(request)
            second = loader.load_handler(request)

            self.assertEqual(first(), 42)
            self.assertIs(first, second)

    def test_module_loader_resolves_compiled_kernel_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = Path(tmp_dir) / "worker_case.py"
            module_path.write_text(
                textwrap.dedent(
                    """
                    from paro_udf import batch_udf, register_compiled_kernel, register_native_jit_kernel

                    def answer_fast():
                        return 141

                    def answer_jit():
                        return 242

                    @register_compiled_kernel(kind="numba", entrypoint="answer_fast")
                    @register_native_jit_kernel(entrypoint="answer_jit")
                    @batch_udf(return_type="int64")
                    def answer():
                        return 42
                    """
                )
            )

            loader = ModuleLoader()
            request = ModuleRequest(module_path=str(module_path), handler="answer")
            resolved = loader.load_execution_target(
                request,
                execution_backend="compiled_kernel",
                compiled_kernel_kind="numba",
            )

            self.assertEqual(resolved.target(), 141)
            self.assertEqual(resolved.compiled_kind, "numba")

    def test_module_loader_resolves_native_jit_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = Path(tmp_dir) / "worker_case.py"
            module_path.write_text(
                textwrap.dedent(
                    """
                    from paro_udf import batch_udf, register_native_jit_kernel

                    def answer_jit():
                        return 242

                    @register_native_jit_kernel(entrypoint="answer_jit")
                    @batch_udf(return_type="int64")
                    def answer():
                        return 42
                    """
                )
            )

            loader = ModuleLoader()
            request = ModuleRequest(module_path=str(module_path), handler="answer")
            resolved = loader.load_execution_target(
                request,
                execution_backend="compiled_kernel",
                compiled_kernel_kind="jit",
            )

            self.assertEqual(resolved.target(), 242)
            self.assertEqual(resolved.compiled_kind, "jit")

    def test_module_loader_builds_execution_chain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = Path(tmp_dir) / "worker_case.py"
            module_path.write_text(
                textwrap.dedent(
                    """
                    from paro_udf import batch_udf

                    @batch_udf(return_type="int64")
                    def first():
                        return 1

                    @batch_udf(return_type="int64")
                    def second():
                        return 2
                    """
                )
            )

            loader = ModuleLoader()
            request = ModuleRequest(module_path=str(module_path), handler="first")
            chain = loader.load_execution_chain(
                request,
                additional_handlers=("second",),
            )

            self.assertEqual(len(chain), 2)
            self.assertEqual(chain[0].target(), 1)
            self.assertEqual(chain[1].target(), 2)


if __name__ == "__main__":
    unittest.main()

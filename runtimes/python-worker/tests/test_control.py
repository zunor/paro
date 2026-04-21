import tempfile
import textwrap
import unittest
import warnings
from pathlib import Path

from paro_runtime_worker.column_decoder import ColumnDecoder
from paro_runtime_worker.column_encoder import ColumnEncoder
from paro_runtime_worker.control import ControlLoop, SubmissionState
from paro_runtime_worker.protocol.control_header import ControlHeader, ControlMessageKind
from paro_udf.column import Column, SlowPathWarning


def _write_module(tmpdir: Path) -> Path:
    module_path = tmpdir / "sample_udf.py"
    module_path.write_text(
        textwrap.dedent(
            """
            from paro_udf import batch_udf, register_compiled_kernel, register_native_jit_kernel

            def compiled_double(ctx, values):
                return [value * 10 for value in values.materialize_py()]

            def jit_double(ctx, values):
                return [value * 100 for value in values.materialize_py()]

            @register_compiled_kernel(kind="numba", entrypoint="compiled_double")
            @register_native_jit_kernel(entrypoint="jit_double")
            @batch_udf(return_type="int64")
            def double(ctx, values):
                return [value * 2 for value in values.materialize_py()]

            @batch_udf(return_type="int64")
            def explode(ctx, values):
                raise ValueError(f"boom:{ctx.batch_id}")

            @batch_udf(return_type="int64")
            def explode_long(ctx, values):
                raise ValueError("boom:" + ("x" * 20000))

            @batch_udf(return_type="int64")
            def expand(ctx, values):
                output = []
                for value in values.materialize_py():
                    output.extend((value, value + 100))
                return output

            @batch_udf(return_type="int64")
            def add_one(ctx, values):
                return [value + 1 for value in values.materialize_py()]

            @batch_udf(return_type="int64")
            def square(ctx, values):
                return [value * value for value in values.materialize_py()]

            @batch_udf(return_type="int64")
            def import_subprocess(ctx, values):
                import subprocess  # noqa: F401
                return values.materialize_py()
            """
        )
    )
    return module_path


class ControlLoopTests(unittest.TestCase):
    def test_control_loop_executes_submit_and_returns_completed_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = _write_module(Path(tmp_dir))
            input_batch = ColumnEncoder().encode_columns(
                [Column([1, 2, 3], logical_type="int64", name="numbers")]
            )
            payload = {
                "module_path": str(module_path),
                "handler": "double",
                "lease": input_batch.to_payload(
                    lease_id=11,
                    ownership={
                        "owner_worker_epoch": 1,
                        "owner_host_epoch": 7,
                        "owner_query_epoch": 3,
                    },
                ),
                "buffers": input_batch.buffers,
                "context": {
                    "query_id": 9,
                    "routine_identity": "routine@9",
                    "capability_profile": "compiled_kernel",
                },
                "return_type": "int64",
                "output_names": ["score"],
                "ownership": {
                    "owner_worker_epoch": 1,
                    "owner_host_epoch": 7,
                    "owner_query_epoch": 3,
                },
                "completion_fence": 99,
            }
            header = ControlHeader.new(
                ControlMessageKind.SUBMIT,
                batch_id=7,
                lease_id=11,
                payload_len=0,
            )

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                response = ControlLoop().handle(header.encode(), payload)

            self.assertEqual(response.kind, ControlMessageKind.COMPLETE)
            self.assertEqual(response.lifecycle.state, SubmissionState.FINISHED)
            self.assertEqual(response.payload["lease"]["completion_fence"], 99)

            decoded = ColumnDecoder().decode_batch(
                response.payload["lease"],
                response.payload["buffers"],
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
            self.assertEqual(decoded.columns[0].materialize_py(), [2, 4, 6])

    def test_control_loop_uses_compiled_kernel_candidate_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = _write_module(Path(tmp_dir))
            input_batch = ColumnEncoder().encode_columns(
                [Column([1, 2, 3], logical_type="int64", name="numbers")]
            )
            payload = {
                "module_path": str(module_path),
                "handler": "double",
                "lease": input_batch.to_payload(lease_id=11),
                "buffers": input_batch.buffers,
                "context": {
                    "capability_profile": "compiled_kernel",
                    "execution_backend": "compiled_kernel",
                    "metadata": {"compiled_kernel_kind": "numba"},
                },
                "return_type": "int64",
                "output_names": ["score"],
            }
            header = ControlHeader.new(
                ControlMessageKind.SUBMIT,
                batch_id=8,
                lease_id=11,
                payload_len=0,
            )

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                response = ControlLoop().handle(header.encode(), payload)

            self.assertEqual(response.kind, ControlMessageKind.COMPLETE)
            decoded = ColumnDecoder().decode_batch(
                response.payload["lease"],
                response.payload["buffers"],
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                self.assertEqual(decoded.columns[0].materialize_py(), [10, 20, 30])

    def test_control_loop_uses_native_jit_candidate_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = _write_module(Path(tmp_dir))
            input_batch = ColumnEncoder().encode_columns(
                [Column([1, 2], logical_type="int64", name="numbers")]
            )
            payload = {
                "module_path": str(module_path),
                "handler": "double",
                "lease": input_batch.to_payload(lease_id=13),
                "buffers": input_batch.buffers,
                "context": {
                    "capability_profile": "compiled_jit",
                    "execution_backend": "compiled_kernel",
                    "metadata": {"compiled_kernel_kind": "jit"},
                },
                "return_type": "int64",
                "output_names": ["score"],
            }
            header = ControlHeader.new(
                ControlMessageKind.SUBMIT,
                batch_id=9,
                lease_id=13,
                payload_len=0,
            )

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                response = ControlLoop().handle(header.encode(), payload)

            self.assertEqual(response.kind, ControlMessageKind.COMPLETE)
            decoded = ColumnDecoder().decode_batch(
                response.payload["lease"],
                response.payload["buffers"],
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                self.assertEqual(decoded.columns[0].materialize_py(), [100, 200])

    def test_control_loop_executes_in_subinterpreter_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = _write_module(Path(tmp_dir))
            input_batch = ColumnEncoder().encode_columns(
                [Column([2, 4], logical_type="int64", name="numbers")]
            )
            payload = {
                "module_path": str(module_path),
                "handler": "double",
                "lease": input_batch.to_payload(lease_id=19),
                "buffers": input_batch.buffers,
                "context": {
                    "capability_profile": "trusted_subinterpreter",
                    "execution_backend": "subinterpreter",
                },
                "return_type": "int64",
                "output_names": ["score"],
            }
            header = ControlHeader.new(
                ControlMessageKind.SUBMIT,
                batch_id=22,
                lease_id=19,
                payload_len=0,
            )

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                response = ControlLoop().handle(header.encode(), payload)

            self.assertEqual(response.kind, ControlMessageKind.COMPLETE)
            decoded = ColumnDecoder().decode_batch(
                response.payload["lease"],
                response.payload["buffers"],
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                self.assertEqual(decoded.columns[0].materialize_py(), [4, 8])

    def test_subinterpreter_policy_blocks_disallowed_imports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = _write_module(Path(tmp_dir))
            input_batch = ColumnEncoder().encode_columns(
                [Column([2], logical_type="int64", name="numbers")]
            )
            payload = {
                "module_path": str(module_path),
                "handler": "import_subprocess",
                "lease": input_batch.to_payload(lease_id=23),
                "buffers": input_batch.buffers,
                "context": {
                    "capability_profile": "trusted_subinterpreter",
                    "execution_backend": "subinterpreter",
                    "metadata": {
                        "subinterpreter_policy": {
                            "import_policy": "allow_list",
                            "allowed_modules": ["math"],
                            "extension_modules": "deny_all",
                        }
                    },
                },
                "return_type": "int64",
                "output_names": ["score"],
            }
            header = ControlHeader.new(
                ControlMessageKind.SUBMIT,
                batch_id=24,
                lease_id=23,
                payload_len=0,
            )

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                response = ControlLoop().handle(header.encode(), payload)

            self.assertEqual(response.kind, ControlMessageKind.ERROR)
            self.assertIn("not allowed by sub-interpreter policy", response.payload["message"])

    def test_control_loop_serializes_python_tracebacks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = _write_module(Path(tmp_dir))
            input_batch = ColumnEncoder().encode_columns(
                [Column([1], logical_type="int64", name="numbers")]
            )
            payload = {
                "module_path": str(module_path),
                "handler": "explode",
                "lease": input_batch.to_payload(lease_id=11),
                "buffers": input_batch.buffers,
                "context": {},
                "return_type": "int64",
            }
            header = ControlHeader.new(
                ControlMessageKind.SUBMIT,
                batch_id=12,
                lease_id=11,
                payload_len=0,
            )

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                response = ControlLoop().handle(header.encode(), payload)

            self.assertEqual(response.kind, ControlMessageKind.ERROR)
            self.assertEqual(response.lifecycle.state, SubmissionState.FAILED)
            self.assertEqual(response.payload["exception_type"], "ValueError")
            self.assertIn("boom:12", response.payload["message"])
            self.assertIn("explode", response.payload["formatted_traceback"])
            self.assertFalse(response.payload["truncated"])

    def test_control_loop_truncates_oversized_tracebacks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = _write_module(Path(tmp_dir))
            input_batch = ColumnEncoder().encode_columns(
                [Column([1], logical_type="int64", name="numbers")]
            )
            payload = {
                "module_path": str(module_path),
                "handler": "explode_long",
                "lease": input_batch.to_payload(lease_id=11),
                "buffers": input_batch.buffers,
                "context": {},
                "return_type": "int64",
            }
            header = ControlHeader.new(
                ControlMessageKind.SUBMIT,
                batch_id=13,
                lease_id=11,
                payload_len=0,
            )

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                response = ControlLoop().handle(header.encode(), payload)

            self.assertEqual(response.kind, ControlMessageKind.ERROR)
            self.assertTrue(response.payload["truncated"])
            self.assertLessEqual(len(response.payload["message"]), 2_048)
            self.assertLessEqual(len(response.payload["formatted_traceback"]), 16_384)
            self.assertIn("truncated", response.payload["formatted_traceback"])

    def test_cancel_before_submit_keeps_batch_cancelled(self) -> None:
        loop = ControlLoop()
        cancel_header = ControlHeader.new(
            ControlMessageKind.CANCEL,
            batch_id=99,
            lease_id=5,
            payload_len=0,
        )
        cancel_response = loop.handle(cancel_header.encode())
        self.assertEqual(cancel_response.payload["state"], SubmissionState.CANCELLED.value)

        submit_header = ControlHeader.new(
            ControlMessageKind.SUBMIT,
            batch_id=99,
            lease_id=5,
            payload_len=0,
        )
        submit_response = loop.handle(submit_header.encode(), {"lease": {"lease_id": 5, "row_count": 0, "state": "Committed", "ownership": {}, "columns": []}})
        self.assertEqual(submit_response.payload["state"], SubmissionState.CANCELLED.value)
        self.assertEqual(submit_response.lifecycle.state, SubmissionState.CANCELLED)

    def test_control_loop_allows_relation_expanding_results_when_row_count_is_unbounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = _write_module(Path(tmp_dir))
            input_batch = ColumnEncoder().encode_columns(
                [Column([3, 5], logical_type="int64", name="numbers")]
            )
            payload = {
                "module_path": str(module_path),
                "handler": "expand",
                "lease": input_batch.to_payload(lease_id=17),
                "buffers": input_batch.buffers,
                "context": {},
                "return_type": "int64",
                "expected_row_count": None,
                "output_names": ["value"],
            }
            header = ControlHeader.new(
                ControlMessageKind.SUBMIT,
                batch_id=21,
                lease_id=17,
                payload_len=0,
            )

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                response = ControlLoop().handle(header.encode(), payload)

            self.assertEqual(response.kind, ControlMessageKind.COMPLETE)
            decoded = ColumnDecoder().decode_batch(
                response.payload["lease"],
                response.payload["buffers"],
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                self.assertEqual(decoded.columns[0].materialize_py(), [3, 103, 5, 105])

    def test_control_loop_executes_row_preserving_kernel_fusion_chain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            module_path = _write_module(Path(tmp_dir))
            input_batch = ColumnEncoder().encode_columns(
                [Column([2, 4], logical_type="int64", name="numbers")]
            )
            payload = {
                "module_path": str(module_path),
                "handler": "add_one",
                "lease": input_batch.to_payload(lease_id=27),
                "buffers": input_batch.buffers,
                "context": {
                    "metadata": {
                        "kernel_fusion": {
                            "mode": "row_preserving_chain",
                            "handlers": ["square"],
                        }
                    }
                },
                "return_type": "int64",
                "expected_row_count": 2,
                "output_names": ["score"],
            }
            header = ControlHeader.new(
                ControlMessageKind.SUBMIT,
                batch_id=28,
                lease_id=27,
                payload_len=0,
            )

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                response = ControlLoop().handle(header.encode(), payload)

            self.assertEqual(response.kind, ControlMessageKind.COMPLETE)
            decoded = ColumnDecoder().decode_batch(
                response.payload["lease"],
                response.payload["buffers"],
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SlowPathWarning)
                self.assertEqual(decoded.columns[0].materialize_py(), [9, 25])


if __name__ == "__main__":
    unittest.main()

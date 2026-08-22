import argparse
import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).with_name("mininet-mptcp-minimal-run.py")
SPEC = importlib.util.spec_from_file_location("mininet_mptcp_minimal_run", SCRIPT_PATH)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class MinimalRunnerSafetyTest(unittest.TestCase):
    def test_minimal_mode_is_fixed_to_one_subflow(self):
        self.assertEqual(RUNNER.MPTCP_SUBFLOW_MODE, "single")
        self.assertEqual(RUNNER.MPTCP_EXPERIMENT_MODE, "baseline_ecmp")
        self.assertEqual(RUNNER.MPTCP_MAX_TOTAL_SUBFLOWS, 1)
        self.assertFalse(RUNNER.MPTCP_ENABLE_PRODUCER_SUBFLOW_ENDPOINTS)
        self.assertFalse(RUNNER.MPTCP_SIGNAL_CONSUMER_ENDPOINTS)
        self.assertFalse(RUNNER.MPTCP_BIND_SENDER_TO_DATA_ENDPOINT)

    def test_run_path_is_isolated_by_variant_and_batch(self):
        path = Path(RUNNER.run_dir_for_mode("heterogeneous", "batch-1", 0.01))
        self.assertEqual(
            path.parts[-3:],
            ("minimal", "batch-1", "con_core_loss_0p01pct"),
        )

    def test_batch_id_rejects_path_traversal(self):
        for value in ("../oracle", "/tmp/run", "a/b", ""):
            with self.subTest(value=value):
                with self.assertRaises(argparse.ArgumentTypeError):
                    RUNNER.normalize_batch_id(value)

    def test_existing_loss_directory_is_never_modified(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            batch_dir = Path(temp_dir) / "minimal" / "batch-1"
            run_dir = batch_dir / "con_core_loss_0pct"
            run_dir.mkdir(parents=True)
            sentinel = run_dir / "existing.log"
            sentinel.write_text("must remain unchanged\n", encoding="utf-8")

            old_batch_dir = RUNNER.RUN_BATCH_DIR
            old_run_dir = RUNNER.RUN_DIR
            try:
                RUNNER.RUN_BATCH_DIR = str(batch_dir)
                RUNNER.RUN_DIR = str(run_dir)
                with self.assertRaisesRegex(RuntimeError, "refusing to overwrite"):
                    RUNNER.prepare_run_dir()
            finally:
                RUNNER.RUN_BATCH_DIR = old_batch_dir
                RUNNER.RUN_DIR = old_run_dir

            self.assertEqual(sentinel.read_text(encoding="utf-8"), "must remain unchanged\n")


if __name__ == "__main__":
    unittest.main()

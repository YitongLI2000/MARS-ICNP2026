import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).with_name("run_mptcp_minimal_batch.py")
SPEC = importlib.util.spec_from_file_location("run_mptcp_minimal_batch", SCRIPT_PATH)
BATCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BATCH)


class MinimalBatchSafetyTest(unittest.TestCase):
    def test_batch_has_the_four_required_loss_rates(self):
        self.assertEqual(BATCH.LOSS_RATES, ("0", "0.01", "0.1", "1"))
        self.assertEqual(BATCH.LOSS_LABELS, ("0", "0p01", "0p1", "1"))

    def test_protected_digest_excludes_only_the_new_batch(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "results"
            protected = root / "heterogeneous" / "mars_3032" / "con_core_loss_0pct"
            excluded = root / "heterogeneous" / "mars_3032" / "minimal" / "batch-1"
            protected.mkdir(parents=True)
            excluded.mkdir(parents=True)
            protected_log = protected / "con0.log"
            protected_log.write_text("original\n", encoding="utf-8")

            old_results_root = BATCH.RESULTS_ROOT
            try:
                BATCH.RESULTS_ROOT = root
                before = BATCH.protected_results_digest(excluded)
                (excluded / "new.log").write_text("new batch\n", encoding="utf-8")
                self.assertEqual(BATCH.protected_results_digest(excluded), before)
                protected_log.write_text("changed\n", encoding="utf-8")
                self.assertNotEqual(BATCH.protected_results_digest(excluded), before)
            finally:
                BATCH.RESULTS_ROOT = old_results_root

    def test_batch_audit_records_the_wrapper_source(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            batch_dir = Path(temp_dir)
            BATCH.write_batch_audit(batch_dir, "batch-1", "same", "same")
            audit = (batch_dir / "batch_audit.log").read_text(encoding="utf-8")

        self.assertIn("sha256.run_mptcp_minimal_batch.py=", audit)


if __name__ == "__main__":
    unittest.main()

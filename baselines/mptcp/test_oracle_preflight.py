import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).with_name("mininet-mptcp-run.py")
SPEC = importlib.util.spec_from_file_location("mininet_mptcp_run_preflight", SCRIPT_PATH)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class OraclePreflightTest(unittest.TestCase):
    def test_cubic_is_accepted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            setting = Path(temp_dir) / "tcp_congestion_control"
            setting.write_text("cubic\n", encoding="ascii")

            self.assertEqual(
                RUNNER.require_cubic_congestion_control(str(setting)),
                "cubic",
            )

    def test_non_cubic_setting_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            setting = Path(temp_dir) / "tcp_congestion_control"
            setting.write_text("reno\n", encoding="ascii")

            with self.assertRaisesRegex(
                RuntimeError,
                r"net\.ipv4\.tcp_congestion_control must be cubic \(found reno\)",
            ):
                RUNNER.require_cubic_congestion_control(str(setting))

    def test_unreadable_setting_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing = Path(temp_dir) / "missing"

            with self.assertRaisesRegex(
                RuntimeError, "cannot read TCP congestion control setting"
            ):
                RUNNER.require_cubic_congestion_control(str(missing))


if __name__ == "__main__":
    unittest.main()

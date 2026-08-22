import hashlib
import os
import subprocess
import tempfile
import unittest
from unittest import mock

import ndnd_run_failure_rto1 as failure_runner


class FailureArtifactInspectionTest(unittest.TestCase):
    def test_rebuilt_artifacts_are_recorded_without_fixed_hashes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runner_path = os.path.join(temp_dir, "ndnd_run.py")
            binary_path = os.path.join(temp_dir, "consumer")
            with open(runner_path, "wb") as output_file:
                output_file.write(b"runner source")
            with open(binary_path, "wb") as output_file:
                output_file.write(b"first build")
            os.chmod(binary_path, 0o755)

            artifacts = {
                "runner": (runner_path, False),
                "consumer": (binary_path, True),
            }
            with mock.patch.object(failure_runner, "ARTIFACT_FILES", artifacts):
                first_hashes = failure_runner.inspect_artifacts()
                with open(binary_path, "wb") as output_file:
                    output_file.write(b"rebuilt binary")
                second_hashes = failure_runner.inspect_artifacts()

            self.assertEqual(
                first_hashes["runner"],
                hashlib.sha256(b"runner source").hexdigest(),
            )
            self.assertNotEqual(
                first_hashes["consumer"], second_hashes["consumer"]
            )
            self.assertEqual(
                second_hashes["consumer"],
                hashlib.sha256(b"rebuilt binary").hexdigest(),
            )

    def test_non_executable_binary_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            binary_path = os.path.join(temp_dir, "consumer")
            with open(binary_path, "wb") as output_file:
                output_file.write(b"binary")
            os.chmod(binary_path, 0o644)

            artifacts = {"consumer": (binary_path, True)}
            with mock.patch.object(failure_runner, "ARTIFACT_FILES", artifacts):
                with self.assertRaisesRegex(RuntimeError, "not executable"):
                    failure_runner.inspect_artifacts()

    def test_consumer_configuration_is_checked_semantically(self):
        matching_result = subprocess.CompletedProcess(
            args=["consumer", "--print-rto-config"],
            returncode=0,
            stdout=failure_runner.EXPECTED_CONSUMER_RTO_CONFIG + "\n",
            stderr="",
        )
        with mock.patch.object(
            failure_runner.subprocess, "run", return_value=matching_result
        ):
            failure_runner.verify_consumer_binary_configuration()

        mismatched_result = subprocess.CompletedProcess(
            args=["consumer", "--print-rto-config"],
            returncode=0,
            stdout="rtoOuterMultiplier=2\n",
            stderr="",
        )
        with mock.patch.object(
            failure_runner.subprocess, "run", return_value=mismatched_result
        ):
            with self.assertRaisesRegex(RuntimeError, "configuration mismatch"):
                failure_runner.verify_consumer_binary_configuration()


if __name__ == "__main__":
    unittest.main()

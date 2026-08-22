import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT_PATH = Path(__file__).with_name("mininet-mpquic-minimal-four-loss.py")
SPEC = importlib.util.spec_from_file_location("mininet_mpquic_minimal", SCRIPT_PATH)
MINIMAL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MINIMAL)


class MinimalBatchDirectoryTest(unittest.TestCase):
    @staticmethod
    def patched_paths(example_dir):
        minimal_root = example_dir / "results" / "minimal"
        batch_root = minimal_root / "batch-1"
        return mock.patch.multiple(
            MINIMAL,
            MINIMAL_RESULTS_ROOT=str(minimal_root),
            RESULTS_ROOT=str(batch_root),
            BIN_DIR=str(batch_root / "bin"),
            PROVENANCE_DIR=str(batch_root / "provenance"),
        )

    def test_missing_results_parent_is_created(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            example_dir = Path(temp_dir) / "example"
            example_dir.mkdir()

            with self.patched_paths(example_dir):
                MINIMAL.prepare_batch_directories()

            self.assertTrue((example_dir / "results").is_dir())
            self.assertTrue(
                (example_dir / "results" / "minimal" / "batch-1" / "bin").is_dir()
            )
            self.assertTrue(
                (
                    example_dir
                    / "results"
                    / "minimal"
                    / "batch-1"
                    / "provenance"
                ).is_dir()
            )

    def test_existing_results_parent_is_accepted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            example_dir = Path(temp_dir) / "example"
            (example_dir / "results").mkdir(parents=True)

            with self.patched_paths(example_dir):
                MINIMAL.prepare_batch_directories()

            self.assertTrue(
                (example_dir / "results" / "minimal" / "batch-1").is_dir()
            )

    def test_results_parent_rejects_symlink_and_non_directory(self):
        for invalid_kind in ("symlink", "file"):
            with self.subTest(invalid_kind=invalid_kind):
                with tempfile.TemporaryDirectory() as temp_dir:
                    example_dir = Path(temp_dir) / "example"
                    example_dir.mkdir()
                    results_parent = example_dir / "results"
                    if invalid_kind == "symlink":
                        target = example_dir / "elsewhere"
                        target.mkdir()
                        results_parent.symlink_to(target, target_is_directory=True)
                    else:
                        results_parent.write_text("not a directory\n", encoding="utf-8")

                    with self.patched_paths(example_dir):
                        with self.assertRaisesRegex(
                            RuntimeError, "Results parent is not a real directory"
                        ):
                            MINIMAL.prepare_batch_directories()

    def test_existing_batch_is_never_reused(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            example_dir = Path(temp_dir) / "example"
            batch_root = example_dir / "results" / "minimal" / "batch-1"
            batch_root.mkdir(parents=True)
            marker = batch_root / "existing.log"
            marker.write_text("preserve me\n", encoding="utf-8")

            with self.patched_paths(example_dir):
                with self.assertRaises(FileExistsError):
                    MINIMAL.prepare_batch_directories()

            self.assertEqual(marker.read_text(encoding="utf-8"), "preserve me\n")
            self.assertFalse((batch_root / "bin").exists())
            self.assertFalse((batch_root / "provenance").exists())


if __name__ == "__main__":
    unittest.main()

import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).with_name("mininet-mpquic-run.py")
SPEC = importlib.util.spec_from_file_location("mininet_mpquic_run", SCRIPT_PATH)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class ExactFctSummaryTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_consumer_log(self, consumer_index, samples):
        lines = []
        for sample in samples:
            session = sample[0]
            fct95, fct99, fct100 = sample[1:4]
            status = sample[4] if len(sample) > 4 else "complete"
            expected = sample[5] if len(sample) > 5 else RUNNER.APP_EXPECTED_MESSAGES
            received = sample[6] if len(sample) > 6 else expected
            expected_bytes = (
                sample[7] if len(sample) > 7 else RUNNER.APP_TRANSFER_TOTAL_BYTES
            )
            received_bytes = sample[8] if len(sample) > 8 else expected_bytes
            lines.append(
                f"[12:00:00.000] [fct_summary] session={session} "
                f"remote=192.0.2.{consumer_index + 1}:{5000 + session} "
                f"expected_msgs={expected} received_msgs={received} "
                f"expected_bytes={expected_bytes} received_bytes={received_bytes} "
                f"fct95_s={fct95} fct99_s={fct99} fct100_s={fct100} "
                "method=nearest_rank metric=message_completion "
                f"clock=monotonic status={status}\n"
            )
        (self.run_dir / f"con{consumer_index}.log").write_text(
            "".join(lines), encoding="utf-8"
        )

    def write_complete_sample_set(self):
        for consumer_index in range(RUNNER.NUM_CONSUMERS):
            samples = []
            for session in range(1, RUNNER.PRODUCERS_PER_CONSUMER + 1):
                fct95 = consumer_index + session
                samples.append((session, fct95, fct95 + 10, fct95 + 20))
            self.write_consumer_log(consumer_index, samples)

    def test_exact_samples_produce_requested_two_level_aggregation(self):
        self.write_complete_sample_set()

        samples = RUNNER.collect_receiver_fct_samples(str(self.run_dir))
        consumer_stats = RUNNER.consumer_fct_statistics(samples)
        cross_stats = RUNNER.cross_consumer_fct_statistics(consumer_stats)

        self.assertTrue(RUNNER.has_complete_receiver_fct_set(samples))
        self.assertEqual(len(samples), RUNNER.NUM_PRODUCERS)
        self.assertEqual(
            consumer_stats[0],
            {
                "consumer": "con0",
                "flows": 5,
                "mean_fct95": 3.0,
                "mean_fct99": 13.0,
                "mean_fct100": 23.0,
                "max_flow_fct100": 25.0,
            },
        )
        self.assertEqual(
            cross_stats,
            {
                "median_fct95": 5.0,
                "max_fct95": 7.0,
                "median_fct99": 15.0,
                "max_fct99": 17.0,
                "median_fct100": 25.0,
                "max_fct100": 27.0,
            },
        )

    def test_run_summary_contains_per_consumer_and_cross_consumer_rows(self):
        self.write_complete_sample_set()

        summary_path = Path(RUNNER.write_run_fct_summary(str(self.run_dir)))
        summary = summary_path.read_text(encoding="utf-8")

        self.assertIn(
            "[consumer_summary] consumer=con0 mean_fct95_s=3.000000 "
            "mean_fct99_s=13.000000 mean_fct100_s=23.000000 "
            "max_flow_fct100_s=25.000000 flows=5 "
            "aggregation=arithmetic_mean status=complete",
            summary,
        )
        self.assertIn(
            "[cross_consumer] median_fct95_s=5.000000 max_fct95_s=7.000000 "
            "median_fct99_s=15.000000 max_fct99_s=17.000000 "
            "median_fct100_s=25.000000 max_fct100_s=27.000000 "
            "consumers=5 aggregation=consumer_flow_means status=complete",
            summary,
        )

    def test_incomplete_or_mismatched_samples_are_excluded(self):
        self.write_complete_sample_set()
        self.write_consumer_log(
            4,
            [
                (1, 5, 15, 25),
                (2, 6, 16, 26),
                (3, 7, 17, 27),
                (4, 8, 18, 28),
                (5, 9, "NA", "NA", "incomplete", 3038, 3000),
            ],
        )

        samples = RUNNER.collect_receiver_fct_samples(str(self.run_dir))

        self.assertEqual(len(samples), RUNNER.NUM_PRODUCERS - 1)
        self.assertFalse(RUNNER.has_complete_receiver_fct_set(samples))
        summary = Path(
            RUNNER.write_run_fct_summary(str(self.run_dir))
        ).read_text(encoding="utf-8")
        self.assertIn("status=incomplete", summary)
        self.assertIn("flows=24 expected_flows=25", summary)
        self.assertNotIn("all_flows_p95_fct_s", summary)

    def test_legacy_close_fct_is_not_used_as_exact_fct100(self):
        self.write_complete_sample_set()
        con0_log = self.run_dir / "con0.log"
        with con0_log.open("a", encoding="utf-8") as log_file:
            log_file.write(
                "[12:00:01.000] Session #99 closed | Remote: 192.0.2.1:9999 | "
                "Total: 3038 msgs | 18240512 bytes | FCT: 999.0000 s\n"
            )

        samples = RUNNER.collect_receiver_fct_samples(str(self.run_dir))

        self.assertEqual(len(samples), RUNNER.NUM_PRODUCERS)
        self.assertNotIn(999.0, [sample["fct100"] for sample in samples])

    def test_complete_status_with_wrong_byte_count_is_excluded(self):
        self.write_complete_sample_set()
        self.write_consumer_log(
            4,
            [
                (1, 5, 15, 25),
                (2, 6, 16, 26),
                (3, 7, 17, 27),
                (4, 8, 18, 28),
                (
                    5,
                    9,
                    19,
                    29,
                    "complete",
                    3038,
                    3038,
                    RUNNER.APP_TRANSFER_TOTAL_BYTES,
                    RUNNER.APP_TRANSFER_TOTAL_BYTES - 1,
                ),
            ],
        )

        samples = RUNNER.collect_receiver_fct_samples(str(self.run_dir))

        self.assertEqual(len(samples), RUNNER.NUM_PRODUCERS - 1)
        self.assertFalse(RUNNER.has_complete_receiver_fct_set(samples))

    def test_append_consumer_summary_uses_flow_means(self):
        self.write_complete_sample_set()

        RUNNER.append_fct_summaries(str(self.run_dir))
        summary = (self.run_dir / "con0.log").read_text(encoding="utf-8")

        self.assertIn(
            "[summary] con0 mean_fct95_s=3.000000 mean_fct99_s=13.000000 "
            "mean_fct100_s=23.000000 max_flow_fct100_s=25.000000 "
            "flows=5 aggregation=arithmetic_mean status=complete",
            summary,
        )


if __name__ == "__main__":
    unittest.main()

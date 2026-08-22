import argparse
import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT_PATH = Path(__file__).with_name("mininet-mptcp-run.py")
SPEC = importlib.util.spec_from_file_location("mininet_mptcp_run", SCRIPT_PATH)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class ExactFctSummaryTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.temp_dir.name)
        self.target_messages, self.target_bytes = RUNNER.expected_fct_targets()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_loss_percentage_cli_normalization(self):
        self.assertEqual(RUNNER.normalize_loss_percentage("0"), 0)
        self.assertEqual(RUNNER.normalize_loss_percentage("0.01"), 0.01)
        self.assertEqual(RUNNER.normalize_loss_percentage("1"), 1)
        with self.assertRaises(argparse.ArgumentTypeError):
            RUNNER.normalize_loss_percentage("-0.1")
        with self.assertRaises(argparse.ArgumentTypeError):
            RUNNER.normalize_loss_percentage("not-a-number")

    def write_consumer_log(self, consumer_index, samples, include_legacy=True):
        lines = []
        for sample in samples:
            session, fct95, fct99, fct100 = sample[:4]
            status = sample[4] if len(sample) > 4 else "complete"
            received_bytes = (
                sample[5] if len(sample) > 5 else RUNNER.APP_TRANSFER_TOTAL_BYTES
            )
            fct99_target_bytes = (
                sample[6] if len(sample) > 6 else self.target_bytes[99]
            )
            remote = f"192.0.2.{consumer_index + 1}:{5000 + session}"
            lines.append(
                f"[12:00:00.000] [fct_summary] session={session} remote={remote} "
                f"expected_msgs={RUNNER.APP_EXPECTED_MESSAGES} "
                f"message_size_bytes={RUNNER.APP_CHUNK_SIZE_BYTES} "
                f"expected_bytes={RUNNER.APP_TRANSFER_TOTAL_BYTES} "
                f"received_bytes={received_bytes} "
                f"fct95_target_msgs={self.target_messages[95]} "
                f"fct95_target_bytes={self.target_bytes[95]} "
                f"fct99_target_msgs={self.target_messages[99]} "
                f"fct99_target_bytes={fct99_target_bytes} "
                f"fct100_target_msgs={self.target_messages[100]} "
                f"fct100_target_bytes={self.target_bytes[100]} "
                f"fct95_s={fct95} fct99_s={fct99} fct100_s={fct100} "
                f"method=nearest_rank metric={RUNNER.MARS_FCT_METRIC} "
                f"clock=monotonic status={status} "
                f"standard={RUNNER.MARS_FCT_STANDARD} "
                f"start_boundary={RUNNER.MARS_START_BOUNDARY} "
                f"logical_unit={RUNNER.MARS_LOGICAL_UNIT}\n"
            )
            if include_legacy:
                legacy_fct = fct100 if fct100 != "NA" else 999.0
                lines.append(
                    f"[12:00:01.000] Session #{session} closed | Remote: {remote} | "
                    "Transport: mptcp | Total: 2000 reads | "
                    f"{received_bytes} bytes | FCT: {float(legacy_fct):.4f} s\n"
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

    def test_targets_match_standardized_logical_chunk_geometry(self):
        self.assertEqual(RUNNER.APP_EXPECTED_MESSAGES, 3032)
        self.assertEqual(RUNNER.APP_CHUNK_SIZE_BYTES, 6016)
        self.assertEqual(self.target_messages, {95: 2881, 99: 3002, 100: 3032})
        self.assertEqual(
            self.target_bytes,
            {95: 17332096, 99: 18060032, 100: 18240512},
        )

    def test_exact_samples_produce_both_cross_flow_aggregations(self):
        self.write_complete_sample_set()

        samples = RUNNER.collect_receiver_fct_samples(str(self.run_dir))
        consumer_stats = RUNNER.consumer_fct_statistics(samples)
        cross_consumer_stats = RUNNER.cross_consumer_fct_statistics(consumer_stats)
        cross_flow_stats = RUNNER.cross_flow_fct_statistics(samples)

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
            cross_consumer_stats,
            {
                "median_fct95": 5.0,
                "max_fct95": 7.0,
                "median_fct99": 15.0,
                "max_fct99": 17.0,
                "median_fct100": 25.0,
                "max_fct100": 27.0,
            },
        )
        self.assertEqual(
            cross_flow_stats,
            {
                "median_fct95": 5.0,
                "max_fct95": 9.0,
                "median_fct99": 15.0,
                "max_fct99": 19.0,
                "median_fct100": 25.0,
                "max_fct100": 29.0,
            },
        )

    def test_run_summary_contains_all_three_aggregation_levels(self):
        self.write_complete_sample_set()

        summary_path = Path(RUNNER.write_run_fct_summary(str(self.run_dir)))
        summary = summary_path.read_text(encoding="utf-8")

        self.assertIn(
            "[run_summary] consumer=con0 session=1 "
            "remote=192.0.2.1:5001 fct95_s=1.000000000 "
            "fct99_s=11.000000000 fct100_s=21.000000000 status=complete",
            summary,
        )
        self.assertIn(
            "[consumer_summary] consumer=con0 mean_fct95_s=3.000000000 "
            "mean_fct99_s=13.000000000 mean_fct100_s=23.000000000 "
            "max_flow_fct100_s=25.000000000 flows=5 "
            "aggregation=arithmetic_mean status=complete",
            summary,
        )
        self.assertIn(
            "[cross_consumer] median_fct95_s=5.000000000 "
            "max_fct95_s=7.000000000 median_fct99_s=15.000000000 "
            "max_fct99_s=17.000000000 median_fct100_s=25.000000000 "
            "max_fct100_s=27.000000000 "
            "consumers=5 aggregation=consumer_flow_means status=complete",
            summary,
        )
        self.assertIn(
            "[cross_flow] median_flow_fct95_s=5.000000000 "
            "max_flow_fct95_s=9.000000000 "
            "median_flow_fct99_s=15.000000000 "
            "max_flow_fct99_s=19.000000000 "
            "median_flow_fct100_s=25.000000000 "
            "max_flow_fct100_s=29.000000000 flows=25 "
            "aggregation=all_flows",
            summary,
        )
        self.assertNotIn("all_flows_p95_fct_s", summary)

    def test_incomplete_sample_is_excluded_from_aggregates(self):
        self.write_complete_sample_set()
        self.write_consumer_log(
            4,
            [
                (1, 5, 15, 25),
                (2, 6, 16, 26),
                (3, 7, 17, 27),
                (4, 8, 18, 28),
                (5, 9, "NA", "NA", "incomplete", 18000000),
            ],
        )

        samples = RUNNER.collect_receiver_fct_samples(str(self.run_dir))
        summary = Path(
            RUNNER.write_run_fct_summary(str(self.run_dir))
        ).read_text(encoding="utf-8")

        self.assertEqual(len(samples), RUNNER.NUM_PRODUCERS - 1)
        self.assertFalse(RUNNER.has_complete_receiver_fct_set(samples))
        self.assertNotIn("all_flows_p95_fct_s", summary)
        self.assertIn("flows=24 expected_flows=25", summary)
        self.assertIn("aggregation=consumer_flow_means status=incomplete", summary)
        self.assertIn("aggregation=all_flows", summary)

    def test_duplicate_session_ids_make_the_run_incomplete(self):
        self.write_complete_sample_set()
        self.write_consumer_log(
            4,
            [
                (1, 5, 15, 25),
                (2, 6, 16, 26),
                (3, 7, 17, 27),
                (4, 8, 18, 28),
                (4, 9, 19, 29),
            ],
        )

        samples = RUNNER.collect_receiver_fct_samples(str(self.run_dir))
        self.assertEqual(len(samples), RUNNER.NUM_PRODUCERS)
        self.assertFalse(RUNNER.has_complete_receiver_fct_set(samples))

    def test_byte_or_target_mismatch_is_excluded(self):
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
                    RUNNER.APP_TRANSFER_TOTAL_BYTES,
                    self.target_bytes[99] - 1,
                ),
            ],
        )

        samples = RUNNER.collect_receiver_fct_samples(str(self.run_dir))
        self.assertEqual(len(samples), RUNNER.NUM_PRODUCERS - 1)
        self.assertFalse(RUNNER.has_complete_receiver_fct_set(samples))

    def test_legacy_close_fct_does_not_substitute_for_exact_milestones(self):
        self.write_complete_sample_set()
        (self.run_dir / "con0.log").write_text(
            "[12:00:01.000] Session #1 closed | Remote: 192.0.2.1:5001 | "
            "Transport: mptcp | Total: 3032 reads | 18240512 bytes | "
            "FCT: 999.0000 s\n",
            encoding="utf-8",
        )

        samples = RUNNER.collect_receiver_fct_samples(str(self.run_dir))

        self.assertEqual(len(samples), RUNNER.NUM_PRODUCERS - 5)
        self.assertNotIn(999.0, [sample["fct100"] for sample in samples])

    def test_append_preserves_per_session_summary_and_adds_exact_flow_means(self):
        self.write_complete_sample_set()

        RUNNER.append_fct_summaries(str(self.run_dir))
        summary = (self.run_dir / "con0.log").read_text(encoding="utf-8")

        self.assertIn("[summary] con0 per_session_fct_begin flows=5", summary)
        self.assertNotIn("overall_p95_fct_s", summary)
        self.assertIn("[summary] con0 exact_per_flow_fct_begin flows=5", summary)
        self.assertIn(
            "[summary] con0 mean_fct95_s=3.000000000 "
            "mean_fct99_s=13.000000000 mean_fct100_s=23.000000000 "
            "max_flow_fct100_s=25.000000000 "
            "flows=5 aggregation=arithmetic_mean status=complete",
            summary,
        )


if __name__ == "__main__":
    unittest.main()

import unittest

import fct_protocol
import receiver


class FlowFCTMilestonesTest(unittest.TestCase):
    def setUp(self):
        self.milestones = receiver.FlowFCTMilestones(
            expected_messages=fct_protocol.MARS_EXPECTED_MESSAGES,
            expected_bytes=fct_protocol.MARS_EXPECTED_BYTES,
            message_size_bytes=fct_protocol.MARS_MESSAGE_SIZE_BYTES,
            metric=fct_protocol.MARS_FCT_METRIC,
            standard=fct_protocol.MARS_FCT_STANDARD,
            start_boundary=fct_protocol.MARS_START_BOUNDARY,
            logical_unit=fct_protocol.MARS_LOGICAL_UNIT,
        )

    def test_targets_match_message_nearest_rank(self):
        self.assertEqual(self.milestones.target_messages[95], 2881)
        self.assertEqual(self.milestones.target_messages[99], 3002)
        self.assertEqual(self.milestones.target_messages[100], 3032)
        self.assertEqual(self.milestones.target_bytes[95], 17332096)
        self.assertEqual(self.milestones.target_bytes[99], 18060032)
        self.assertEqual(self.milestones.target_bytes[100], 18240512)

    def test_observe_keeps_first_crossing_time(self):
        self.milestones.observe(17332095, 4.0)
        self.milestones.observe(17332096, 4.1)
        self.milestones.observe(18060032, 5.2)
        self.milestones.observe(18240512, 6.0)
        self.milestones.observe(18240512, 7.0)

        self.assertEqual(self.milestones.elapsed, {95: 4.1, 99: 5.2, 100: 6.0})
        self.assertEqual(self.milestones.status(18240512), "complete")

    def test_summary_is_machine_readable_and_preserves_incomplete_status(self):
        self.milestones.observe(17332096, 4.1)

        line = self.milestones.summary_line(7, "192.0.2.1:5000", 17332096)

        self.assertIn("session=7 remote=192.0.2.1:5000", line)
        self.assertIn("fct95_s=4.100000000 fct99_s=NA fct100_s=NA", line)
        self.assertIn(f"metric={fct_protocol.MARS_FCT_METRIC}", line)
        self.assertIn("clock=monotonic status=incomplete", line)
        self.assertIn(f"standard={fct_protocol.MARS_FCT_STANDARD}", line)
        self.assertIn(f"start_boundary={fct_protocol.MARS_START_BOUNDARY}", line)

    def test_recv_size_stops_exactly_at_pending_milestones(self):
        target95 = self.milestones.target_bytes[95]
        target99 = self.milestones.target_bytes[99]
        target100 = self.milestones.target_bytes[100]

        self.assertEqual(
            receiver.next_payload_recv_size(target95 - 17, self.milestones),
            17,
        )
        self.assertEqual(
            receiver.next_payload_recv_size(target99 - 9, self.milestones),
            9,
        )
        self.assertEqual(
            receiver.next_payload_recv_size(target100 - 1, self.milestones),
            1,
        )
        self.assertEqual(
            receiver.next_payload_recv_size(target100, self.milestones),
            receiver.DEFAULT_RECV_SIZE_BYTES,
        )

    def test_invalid_message_shape_is_rejected(self):
        with self.assertRaises(ValueError):
            receiver.FlowFCTMilestones(
                expected_messages=3031,
                expected_bytes=18240512,
                message_size_bytes=6016,
            )

    def test_partial_or_negative_configuration_is_rejected(self):
        with self.assertRaises(ValueError):
            receiver.FlowFCTMilestones(
                expected_messages=3032,
                expected_bytes=0,
                message_size_bytes=6016,
            )
        with self.assertRaises(ValueError):
            receiver.FlowFCTMilestones(
                expected_messages=-1,
                expected_bytes=-1,
                message_size_bytes=-1,
            )


if __name__ == "__main__":
    unittest.main()

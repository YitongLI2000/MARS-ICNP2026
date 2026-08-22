import tempfile
import unittest

from flow_completion import parse_flow_summary_line, read_flow_summaries


class FlowCompletionTest(unittest.TestCase):
    def test_complete_summary(self):
        summary = parse_flow_summary_line(
            'time=x level=INFO msg="Flow Summary" flow=/pro0app complete=true '
            'receivedPackets=3032 expectedPackets=3032 missingPackets=0 fct100=20.1\n'
        )

        self.assertEqual(summary.flow, '/pro0app')
        self.assertTrue(summary.is_complete())

    def test_incomplete_summary(self):
        summary = parse_flow_summary_line(
            'time=x level=INFO msg="Flow Summary" flow=/pro0app complete=false '
            'receivedPackets=2880 expectedPackets=3032 missingPackets=152 fct100=0\n'
        )

        self.assertFalse(summary.is_complete())

    def test_wrong_expected_packet_count_is_rejected(self):
        summary = parse_flow_summary_line(
            'time=x level=INFO msg="Flow Summary" flow=/pro0app complete=true '
            'receivedPackets=10 expectedPackets=10 missingPackets=0 fct100=1\n'
        )

        self.assertFalse(summary.is_complete())

    def test_old_summary_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'missing fields'):
            parse_flow_summary_line(
                'time=x level=INFO msg="Flow Summary" flow=/pro0app totalBytes=1\n'
            )

    def test_reader_rejects_duplicate_flow(self):
        line = (
            'time=x level=INFO msg="Flow Summary" flow=/pro0app complete=true '
            'receivedPackets=3032 expectedPackets=3032 missingPackets=0\n'
        )
        with tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8') as log_file:
            log_file.write(line)
            log_file.write(line)
            log_file.flush()
            summaries, errors = read_flow_summaries(log_file.name)

        self.assertEqual(set(summaries), {'/pro0app'})
        self.assertEqual(len(errors), 1)
        self.assertIn('duplicate flow', errors[0])

    def test_reader_ignores_partial_final_line(self):
        with tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8') as log_file:
            log_file.write(
                'time=x level=INFO msg="Flow Summary" flow=/pro0app complete=true '
                'receivedPackets=3032'
            )
            log_file.flush()
            summaries, errors = read_flow_summaries(log_file.name)

        self.assertEqual(summaries, {})
        self.assertEqual(errors, [])


if __name__ == '__main__':
    unittest.main()

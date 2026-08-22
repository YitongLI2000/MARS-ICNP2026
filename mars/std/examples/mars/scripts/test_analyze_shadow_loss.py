import os
import tempfile
import unittest

import analyze_shadow_loss


def shadow_line(
    age=250,
    later=8,
    suspects=10,
    resolved=2,
    reached=8,
    active=0,
    first_rtos=12,
    lead_total=800.0,
    inflight_total=1000.0,
    behavioral=False,
):
    avg_lead = lead_total / reached if reached else 0.0
    avg_inflight = inflight_total / suspects if suspects else 0.0
    return (
        'time=2026-08-13T00:00:00Z level=INFO '
        'msg="Shadow Loss Summary" tag=NDN-Consumer '
        f'flow=/pro0app behavioralRetransmission={str(behavioral).lower()} '
        f'ageFloorMs={age} laterAckThreshold={later} '
        f'shadowSuspects={suspects} resolvedBeforeRTO={resolved} '
        f'reachedRTO={reached} activeAtEnd={active} '
        f'firstAttemptRTOs={first_rtos} '
        f'leadToRTOTotalMs={lead_total} avgLeadToRTOMs={avg_lead} '
        f'suspectInflightTotalMs={inflight_total} '
        f'suspectInflightAvgMs={avg_inflight} source=main.main\n'
    )


class ShadowLossAnalysisTest(unittest.TestCase):
    def test_parse_summary(self):
        summary = analyze_shadow_loss.parse_shadow_loss_summary_line(shadow_line())
        self.assertEqual(summary.flow, '/pro0app')
        self.assertEqual(summary.age_floor_ms, 250)
        self.assertEqual(summary.later_ack_threshold, 8)
        self.assertEqual(summary.shadow_suspects, 10)
        self.assertEqual(summary.resolved_before_rto, 2)
        self.assertEqual(summary.reached_rto, 8)
        self.assertEqual(summary.first_attempt_rtos, 12)
        self.assertFalse(summary.behavioral_retransmission)

    def test_rejects_inconsistent_classification(self):
        with self.assertRaisesRegex(ValueError, 'classified candidates'):
            analyze_shadow_loss.parse_shadow_loss_summary_line(
                shadow_line(suspects=10, resolved=1, reached=8)
            )

    def test_behavioral_repair_allows_recovery_rto_to_exceed_first_attempt_rto(self):
        summary = analyze_shadow_loss.parse_shadow_loss_summary_line(
            shadow_line(
                suspects=10,
                resolved=2,
                reached=8,
                first_rtos=3,
                behavioral=True,
            )
        )
        self.assertTrue(summary.behavioral_retransmission)
        self.assertEqual(summary.reached_rto, 8)
        self.assertEqual(summary.first_attempt_rtos, 3)

    def test_read_summaries(self):
        with tempfile.TemporaryDirectory() as run_dir:
            log_path = os.path.join(run_dir, 'con0_app.log')
            with open(log_path, 'w', encoding='ascii') as log_file:
                log_file.write('unrelated line\n')
                log_file.write(shadow_line(behavioral=True))

            summaries, errors = analyze_shadow_loss.read_shadow_loss_summaries(
                log_path
            )
            self.assertEqual(errors, [])
            self.assertEqual(len(summaries), 1)
            self.assertTrue(summaries[0].behavioral_retransmission)


if __name__ == '__main__':
    unittest.main()

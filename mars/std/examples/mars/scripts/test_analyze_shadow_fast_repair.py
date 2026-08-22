import contextlib
import io
import json
import os
import tempfile
import unittest

import analyze_shadow_fast_repair


def fast_summary_line(backoff_field='', rto_field=''):
    return (
        'time=x level=INFO msg="Shadow Fast Repair Summary" tag=NDN-Consumer '
        'flow=/pro0app behavioralRetransmission=true ageFloorMs=350 '
        'laterAckThreshold=8 maxPerGeneration=1 usesUnifiedPacing=true '
        f'rateBackoff=false cwndBackoff=false {backoff_field}{rto_field}'
        'triggers=10 queued=8 '
        'alreadyQueued=1 alreadyTriggered=0 stale=1 sent=7 resolved=8 '
        'resolvedAfterSend=7 source=main.main\n'
    )


def shadow_summary_line():
    return (
        'time=x level=INFO msg="Shadow Loss Summary" tag=NDN-Consumer '
        'flow=/pro0app behavioralRetransmission=true ageFloorMs=350 '
        'laterAckThreshold=8 shadowSuspects=10 resolvedBeforeRTO=8 '
        'reachedRTO=2 activeAtEnd=0 firstAttemptRTOs=2 '
        'leadToRTOTotalMs=200 avgLeadToRTOMs=100 '
        'suspectInflightTotalMs=500 suspectInflightAvgMs=50 source=main.main\n'
    )


def current_manifest(legacy_comparison_fields=False):
    manifest = {
        'consumer': {
            'variant': 'shadow-fast-350-8-no-backoff-rto1',
            'sha256': 'rto1-sha',
        },
        'forwarder': {
            'default': {
                'variant': 'persistent-2ms-netem-bwcap',
                'sha256': 'forwarder-sha',
            },
        },
        'shadowLossDiagnostics': {
            'enabled': True,
            'behavioralRetransmission': True,
            'expectedFlowCount': 1,
        },
        'fastRepair': {
            'enabled': True,
            'ageFloorMs': 350,
            'laterAckThreshold': 8,
            'maxPerGeneration': 1,
            'usesExistingRetransmissionQueue': True,
            'usesUnifiedPacing': True,
            'rateBackoff': False,
            'cwndBackoff': False,
            'rtoBackoffPolicy': 'fast-repair-neutral',
            'fastRepairAdvancesRtoBackoff': False,
            'regularRecoveryAdvancesRtoBackoff': True,
        },
        'rtoEstimator': {
            'family': 'sliding-window-mean-population-stddev',
            'formulaAfterTwoSamples': (
                'clamp(M * (meanRTT + 4 * stdDev), MIN_RTO, MAX_RTO)'
            ),
            'formulaBeforeTwoSamples': (
                'clamp(M * max(MIN_RTO, 2 * latestRTT), MIN_RTO, MAX_RTO)'
            ),
            'outerMultiplier': 1,
            'stdDevMultiplier': 4,
            'rttWindowMs': 600,
            'minRtoMs': 240,
            'maxRtoMs': 3500,
            'retransmissionJitterRatio': 0.10,
            'ordinaryRecoveryBackoffFactor': 2,
            'buildTimeFixed': True,
        },
    }
    if legacy_comparison_fields:
        manifest['fastRepair']['rtoParametersChanged'] = True
        manifest['rtoEstimator']['frozenOuterMultiplier'] = 4
        manifest['rtoEstimator']['parametersChangedFromFrozen'] = True
    return manifest


def write_complete_run(run_dir, manifest, multiplier=1):
    with open(
        os.path.join(run_dir, 'run_manifest.json'), 'w', encoding='ascii'
    ) as manifest_file:
        json.dump(manifest, manifest_file)
    with open(
        os.path.join(run_dir, 'con0_app.log'), 'w', encoding='ascii'
    ) as log_file:
        log_file.write(
            'time=x level=INFO msg="Flow Summary" flow=/pro0app '
            'complete=true receivedPackets=3032 expectedPackets=3032 '
            'missingPackets=0\n'
        )
        log_file.write(shadow_summary_line())
        backoff_field = 'fastRepairAdvancesRtoBackoff=false '
        rto_field = f'rtoOuterMultiplier={multiplier} '
        log_file.write(fast_summary_line(backoff_field, rto_field))


class FastRepairAnalysisTest(unittest.TestCase):
    def test_parse_summary(self):
        summary = analyze_shadow_fast_repair.parse_fast_repair_summary_line(
            fast_summary_line()
        )
        self.assertEqual(summary.triggers, 10)
        self.assertEqual(summary.queued, 8)
        self.assertEqual(summary.sent, 7)

    def test_parse_decoupled_summary(self):
        summary = analyze_shadow_fast_repair.parse_fast_repair_summary_line(
            fast_summary_line('fastRepairAdvancesRtoBackoff=false ')
        )
        self.assertFalse(summary.fast_repair_advances_rto_backoff)

    def test_parse_rto_multiplier(self):
        summary = analyze_shadow_fast_repair.parse_fast_repair_summary_line(
            fast_summary_line(
                'fastRepairAdvancesRtoBackoff=false ',
                'rtoOuterMultiplier=1 ',
            )
        )
        self.assertEqual(summary.rto_outer_multiplier, 1)

    def test_rejects_obsolete_rto_multiplier(self):
        with self.assertRaisesRegex(ValueError, 'rtoOuterMultiplier must be 1'):
            analyze_shadow_fast_repair.parse_fast_repair_summary_line(
                fast_summary_line(
                    'fastRepairAdvancesRtoBackoff=false ',
                    'rtoOuterMultiplier=2 ',
                )
            )

    def test_rejects_inconsistent_outcomes(self):
        with self.assertRaisesRegex(ValueError, 'queue outcomes'):
            analyze_shadow_fast_repair.parse_fast_repair_summary_line(
                fast_summary_line().replace('stale=1', 'stale=0')
            )

    def test_analyze_complete_run(self):
        with tempfile.TemporaryDirectory() as run_dir:
            write_complete_run(run_dir, current_manifest())

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = analyze_shadow_fast_repair.analyze_run(run_dir)
            self.assertEqual(result, 0)
            self.assertIn('Complete flows: 1', output.getvalue())
            self.assertIn('triggers=10', output.getvalue())
            self.assertIn('policy=fast-repair-neutral', output.getvalue())
            self.assertIn('outer-multiplier=1', output.getvalue())

    def test_validate_legacy_rto1_manifest_with_comparison_fields(self):
        with tempfile.TemporaryDirectory() as run_dir:
            manifest = current_manifest(legacy_comparison_fields=True)
            with open(
                os.path.join(run_dir, 'run_manifest.json'), 'w', encoding='ascii'
            ) as manifest_file:
                json.dump(manifest, manifest_file)

            validated = analyze_shadow_fast_repair.validate_run_manifest(run_dir)
            self.assertEqual(
                validated['_validatedRtoBackoffPolicy'], 'fast-repair-neutral'
            )
            self.assertFalse(
                validated['_validatedFastRepairAdvancesRtoBackoff']
            )

    def test_rejects_obsolete_consumer_variants(self):
        obsolete_variants = (
            'shadow-fast-350-8',
            'shadow-fast-350-8-no-backoff',
            'shadow-fast-350-8-no-backoff-rto2',
            'shadow-fast-350-8-no-backoff-rto3',
            'shadow-fast-350-8-no-backoff-rto4',
        )
        for variant in obsolete_variants:
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as run_dir:
                manifest = current_manifest()
                manifest['consumer']['variant'] = variant
                with open(
                    os.path.join(run_dir, 'run_manifest.json'),
                    'w',
                    encoding='ascii',
                ) as manifest_file:
                    json.dump(manifest, manifest_file)

                with self.assertRaisesRegex(ValueError, 'unsupported.*consumer'):
                    analyze_shadow_fast_repair.validate_run_manifest(run_dir)

    def test_rejects_obsolete_forwarder_variant(self):
        with tempfile.TemporaryDirectory() as run_dir:
            manifest = current_manifest()
            manifest['forwarder']['default']['variant'] = 'ndnd-qdisc-netem-2ms'
            with open(
                os.path.join(run_dir, 'run_manifest.json'),
                'w',
                encoding='ascii',
            ) as manifest_file:
                json.dump(manifest, manifest_file)

            with self.assertRaisesRegex(ValueError, 'unsupported forwarder'):
                analyze_shadow_fast_repair.validate_run_manifest(run_dir)

    def test_rejects_rto_manifest_parameter_drift(self):
        with tempfile.TemporaryDirectory() as run_dir:
            manifest = current_manifest()
            manifest['rtoEstimator']['minRtoMs'] = 200
            with open(
                os.path.join(run_dir, 'run_manifest.json'),
                'w',
                encoding='ascii',
            ) as manifest_file:
                json.dump(manifest, manifest_file)

            with self.assertRaisesRegex(ValueError, 'rtoEstimator.minRtoMs'):
                analyze_shadow_fast_repair.validate_run_manifest(run_dir)


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python3

import argparse
from dataclasses import dataclass
import glob
import json
import os
import re
import sys
from typing import Optional

from analyze_shadow_loss import read_shadow_loss_summaries
from flow_completion import read_flow_summaries


_FIELD_RE = re.compile(r'([A-Za-z][A-Za-z0-9]*)=("[^"]*"|\S+)')
_CONSUMER_VARIANT = 'shadow-fast-350-8-no-backoff-rto1'
_FORWARDER_VARIANT = 'persistent-2ms-netem-bwcap'
_RTO_BACKOFF_POLICY = 'fast-repair-neutral'
_RTO_OUTER_MULTIPLIER = 1


@dataclass(frozen=True)
class FastRepairSummary:
    flow: str
    triggers: int
    queued: int
    already_queued: int
    already_triggered: int
    stale: int
    sent: int
    resolved: int
    resolved_after_send: int
    fast_repair_advances_rto_backoff: Optional[bool]
    rto_outer_multiplier: Optional[int]


def _unquote(value):
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    return value


def _parse_bool(fields, name, expected):
    value = fields[name].lower()
    if value not in {'true', 'false'}:
        raise ValueError(f'{name} must be true or false')
    parsed = value == 'true'
    if parsed is not expected:
        raise ValueError(f'{name} must be {str(expected).lower()}')


def _optional_bool(fields, name):
    if name not in fields:
        return None
    value = fields[name].lower()
    if value not in {'true', 'false'}:
        raise ValueError(f'{name} must be true or false')
    return value == 'true'


def _optional_int(fields, name):
    if name not in fields:
        return None
    return int(fields[name])


def parse_fast_repair_summary_line(line):
    if 'msg="Shadow Fast Repair Summary"' not in line:
        return None

    fields = {key: _unquote(value) for key, value in _FIELD_RE.findall(line)}
    required = {
        'flow',
        'behavioralRetransmission',
        'ageFloorMs',
        'laterAckThreshold',
        'maxPerGeneration',
        'usesUnifiedPacing',
        'rateBackoff',
        'cwndBackoff',
        'triggers',
        'queued',
        'alreadyQueued',
        'alreadyTriggered',
        'stale',
        'sent',
        'resolved',
        'resolvedAfterSend',
    }
    missing = sorted(required.difference(fields))
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")

    _parse_bool(fields, 'behavioralRetransmission', True)
    _parse_bool(fields, 'usesUnifiedPacing', True)
    _parse_bool(fields, 'rateBackoff', False)
    _parse_bool(fields, 'cwndBackoff', False)
    if int(fields['ageFloorMs']) != 350 or int(fields['laterAckThreshold']) != 8:
        raise ValueError('unexpected fast-repair threshold')
    if int(fields['maxPerGeneration']) != 1:
        raise ValueError('maxPerGeneration must be 1')

    try:
        summary = FastRepairSummary(
            flow=fields['flow'],
            triggers=int(fields['triggers']),
            queued=int(fields['queued']),
            already_queued=int(fields['alreadyQueued']),
            already_triggered=int(fields['alreadyTriggered']),
            stale=int(fields['stale']),
            sent=int(fields['sent']),
            resolved=int(fields['resolved']),
            resolved_after_send=int(fields['resolvedAfterSend']),
            fast_repair_advances_rto_backoff=_optional_bool(
                fields, 'fastRepairAdvancesRtoBackoff'
            ),
            rto_outer_multiplier=_optional_int(fields, 'rtoOuterMultiplier'),
        )
    except ValueError as error:
        raise ValueError('invalid numeric field') from error

    counters = (
        summary.triggers,
        summary.queued,
        summary.already_queued,
        summary.already_triggered,
        summary.stale,
        summary.sent,
        summary.resolved,
        summary.resolved_after_send,
    )
    if any(value < 0 for value in counters):
        raise ValueError('counters must be non-negative')
    outcomes = (
        summary.queued
        + summary.already_queued
        + summary.already_triggered
        + summary.stale
    )
    if outcomes != summary.triggers:
        raise ValueError(f'queue outcomes {outcomes} != triggers {summary.triggers}')
    if summary.sent > summary.queued:
        raise ValueError('sent exceeds queued')
    if summary.resolved > summary.queued:
        raise ValueError('resolved exceeds queued')
    if summary.resolved_after_send > summary.sent or (
        summary.resolved_after_send > summary.resolved
    ):
        raise ValueError('resolvedAfterSend exceeds sent or resolved')
    if (
        summary.rto_outer_multiplier is not None
        and summary.rto_outer_multiplier != _RTO_OUTER_MULTIPLIER
    ):
        raise ValueError('rtoOuterMultiplier must be 1')
    return summary


def validate_run_manifest(run_dir):
    manifest_path = os.path.join(run_dir, 'run_manifest.json')
    try:
        with open(manifest_path, 'r', encoding='ascii') as manifest_file:
            manifest = json.load(manifest_file)
    except (FileNotFoundError, json.JSONDecodeError) as error:
        raise ValueError(f'invalid or missing run manifest: {error}') from error

    variant = manifest.get('consumer', {}).get('variant')
    if variant != _CONSUMER_VARIANT:
        raise ValueError(
            f'unsupported fast-repair consumer variant {variant!r}; '
            f'expected {_CONSUMER_VARIANT!r}'
        )
    forwarder_variant = (
        manifest.get('forwarder', {}).get('default', {}).get('variant')
    )
    if forwarder_variant != _FORWARDER_VARIANT:
        raise ValueError(
            f'unsupported forwarder variant {forwarder_variant!r}; '
            f'expected {_FORWARDER_VARIANT!r}'
        )
    diagnostics = manifest.get('shadowLossDiagnostics', {})
    if diagnostics.get('enabled') is not True:
        raise ValueError('shadow diagnostics are not enabled')
    if diagnostics.get('behavioralRetransmission') is not True:
        raise ValueError('behavioral retransmission is not enabled')
    fast_repair = manifest.get('fastRepair', {})
    expected = {
        'enabled': True,
        'ageFloorMs': 350,
        'laterAckThreshold': 8,
        'maxPerGeneration': 1,
        'usesExistingRetransmissionQueue': True,
        'usesUnifiedPacing': True,
        'rateBackoff': False,
        'cwndBackoff': False,
    }
    for key, value in expected.items():
        if fast_repair.get(key) != value:
            raise ValueError(f'unexpected fastRepair.{key}')
    if fast_repair.get('rtoBackoffPolicy') != _RTO_BACKOFF_POLICY:
        raise ValueError('unexpected fastRepair.rtoBackoffPolicy')
    if fast_repair.get('fastRepairAdvancesRtoBackoff') is not False:
        raise ValueError('unexpected fastRepair.fastRepairAdvancesRtoBackoff')
    if fast_repair.get('regularRecoveryAdvancesRtoBackoff') is not True:
        raise ValueError('unexpected fastRepair.regularRecoveryAdvancesRtoBackoff')

    expected_rto_estimator = {
        'family': 'sliding-window-mean-population-stddev',
        'formulaAfterTwoSamples': (
            'clamp(M * (meanRTT + 4 * stdDev), MIN_RTO, MAX_RTO)'
        ),
        'formulaBeforeTwoSamples': (
            'clamp(M * max(MIN_RTO, 2 * latestRTT), MIN_RTO, MAX_RTO)'
        ),
        'outerMultiplier': _RTO_OUTER_MULTIPLIER,
        'stdDevMultiplier': 4,
        'rttWindowMs': 600,
        'minRtoMs': 240,
        'maxRtoMs': 3500,
        'retransmissionJitterRatio': 0.10,
        'ordinaryRecoveryBackoffFactor': 2,
        'buildTimeFixed': True,
    }
    rto_estimator = manifest.get('rtoEstimator', {})
    for key, value in expected_rto_estimator.items():
        if rto_estimator.get(key) != value:
            raise ValueError(f'unexpected rtoEstimator.{key}')

    manifest['_validatedRtoBackoffPolicy'] = _RTO_BACKOFF_POLICY
    manifest['_validatedFastRepairAdvancesRtoBackoff'] = False
    manifest['_validatedRtoOuterMultiplier'] = _RTO_OUTER_MULTIPLIER
    expected_flow_count = diagnostics.get('expectedFlowCount')
    if not isinstance(expected_flow_count, int) or expected_flow_count <= 0:
        raise ValueError('invalid expected flow count')
    return manifest


def _percent(numerator, denominator):
    if denominator <= 0:
        return 0.0
    return 100.0 * numerator / denominator


def analyze_run(run_dir):
    run_dir = os.path.abspath(run_dir)
    try:
        manifest = validate_run_manifest(run_dir)
    except ValueError as error:
        print(f'[ERROR] {error}', file=sys.stderr)
        return 1

    log_paths = sorted(glob.glob(os.path.join(run_dir, 'con*_app.log')))
    if not log_paths:
        print(f'No consumer application logs found in {run_dir}', file=sys.stderr)
        return 1

    summaries = []
    selected_shadow = []
    errors = []
    flow_summaries = {}
    for log_path in log_paths:
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as log_file:
                for line_number, line in enumerate(log_file, start=1):
                    if not line.endswith('\n') or 'msg="Shadow Fast Repair Summary"' not in line:
                        continue
                    try:
                        summaries.append(parse_fast_repair_summary_line(line))
                    except ValueError as error:
                        errors.append(f'{log_path}:{line_number}: {error}')
        except FileNotFoundError:
            continue
        shadow_records, shadow_errors = read_shadow_loss_summaries(log_path)
        errors.extend(shadow_errors)
        selected_shadow.extend(
            summary
            for summary in shadow_records
            if summary.age_floor_ms == 350 and summary.later_ack_threshold == 8
        )
        parsed_flows, flow_errors = read_flow_summaries(log_path)
        errors.extend(f'{log_path}: {error}' for error in flow_errors)
        for flow, summary in parsed_flows.items():
            if flow in flow_summaries:
                errors.append(f'duplicate Flow Summary for {flow}')
            flow_summaries[flow] = summary

    if errors:
        for error in errors:
            print(f'[ERROR] {error}', file=sys.stderr)
        return 1
    expected_flow_count = manifest['shadowLossDiagnostics']['expectedFlowCount']
    summary_flows = {summary.flow for summary in summaries}
    if len(summaries) != expected_flow_count or len(summary_flows) != expected_flow_count:
        print('[ERROR] missing or duplicate fast-repair flow summaries', file=sys.stderr)
        return 1
    if set(flow_summaries) != summary_flows:
        print('[ERROR] Flow Summary and fast-repair flow sets differ', file=sys.stderr)
        return 1
    incomplete = [flow for flow, summary in flow_summaries.items() if not summary.is_complete()]
    if incomplete:
        print(f'[ERROR] incomplete flows: {sorted(incomplete)}', file=sys.stderr)
        return 1
    if len(selected_shadow) != expected_flow_count or any(
        not summary.behavioral_retransmission for summary in selected_shadow
    ):
        print('[ERROR] missing or invalid 350 ms / 8 ACK shadow summaries', file=sys.stderr)
        return 1
    expected_advances = manifest['_validatedFastRepairAdvancesRtoBackoff']
    reported_advances = {
        summary.fast_repair_advances_rto_backoff
        for summary in summaries
        if summary.fast_repair_advances_rto_backoff is not None
    }
    if reported_advances != {expected_advances}:
        print('[ERROR] fast-repair summary disagrees with manifest RTO policy', file=sys.stderr)
        return 1
    expected_multiplier = manifest['_validatedRtoOuterMultiplier']
    reported_multipliers = {
        summary.rto_outer_multiplier
        for summary in summaries
        if summary.rto_outer_multiplier is not None
    }
    if expected_multiplier is not None and reported_multipliers != {expected_multiplier}:
        print('[ERROR] fast-repair summary disagrees with manifest RTO multiplier', file=sys.stderr)
        return 1

    totals = {
        field: sum(getattr(summary, field) for summary in summaries)
        for field in (
            'triggers',
            'queued',
            'already_queued',
            'already_triggered',
            'stale',
            'sent',
            'resolved',
            'resolved_after_send',
        )
    }
    shadow_suspects = sum(summary.shadow_suspects for summary in selected_shadow)
    shadow_resolved = sum(summary.resolved_before_rto for summary in selected_shadow)
    shadow_reached = sum(summary.reached_rto for summary in selected_shadow)

    print(f'Run: {run_dir}')
    print(f'Consumer SHA-256: {manifest["consumer"]["sha256"]}')
    print(f'Complete flows: {len(flow_summaries)}; behavioral fast repair: enabled')
    print('Fast repair: age=350 ms, laterACK=8, max/generation=1, unified pacing')
    print(
        'RTO backoff: policy={}; fast-repair-advances={}'.format(
            manifest['_validatedRtoBackoffPolicy'],
            str(expected_advances).lower(),
        )
    )
    if expected_multiplier is not None:
        print(
            'RTO estimator: outer-multiplier={}; '
            'RTO=clamp(M*(meanRTT+4*stdDev), 240ms, 3500ms)'.format(
                expected_multiplier
            )
        )
    print(
        '  triggers={triggers:,}; queued={queued:,}; already-queued={already_queued:,}; '
        'already-triggered={already_triggered:,}; stale={stale:,}'.format(**totals)
    )
    print(
        f'  sent={totals["sent"]:,} ({_percent(totals["sent"], totals["queued"]):.3f}% '
        f'of queued); resolved={totals["resolved"]:,}; '
        f'resolved-after-send={totals["resolved_after_send"]:,}'
    )
    print(
        f'Selected shadow classification: suspects={shadow_suspects:,}; '
        f'resolved-before-RTO={shadow_resolved:,}; reached-RTO={shadow_reached:,}'
    )
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Validate and aggregate the MARS 350 ms / 8 ACK fast-repair run.'
    )
    parser.add_argument('run_dir', help='MARS run log directory')
    args = parser.parse_args()
    return analyze_run(args.run_dir)


if __name__ == '__main__':
    raise SystemExit(main())

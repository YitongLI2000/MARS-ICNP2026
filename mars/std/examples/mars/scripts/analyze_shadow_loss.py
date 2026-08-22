#!/usr/bin/env python3

from dataclasses import dataclass
import re


_FIELD_RE = re.compile(r'([A-Za-z][A-Za-z0-9]*)=("[^"]*"|\S+)')


@dataclass(frozen=True)
class ShadowLossSummary:
    flow: str
    behavioral_retransmission: bool
    age_floor_ms: int
    later_ack_threshold: int
    shadow_suspects: int
    resolved_before_rto: int
    reached_rto: int
    active_at_end: int
    first_attempt_rtos: int
    lead_to_rto_total_ms: float
    avg_lead_to_rto_ms: float
    suspect_inflight_total_ms: float
    suspect_inflight_avg_ms: float


def _unquote(value):
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    return value


def parse_shadow_loss_summary_line(line):
    if 'msg="Shadow Loss Summary"' not in line:
        return None

    fields = {key: _unquote(value) for key, value in _FIELD_RE.findall(line)}
    required = {
        'flow',
        'behavioralRetransmission',
        'ageFloorMs',
        'laterAckThreshold',
        'shadowSuspects',
        'resolvedBeforeRTO',
        'reachedRTO',
        'activeAtEnd',
        'firstAttemptRTOs',
        'leadToRTOTotalMs',
        'avgLeadToRTOMs',
        'suspectInflightTotalMs',
        'suspectInflightAvgMs',
    }
    missing = sorted(required.difference(fields))
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")

    behavioral_text = fields['behavioralRetransmission'].lower()
    if behavioral_text not in {'true', 'false'}:
        raise ValueError('behavioralRetransmission must be true or false')

    try:
        summary = ShadowLossSummary(
            flow=fields['flow'],
            behavioral_retransmission=behavioral_text == 'true',
            age_floor_ms=int(fields['ageFloorMs']),
            later_ack_threshold=int(fields['laterAckThreshold']),
            shadow_suspects=int(fields['shadowSuspects']),
            resolved_before_rto=int(fields['resolvedBeforeRTO']),
            reached_rto=int(fields['reachedRTO']),
            active_at_end=int(fields['activeAtEnd']),
            first_attempt_rtos=int(fields['firstAttemptRTOs']),
            lead_to_rto_total_ms=float(fields['leadToRTOTotalMs']),
            avg_lead_to_rto_ms=float(fields['avgLeadToRTOMs']),
            suspect_inflight_total_ms=float(fields['suspectInflightTotalMs']),
            suspect_inflight_avg_ms=float(fields['suspectInflightAvgMs']),
        )
    except ValueError as error:
        raise ValueError('invalid numeric field') from error

    counts = (
        summary.shadow_suspects,
        summary.resolved_before_rto,
        summary.reached_rto,
        summary.active_at_end,
        summary.first_attempt_rtos,
    )
    durations = (
        summary.lead_to_rto_total_ms,
        summary.avg_lead_to_rto_ms,
        summary.suspect_inflight_total_ms,
        summary.suspect_inflight_avg_ms,
    )
    if any(value < 0 for value in counts) or any(value < 0 for value in durations):
        raise ValueError('counters and durations must be non-negative')
    classified = (
        summary.resolved_before_rto + summary.reached_rto + summary.active_at_end
    )
    if classified != summary.shadow_suspects:
        raise ValueError(
            f'classified candidates {classified} != shadowSuspects '
            f'{summary.shadow_suspects}'
        )
    if (
        not summary.behavioral_retransmission
        and summary.reached_rto > summary.first_attempt_rtos
    ):
        raise ValueError('reachedRTO exceeds firstAttemptRTOs')
    return summary


def read_shadow_loss_summaries(log_path):
    summaries = []
    errors = []
    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as log_file:
            for line_number, line in enumerate(log_file, start=1):
                if not line.endswith('\n') or 'msg="Shadow Loss Summary"' not in line:
                    continue
                try:
                    summaries.append(parse_shadow_loss_summary_line(line))
                except ValueError as error:
                    errors.append(f'{log_path}:{line_number}: {error}')
    except FileNotFoundError:
        pass
    return summaries, errors

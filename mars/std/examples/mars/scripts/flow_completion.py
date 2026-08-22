#!/usr/bin/env python3

from dataclasses import dataclass
import re


_FIELD_RE = re.compile(r'([A-Za-z][A-Za-z0-9]*)=("[^"]*"|\S+)')
EXPECTED_PACKETS_PER_FLOW = 3032


@dataclass(frozen=True)
class FlowSummary:
    flow: str
    complete: bool
    received_packets: int
    expected_packets: int
    missing_packets: int

    def is_complete(self, expected_packets=EXPECTED_PACKETS_PER_FLOW):
        return (
            self.complete
            and self.received_packets == expected_packets
            and self.expected_packets == expected_packets
            and self.missing_packets == 0
        )


def parse_flow_summary_line(line):
    if 'msg="Flow Summary"' not in line:
        return None

    fields = {
        key: value[1:-1] if value.startswith('"') and value.endswith('"') else value
        for key, value in _FIELD_RE.findall(line)
    }
    required = {
        'flow',
        'complete',
        'receivedPackets',
        'expectedPackets',
        'missingPackets',
    }
    missing = sorted(required.difference(fields))
    if missing:
        raise ValueError(f"Flow Summary missing fields: {', '.join(missing)}")

    complete_text = fields['complete'].lower()
    if complete_text not in {'true', 'false'}:
        raise ValueError(f"invalid complete value: {fields['complete']}")

    try:
        received_packets = int(fields['receivedPackets'])
        expected_packets = int(fields['expectedPackets'])
        missing_packets = int(fields['missingPackets'])
    except ValueError as exc:
        raise ValueError("packet counts must be integers") from exc

    return FlowSummary(
        flow=fields['flow'],
        complete=complete_text == 'true',
        received_packets=received_packets,
        expected_packets=expected_packets,
        missing_packets=missing_packets,
    )


def read_flow_summaries(log_path):
    summaries = {}
    errors = []
    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as log_file:
            for line_number, line in enumerate(log_file, start=1):
                # The runner may poll while the final log record is still being written.
                if not line.endswith('\n'):
                    continue
                if 'msg="Flow Summary"' not in line:
                    continue
                try:
                    summary = parse_flow_summary_line(line)
                except ValueError as exc:
                    errors.append(f"line {line_number}: {exc}")
                    continue
                if summary.flow in summaries:
                    errors.append(f"line {line_number}: duplicate flow {summary.flow}")
                    continue
                summaries[summary.flow] = summary
    except FileNotFoundError:
        pass
    return summaries, errors

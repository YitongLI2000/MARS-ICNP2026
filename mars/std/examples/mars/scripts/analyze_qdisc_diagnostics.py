#!/usr/bin/env python3

import argparse
import collections
import dataclasses
import json
import os
import re
import shlex
import sys


DIAGNOSTIC_MESSAGE = 'Qdisc sampler diagnostic'
HIERARCHY_AUDIT_MESSAGE = 'Qdisc hierarchy audit'
GIB = 1024 ** 3


@dataclasses.dataclass(frozen=True)
class DiagnosticRecord:
    node: str
    timestamp: str
    interval_seconds: float
    interfaces: int
    dumps: int
    dump_errors: int
    dump_busy_percent: float
    dump_max_us: float
    dumps_ge_1ms: int
    read_buffer_allocations: int
    netlink_socket_opens: int
    estimated_read_buffer_alloc_bytes: int
    go_total_alloc_bytes: int
    go_mallocs: int
    go_gcs: int
    go_gc_pause_ns: int
    process_cpu_valid: bool
    process_cpu_time_seconds: float
    process_cpu_percent: float
    udp_counters_valid: bool
    udp_in_datagrams: int
    udp_in_errors: int
    udp_rcvbuf_errors: int
    udp_sndbuf_errors: int


@dataclasses.dataclass(frozen=True)
class HierarchyAuditRecord:
    node: str
    timestamp: str
    interface: str
    ifindex: int
    entries: int
    data_qsf_raw_bytes: int
    data_qsf_raw_packets: int
    root_bytes: int
    root_packets: int
    non_root_bytes: int
    non_root_packets: int
    max_layer_bytes: int
    max_layer_packets: int
    possible_double_count: bool
    layers: str
    aggregation: str
    selected_entries: int
    selected_kinds: str
    selected_handles: str
    all_layer_sum_bytes: int
    all_layer_sum_packets: int
    selection_fields_present: bool


@dataclasses.dataclass
class NodeSummary:
    node: str
    interval_seconds: float = 0.0
    interfaces: int = 0
    records: int = 0
    dumps: int = 0
    dump_errors: int = 0
    dump_busy_seconds: float = 0.0
    dump_max_us: float = 0.0
    dumps_ge_1ms: int = 0
    read_buffer_allocations: int = 0
    netlink_socket_opens: int = 0
    estimated_read_buffer_alloc_bytes: int = 0
    go_total_alloc_bytes: int = 0
    go_mallocs: int = 0
    go_gcs: int = 0
    go_gc_pause_ns: int = 0
    cpu_interval_seconds: float = 0.0
    process_cpu_time_seconds: float = 0.0
    peak_process_cpu_percent: float = 0.0
    udp_valid_records: int = 0
    udp_in_datagrams: int = 0
    udp_in_errors: int = 0
    udp_rcvbuf_errors: int = 0
    udp_sndbuf_errors: int = 0

    def add(self, record):
        self.interval_seconds += record.interval_seconds
        self.interfaces = max(self.interfaces, record.interfaces)
        self.records += 1
        self.dumps += record.dumps
        self.dump_errors += record.dump_errors
        self.dump_busy_seconds += (
            record.interval_seconds * record.dump_busy_percent / 100
        )
        self.dump_max_us = max(self.dump_max_us, record.dump_max_us)
        self.dumps_ge_1ms += record.dumps_ge_1ms
        self.read_buffer_allocations += record.read_buffer_allocations
        self.netlink_socket_opens += record.netlink_socket_opens
        self.estimated_read_buffer_alloc_bytes += (
            record.estimated_read_buffer_alloc_bytes
        )
        self.go_total_alloc_bytes += record.go_total_alloc_bytes
        self.go_mallocs += record.go_mallocs
        self.go_gcs += record.go_gcs
        self.go_gc_pause_ns += record.go_gc_pause_ns
        if record.process_cpu_valid:
            self.cpu_interval_seconds += record.interval_seconds
            self.process_cpu_time_seconds += record.process_cpu_time_seconds
            self.peak_process_cpu_percent = max(
                self.peak_process_cpu_percent,
                record.process_cpu_percent,
            )
        if record.udp_counters_valid:
            self.udp_valid_records += 1
            self.udp_in_datagrams += record.udp_in_datagrams
            self.udp_in_errors += record.udp_in_errors
            self.udp_rcvbuf_errors += record.udp_rcvbuf_errors
            self.udp_sndbuf_errors += record.udp_sndbuf_errors

    def rate(self, value):
        if self.interval_seconds <= 0:
            return 0.0
        return value / self.interval_seconds


def parse_bool(value, field_name):
    normalized = value.strip().lower()
    if normalized == 'true':
        return True
    if normalized == 'false':
        return False
    raise ValueError(f'{field_name} has invalid boolean value {value!r}')


def parse_diagnostic_line(line, node):
    fields = {}
    for token in shlex.split(line):
        if '=' not in token:
            continue
        key, value = token.split('=', 1)
        fields[key] = value
    if fields.get('msg') != DIAGNOSTIC_MESSAGE:
        return None

    def integer(name):
        return int(fields[name])

    def number(name):
        return float(fields[name])

    def optional_integer(name):
        return int(fields.get(name, '0'))

    interval_seconds = number('intervalMs') / 1000
    if interval_seconds <= 0:
        raise ValueError('intervalMs must be positive')

    return DiagnosticRecord(
        node=node,
        timestamp=fields.get('time', 'unknown'),
        interval_seconds=interval_seconds,
        interfaces=integer('interfaces'),
        dumps=integer('dumps'),
        dump_errors=integer('dumpErrors'),
        dump_busy_percent=number('dumpBusyPct'),
        dump_max_us=number('dumpMaxUs'),
        dumps_ge_1ms=integer('dumpsGe1ms'),
        read_buffer_allocations=integer('readBufferAllocs'),
        netlink_socket_opens=optional_integer('netlinkSocketOpens'),
        estimated_read_buffer_alloc_bytes=integer(
            'estimatedReadBufferAllocBytes'
        ),
        go_total_alloc_bytes=integer('goTotalAllocBytes'),
        go_mallocs=integer('goMallocs'),
        go_gcs=integer('goGCs'),
        go_gc_pause_ns=integer('goGCPauseNs'),
        process_cpu_valid=parse_bool(
            fields['processCPUValid'], 'processCPUValid'
        ),
        process_cpu_time_seconds=number('processCPUTimeMs') / 1000,
        process_cpu_percent=number('processCPUPct'),
        udp_counters_valid=parse_bool(
            fields['udpCountersValid'], 'udpCountersValid'
        ),
        udp_in_datagrams=integer('udpInDatagrams'),
        udp_in_errors=integer('udpInErrors'),
        udp_rcvbuf_errors=integer('udpRcvbufErrors'),
        udp_sndbuf_errors=integer('udpSndbufErrors'),
    )


def parse_hierarchy_audit_line(line, node):
    fields = {}
    for token in shlex.split(line):
        if '=' not in token:
            continue
        key, value = token.split('=', 1)
        fields[key] = value
    if fields.get('msg') != HIERARCHY_AUDIT_MESSAGE:
        return None

    def integer(name):
        return int(fields[name])

    selection_fields_present = 'allLayerSumPackets' in fields
    data_qsf_raw_bytes = integer('dataQsfRawBytes')
    data_qsf_raw_packets = integer('dataQsfRawPackets')
    return HierarchyAuditRecord(
        node=node,
        timestamp=fields.get('time', 'unknown'),
        interface=fields['interface'],
        ifindex=integer('ifindex'),
        entries=integer('entries'),
        data_qsf_raw_bytes=data_qsf_raw_bytes,
        data_qsf_raw_packets=data_qsf_raw_packets,
        root_bytes=integer('rootBytes'),
        root_packets=integer('rootPackets'),
        non_root_bytes=integer('nonRootBytes'),
        non_root_packets=integer('nonRootPackets'),
        max_layer_bytes=integer('maxLayerBytes'),
        max_layer_packets=integer('maxLayerPackets'),
        possible_double_count=parse_bool(
            fields['possibleDoubleCount'], 'possibleDoubleCount'
        ),
        layers=fields.get('layers', ''),
        aggregation=fields.get('aggregation', 'sum-all-qdisc-layers'),
        selected_entries=int(fields.get('selectedEntries', '0')),
        selected_kinds=fields.get('selectedKinds', ''),
        selected_handles=fields.get('selectedHandles', ''),
        all_layer_sum_bytes=int(
            fields.get('allLayerSumBytes', str(data_qsf_raw_bytes))
        ),
        all_layer_sum_packets=int(
            fields.get('allLayerSumPackets', str(data_qsf_raw_packets))
        ),
        selection_fields_present=selection_fields_present,
    )


def load_records(run_dir):
    records = []
    parse_errors = []
    for entry in sorted(os.scandir(run_dir), key=lambda item: item.name):
        if not entry.is_file() or not entry.name.endswith('.log'):
            continue
        node = entry.name[:-4]
        with open(entry.path, 'r', encoding='utf-8', errors='replace') as log_file:
            for line_number, line in enumerate(log_file, start=1):
                if DIAGNOSTIC_MESSAGE not in line:
                    continue
                try:
                    record = parse_diagnostic_line(line, node)
                except (KeyError, ValueError) as error:
                    parse_errors.append(
                        f'{entry.name}:{line_number}: {error}'
                    )
                    continue
                if record is not None:
                    records.append(record)
    return records, parse_errors


def load_hierarchy_records(run_dir):
    records = []
    parse_errors = []
    for entry in sorted(os.scandir(run_dir), key=lambda item: item.name):
        if not entry.is_file() or not entry.name.endswith('.log'):
            continue
        node = entry.name[:-4]
        with open(entry.path, 'r', encoding='utf-8', errors='replace') as log_file:
            for line_number, line in enumerate(log_file, start=1):
                if HIERARCHY_AUDIT_MESSAGE not in line:
                    continue
                try:
                    record = parse_hierarchy_audit_line(line, node)
                except (KeyError, ValueError) as error:
                    parse_errors.append(
                        f'{entry.name}:{line_number}: {error}'
                    )
                    continue
                if record is not None:
                    records.append(record)
    return records, parse_errors


def summarize_by_node(records):
    summaries = {}
    for record in records:
        summary = summaries.setdefault(record.node, NodeSummary(record.node))
        summary.add(record)
    return summaries


def node_role(node):
    match = re.match(r'([a-z]+)', node)
    return match.group(1) if match else 'other'


def load_manifest(run_dir):
    path = os.path.join(run_dir, 'run_manifest.json')
    if not os.path.isfile(path):
        return None
    with open(path, 'r', encoding='utf-8') as manifest_file:
        return json.load(manifest_file)


def average(values):
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def _percent(numerator, denominator):
    return 100 * numerator / denominator if denominator else 0.0


def print_role_table(node_summaries):
    roles = collections.defaultdict(list)
    for summary in node_summaries.values():
        roles[node_role(summary.node)].append(summary)

    print('\nPer-role averages and UDP overflow totals')
    print(
        'role   nodes  ifaces/node  dumps/s/node  dump-busy  process-CPU  '
        'buffer-GiB/s  RcvbufErrors'
    )
    role_order = {'con': 0, 'core': 1, 'edge': 2, 'pro': 3}
    for role, summaries in sorted(
        roles.items(), key=lambda item: (role_order.get(item[0], 99), item[0])
    ):
        dump_rate = average(summary.rate(summary.dumps) for summary in summaries)
        busy_percent = 100 * average(
            summary.rate(summary.dump_busy_seconds) for summary in summaries
        )
        cpu_percent = 100 * average(
            summary.process_cpu_time_seconds / summary.cpu_interval_seconds
            if summary.cpu_interval_seconds > 0 else 0
            for summary in summaries
        )
        buffer_rate = average(
            summary.rate(summary.estimated_read_buffer_alloc_bytes)
            for summary in summaries
        ) / GIB
        print(
            f'{role:<6} {len(summaries):>5}  '
            f'{average(summary.interfaces for summary in summaries):>11.2f}  '
            f'{dump_rate:>12.2f}  {busy_percent:>9.2f}%  '
            f'{cpu_percent:>10.2f}%  {buffer_rate:>12.3f}  '
            f'{sum(summary.udp_rcvbuf_errors for summary in summaries):>12,}'
        )


def print_summary(run_dir, records, parse_errors):
    node_summaries = summarize_by_node(records)
    print(f'Run: {os.path.abspath(run_dir)}')
    manifest = load_manifest(run_dir)
    if manifest is not None:
        settings = manifest.get('qdiscSamplerDiagnostics', {})
        print(
            'Manifest diagnostics: '
            f"enabled={settings.get('enabled', 'unknown')}, "
            f"interval={settings.get('reportIntervalSec', 'unknown')}s, "
            f"samplerBehaviorChanged={settings.get('samplerBehaviorChanged', 'unknown')}"
        )
        if 'samplerVariant' in settings:
            print(
                'Manifest sampler: '
                f"variant={settings.get('samplerVariant')}, "
                f"pollInterval={settings.get('pollIntervalMs', 'unknown')}ms, "
                f"persistentNetlink={settings.get('persistentNetlink', 'unknown')}, "
                f"udpReceiveBufferChanged="
                f"{settings.get('udpReceiveBufferChanged', 'unknown')}"
            )
        audit_settings = manifest.get('qdiscHierarchyAudit', {})
        if audit_settings:
            print(
                'Manifest hierarchy audit: '
                f"enabled={audit_settings.get('enabled', 'unknown')}, "
                f"extraNetlinkDumps="
                f"{audit_settings.get('extraNetlinkDumps', 'unknown')}, "
                f"affectsDataQsf={audit_settings.get('affectsDataQsf', 'unknown')}"
            )
            if 'dataQsfAggregation' in audit_settings:
                print(
                    'Manifest DataQsf aggregation: '
                    f"{audit_settings.get('dataQsfAggregation')}"
                )
    print(
        f'Diagnostic records: {len(records):,} across '
        f'{len(node_summaries)} forwarders'
    )
    if parse_errors:
        print(f'Warning: {len(parse_errors)} diagnostic lines could not be parsed')
        for error in parse_errors[:10]:
            print(f'  {error}')

    if not records:
        print('No qdisc sampler diagnostic records found.', file=sys.stderr)
        return False

    total_dumps = sum(summary.dumps for summary in node_summaries.values())
    total_dump_errors = sum(
        summary.dump_errors for summary in node_summaries.values()
    )
    total_slow_dumps = sum(
        summary.dumps_ge_1ms for summary in node_summaries.values()
    )
    total_buffer_bytes = sum(
        summary.estimated_read_buffer_alloc_bytes
        for summary in node_summaries.values()
    )
    total_buffer_allocations = sum(
        summary.read_buffer_allocations for summary in node_summaries.values()
    )
    total_socket_opens = sum(
        summary.netlink_socket_opens for summary in node_summaries.values()
    )
    total_go_alloc_bytes = sum(
        summary.go_total_alloc_bytes for summary in node_summaries.values()
    )

    aggregate_dump_rate = sum(
        summary.rate(summary.dumps) for summary in node_summaries.values()
    )
    aggregate_buffer_rate = sum(
        summary.rate(summary.estimated_read_buffer_alloc_bytes)
        for summary in node_summaries.values()
    )
    aggregate_go_alloc_rate = sum(
        summary.rate(summary.go_total_alloc_bytes)
        for summary in node_summaries.values()
    )
    aggregate_dump_busy = sum(
        summary.rate(summary.dump_busy_seconds)
        for summary in node_summaries.values()
    )
    aggregate_process_cpu = sum(
        summary.process_cpu_time_seconds / summary.cpu_interval_seconds
        if summary.cpu_interval_seconds > 0 else 0
        for summary in node_summaries.values()
    )
    aggregate_gc_rate = sum(
        summary.rate(summary.go_gcs) for summary in node_summaries.values()
    )

    print('\nSampler and runtime cost')
    print(f'  qdisc dumps: {total_dumps:,} ({aggregate_dump_rate:,.1f}/s across all forwarders)')
    slow_dump_share = (
        100 * total_slow_dumps / total_dumps if total_dumps > 0 else 0
    )
    print(
        f'  dump errors: {total_dump_errors:,}; dumps >=1 ms: '
        f'{total_slow_dumps:,} ({slow_dump_share:.3f}%)'
    )
    print(
        f'  dump wall-time occupancy: {aggregate_dump_busy:.2f} '
        'sampler-goroutine equivalents (includes syscall wait; not CPU time)'
    )
    print(
        f'  estimated 1 MiB read-buffer allocation: {total_buffer_bytes / GIB:,.2f} GiB total, '
        f'{aggregate_buffer_rate / GIB:,.2f} GiB/s'
    )
    print(
        f'  persistent-reader resources: {total_socket_opens:,} netlink socket open(s), '
        f'{total_buffer_allocations:,} read-buffer allocation(s)'
    )
    print(
        f'  observed Go TotalAlloc: {total_go_alloc_bytes / GIB:,.2f} GiB total, '
        f'{aggregate_go_alloc_rate / GIB:,.2f} GiB/s'
    )
    print(
        f'  process CPU: {aggregate_process_cpu:.2f} CPU-equivalents; '
        f'GC rate: {aggregate_gc_rate:,.1f}/s across all forwarders'
    )
    peak = max(records, key=lambda record: record.process_cpu_percent)
    peak_dump = max(records, key=lambda record: record.dump_busy_percent)
    print(
        f'  peak 5 s process CPU: {peak.process_cpu_percent:.2f}% '
        f'on {peak.node} at {peak.timestamp}'
    )
    print(
        f'  peak 5 s dump busy time: {peak_dump.dump_busy_percent:.2f}% '
        f'on {peak_dump.node} at {peak_dump.timestamp} '
        f'(max single dump {peak_dump.dump_max_us / 1000:.3f} ms)'
    )

    print_role_table(node_summaries)

    udp_valid_records = sum(
        summary.udp_valid_records for summary in node_summaries.values()
    )
    udp_in_datagrams = sum(
        summary.udp_in_datagrams for summary in node_summaries.values()
    )
    udp_in_errors = sum(
        summary.udp_in_errors for summary in node_summaries.values()
    )
    udp_rcvbuf_errors = sum(
        summary.udp_rcvbuf_errors for summary in node_summaries.values()
    )
    udp_sndbuf_errors = sum(
        summary.udp_sndbuf_errors for summary in node_summaries.values()
    )
    affected = sorted(
        (
            (summary.node, summary.udp_rcvbuf_errors)
            for summary in node_summaries.values()
            if summary.udp_rcvbuf_errors > 0
        ),
        key=lambda item: (-item[1], item[0]),
    )

    print('\nUDP receive-buffer evidence (/proc/net/snmp deltas)')
    print(f'  valid diagnostic intervals: {udp_valid_records:,}/{len(records):,}')
    print(f'  InDatagrams: {udp_in_datagrams:,}')
    print(f'  InErrors: {udp_in_errors:,}')
    print(f'  RcvbufErrors: {udp_rcvbuf_errors:,}')
    print(f'  SndbufErrors: {udp_sndbuf_errors:,}')
    if affected:
        print(f'  affected forwarders: {len(affected)}')
        for node, errors in affected:
            print(f'    {node}: {errors:,}')
        print(
            'Conclusion: nonzero RcvbufErrors is direct evidence that UDP '
            'datagrams were dropped because receive socket buffers overflowed.'
        )
    else:
        print(
            'Conclusion: this run contains no kernel RcvbufErrors evidence. '
            'That rules out UDP receive-buffer overflow for the observed intervals, '
            'but not qdisc drops or NDNLP reassembly eviction.'
        )
    return True


def print_hierarchy_summary(records, parse_errors):
    print('\nQdisc hierarchy / DataQsf aggregation audit')
    if parse_errors:
        print(f'  warning: {len(parse_errors)} hierarchy lines could not be parsed')
        for error in parse_errors[:10]:
            print(f'    {error}')
    if not records:
        print('  no hierarchy audit records found')
        return

    selected_aggregation = any(
        record.selection_fields_present for record in records
    )
    active = [record for record in records if record.all_layer_sum_packets > 0]
    possible_duplicates = [
        record for record in active if record.possible_double_count
    ]

    print(
        f'  records: {len(records):,}; active backlog records: {len(active):,}'
    )
    duplicate_share = (
        100 * len(possible_duplicates) / len(active) if active else 0
    )
    print(
        f'  matching nonzero root/non-root backlog: '
        f'{len(possible_duplicates):,}/{len(active):,} ({duplicate_share:.3f}%)'
    )

    if selected_aggregation:
        selected_matches_max = [
            record
            for record in active
            if record.data_qsf_raw_packets == record.max_layer_packets
        ]
        all_sum_exceeds_selected = [
            record
            for record in active
            if record.all_layer_sum_packets > record.data_qsf_raw_packets
        ]
        ratios = [
            record.all_layer_sum_packets / record.data_qsf_raw_packets
            for record in active
            if record.data_qsf_raw_packets > 0
        ]
        netem_selected = [
            record
            for record in records
            if 'netem' in record.selected_kinds.split(',')
        ]
        print(
            f'  selected DataQsf equals largest layer: '
            f'{len(selected_matches_max):,}/{len(active):,} '
            f'({_percent(len(selected_matches_max), len(active)):.3f}%)'
        )
        print(
            f'  all-layer sum exceeds selected DataQsf: '
            f'{len(all_sum_exceeds_selected):,}/{len(active):,} '
            f'({_percent(len(all_sum_exceeds_selected), len(active)):.3f}%)'
        )
        print(
            f'  records selecting netem: {len(netem_selected):,}/{len(records):,} '
            f'({_percent(len(netem_selected), len(records)):.3f}%)'
        )
        if ratios:
            print(
                f'  all-layer sum / selected DataQsf packets: '
                f'average={sum(ratios) / len(ratios):.3f}, '
                f'max={max(ratios):.3f}'
            )
        examples = sorted(
            all_sum_exceeds_selected,
            key=lambda record: (
                record.all_layer_sum_packets /
                max(record.data_qsf_raw_packets, 1),
                record.all_layer_sum_packets,
            ),
            reverse=True,
        )[:10]
        if examples:
            print('  strongest de-duplication examples:')
            for record in examples:
                ratio = (
                    record.all_layer_sum_packets /
                    max(record.data_qsf_raw_packets, 1)
                )
                print(
                    f'    {record.node}/{record.interface} at {record.timestamp}: '
                    f'selected={record.data_qsf_raw_packets}, '
                    f'all-sum={record.all_layer_sum_packets}, ratio={ratio:.3f}; '
                    f'selected={record.selected_kinds}@{record.selected_handles}; '
                    f'{record.layers}'
                )
        return

    inflated = [
        record
        for record in active
        if record.max_layer_packets > 0
        and record.data_qsf_raw_packets > record.max_layer_packets
    ]
    inflation = [
        record.data_qsf_raw_packets / record.max_layer_packets
        for record in active
        if record.max_layer_packets > 0
    ]
    inflated_share = 100 * len(inflated) / len(active) if active else 0
    print(
        f'  current all-qdisc sum exceeds largest layer: '
        f'{len(inflated):,}/{len(active):,} ({inflated_share:.3f}%)'
    )
    if inflation:
        print(
            f'  all-qdisc sum / largest-layer packets: '
            f'average={sum(inflation) / len(inflation):.3f}, '
            f'max={max(inflation):.3f}'
        )

    examples = sorted(
        inflated,
        key=lambda record: (
            record.data_qsf_raw_packets / record.max_layer_packets,
            record.data_qsf_raw_packets,
        ),
        reverse=True,
    )[:10]
    if examples:
        print('  strongest active examples:')
        for record in examples:
            ratio = record.data_qsf_raw_packets / record.max_layer_packets
            print(
                f'    {record.node}/{record.interface} at {record.timestamp}: '
                f'sum={record.data_qsf_raw_packets}, '
                f'max-layer={record.max_layer_packets}, ratio={ratio:.3f}; '
                f'{record.layers}'
            )


def main():
    parser = argparse.ArgumentParser(
        description='Aggregate temporary Mars qdisc sampler diagnostics.'
    )
    parser.add_argument('run_dir', help='Mars run log directory')
    args = parser.parse_args()

    if not os.path.isdir(args.run_dir):
        parser.error(f'run directory does not exist: {args.run_dir}')
    records, parse_errors = load_records(args.run_dir)
    hierarchy_records, hierarchy_errors = load_hierarchy_records(args.run_dir)
    if not print_summary(args.run_dir, records, parse_errors):
        return 1
    print_hierarchy_summary(hierarchy_records, hierarchy_errors)
    return 0 if not parse_errors and not hierarchy_errors else 2


if __name__ == '__main__':
    sys.exit(main())

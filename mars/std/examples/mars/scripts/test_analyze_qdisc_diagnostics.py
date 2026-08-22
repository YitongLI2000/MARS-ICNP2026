import contextlib
import io
import os
import tempfile
import unittest

from analyze_qdisc_diagnostics import (
    load_hierarchy_records,
    load_records,
    parse_diagnostic_line,
    parse_hierarchy_audit_line,
    print_hierarchy_summary,
    summarize_by_node,
)


def diagnostic_line(**overrides):
    fields = {
        'time': '2026-08-13T20:45:42.000-05:00',
        'level': 'INFO',
        'msg': '"Qdisc sampler diagnostic"',
        'intervalMs': '5000',
        'interfaces': '5',
        'dumps': '4000',
        'dumpErrors': '0',
        'dumpRateHz': '800',
        'dumpBusyPct': '25',
        'dumpAvgUs': '312.5',
        'dumpMaxUs': '2500',
        'dumpsGe1ms': '20',
        'readBufferAllocs': '4000',
        'netlinkSocketOpens': '1',
        'estimatedReadBufferAllocBytes': '4194304000',
        'goTotalAllocBytes': '4200000000',
        'goHeapAllocBytes': '10000000',
        'goMallocs': '20000',
        'goGCs': '12',
        'goGCPauseNs': '3000000',
        'processCPUValid': 'true',
        'processCPUTimeMs': '3000',
        'processCPUPct': '60',
        'udpCountersValid': 'true',
        'udpInDatagrams': '1000',
        'udpInErrors': '3',
        'udpRcvbufErrors': '2',
        'udpSndbufErrors': '0',
    }
    fields.update(overrides)
    return ' '.join(f'{key}={value}' for key, value in fields.items()) + '\n'


def hierarchy_line(**overrides):
    fields = {
        'time': '2026-08-13T20:45:42.000-05:00',
        'level': 'INFO',
        'msg': '"Qdisc hierarchy audit"',
        'interface': 'core0-con0',
        'ifindex': '7',
        'entries': '2',
        'dataQsfRawBytes': '28400',
        'dataQsfRawPackets': '20',
        'rootBytes': '14200',
        'rootPackets': '10',
        'nonRootBytes': '14200',
        'nonRootPackets': '10',
        'maxLayerBytes': '14200',
        'maxLayerPackets': '10',
        'possibleDoubleCount': 'true',
        'layers': '"htb@5:0,parent=root,bytes=14200,packets=10;'
                  'netem@a:0,parent=5:1,bytes=14200,packets=10"',
    }
    fields.update(overrides)
    return ' '.join(f'{key}={value}' for key, value in fields.items()) + '\n'


def selected_hierarchy_line(**overrides):
    fields = {
        'aggregation': 'prefer-non-root-netem',
        'dataQsfRawBytes': '14200',
        'dataQsfRawPackets': '10',
        'selectedEntries': '1',
        'selectedKinds': 'netem',
        'selectedHandles': 'a:0',
        'allLayerSumBytes': '28400',
        'allLayerSumPackets': '20',
    }
    fields.update(overrides)
    return hierarchy_line(**fields)


class AnalyzeQdiscDiagnosticsTest(unittest.TestCase):
    def test_parse_diagnostic_line(self):
        record = parse_diagnostic_line(diagnostic_line(), 'con0')
        self.assertEqual(record.node, 'con0')
        self.assertEqual(record.interval_seconds, 5)
        self.assertEqual(record.dumps, 4000)
        self.assertEqual(record.netlink_socket_opens, 1)
        self.assertEqual(record.udp_rcvbuf_errors, 2)
        self.assertAlmostEqual(record.process_cpu_time_seconds, 3)

    def test_load_and_summarize_records(self):
        with tempfile.TemporaryDirectory() as run_dir:
            with open(
                os.path.join(run_dir, 'con0.log'),
                'w',
                encoding='ascii',
            ) as log_file:
                log_file.write('unrelated line\n')
                log_file.write(diagnostic_line())
                log_file.write(diagnostic_line(udpRcvbufErrors='4'))

            records, errors = load_records(run_dir)
            self.assertEqual(errors, [])
            self.assertEqual(len(records), 2)
            summary = summarize_by_node(records)['con0']
            self.assertEqual(summary.dumps, 8000)
            self.assertEqual(summary.netlink_socket_opens, 2)
            self.assertEqual(summary.udp_rcvbuf_errors, 6)
            self.assertAlmostEqual(summary.interval_seconds, 10)

    def test_parse_hierarchy_audit_line(self):
        record = parse_hierarchy_audit_line(hierarchy_line(), 'core0')
        self.assertEqual(record.interface, 'core0-con0')
        self.assertEqual(record.data_qsf_raw_packets, 20)
        self.assertEqual(record.max_layer_packets, 10)
        self.assertTrue(record.possible_double_count)
        self.assertIn('netem@a:0', record.layers)
        self.assertFalse(record.selection_fields_present)
        self.assertEqual(record.all_layer_sum_packets, 20)

    def test_parse_selected_hierarchy_audit_line(self):
        record = parse_hierarchy_audit_line(
            selected_hierarchy_line(),
            'core0',
        )
        self.assertEqual(record.aggregation, 'prefer-non-root-netem')
        self.assertEqual(record.data_qsf_raw_packets, 10)
        self.assertEqual(record.all_layer_sum_packets, 20)
        self.assertEqual(record.selected_entries, 1)
        self.assertEqual(record.selected_kinds, 'netem')
        self.assertEqual(record.selected_handles, 'a:0')
        self.assertTrue(record.selection_fields_present)

    def test_print_selected_hierarchy_summary(self):
        record = parse_hierarchy_audit_line(
            selected_hierarchy_line(),
            'core0',
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            print_hierarchy_summary([record], [])
        rendered = output.getvalue()
        self.assertIn('selected DataQsf equals largest layer: 1/1', rendered)
        self.assertIn('all-layer sum exceeds selected DataQsf: 1/1', rendered)
        self.assertIn('records selecting netem: 1/1', rendered)

    def test_load_hierarchy_records(self):
        with tempfile.TemporaryDirectory() as run_dir:
            with open(
                os.path.join(run_dir, 'core0.log'),
                'w',
                encoding='ascii',
            ) as log_file:
                log_file.write('unrelated line\n')
                log_file.write(hierarchy_line())

            records, errors = load_hierarchy_records(run_dir)
            self.assertEqual(errors, [])
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].node, 'core0')


if __name__ == '__main__':
    unittest.main()

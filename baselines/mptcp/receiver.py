#!/usr/bin/env python3

import argparse
import csv
import queue
import signal
import socket
import threading
import time

from fct_protocol import (
    MARS_EXPECTED_BYTES,
    MARS_EXPECTED_MESSAGES,
    MARS_FCT_METRIC,
    MARS_FCT_STANDARD,
    MARS_LOGICAL_UNIT,
    MARS_MESSAGE_SIZE_BYTES,
    MARS_START_BOUNDARY,
    START_PREAMBLE_SIZE,
    decode_start_preamble,
)

IPPROTO_MPTCP = getattr(socket, "IPPROTO_MPTCP", 262)
SO_PROTOCOL = getattr(socket, "SO_PROTOCOL", 38)
SESSION_LOG_INTERVAL_SECONDS = 0.5
FCT_PERCENTILES = (95, 99, 100)
DEFAULT_RECV_SIZE_BYTES = 65536


def parse_bool(value):
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def parse_csv_intervals(raw_value):
    intervals = []
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            interval_ms = int(item)
        except ValueError:
            continue
        if interval_ms > 0:
            intervals.append(interval_ms / 1000.0)
    return intervals or [0.05, 0.10]


def log_print(message):
    print(message, flush=True)


def nearest_rank_target(total, percentile):
    if total <= 0 or percentile <= 0:
        return 0
    if percentile >= 100:
        return total
    return (total * percentile + 99) // 100


class FlowFCTMilestones:
    """Track message-equivalent completion targets on a TCP byte stream."""

    def __init__(
        self,
        expected_messages,
        expected_bytes,
        message_size_bytes,
        metric="message_equivalent_byte_completion",
        standard="native",
        start_boundary="receiver_after_accept",
        logical_unit="application_write",
    ):
        self.expected_messages = expected_messages
        self.expected_bytes = expected_bytes
        self.message_size_bytes = message_size_bytes
        self.metric = metric
        self.standard = standard
        self.start_boundary = start_boundary
        self.logical_unit = logical_unit
        self.enabled = any(
            value != 0
            for value in (expected_messages, expected_bytes, message_size_bytes)
        )

        if not self.enabled:
            self.target_messages = {}
            self.target_bytes = {}
            self.elapsed = {}
            return

        if min(expected_messages, expected_bytes, message_size_bytes) <= 0:
            raise ValueError(
                "expected messages, expected bytes, and message size must all be positive"
            )
        if not (
            (expected_messages - 1) * message_size_bytes
            < expected_bytes
            <= expected_messages * message_size_bytes
        ):
            raise ValueError(
                "expected bytes must describe exactly the configured number of messages"
            )

        self.target_messages = {
            percentile: nearest_rank_target(expected_messages, percentile)
            for percentile in FCT_PERCENTILES
        }
        self.target_bytes = {
            percentile: (
                expected_bytes
                if percentile == 100
                else min(
                    self.target_messages[percentile] * message_size_bytes,
                    expected_bytes,
                )
            )
            for percentile in FCT_PERCENTILES
        }
        self.elapsed = {percentile: None for percentile in FCT_PERCENTILES}

    def observe(self, total_bytes, elapsed_seconds):
        if not self.enabled:
            return
        for percentile in FCT_PERCENTILES:
            if (
                self.elapsed[percentile] is None
                and total_bytes >= self.target_bytes[percentile]
            ):
                self.elapsed[percentile] = max(elapsed_seconds, 0.0)

    def status(self, received_bytes):
        if not self.enabled:
            return "disabled"
        if received_bytes < self.expected_bytes:
            return "incomplete"
        if received_bytes > self.expected_bytes:
            return "byte_mismatch"
        if any(self.elapsed[percentile] is None for percentile in FCT_PERCENTILES):
            return "incomplete"
        return "complete"

    def format_elapsed(self, percentile):
        value = self.elapsed.get(percentile)
        return "NA" if value is None else f"{value:.9f}"

    def summary_line(self, session_num, remote_addr, received_bytes):
        if not self.enabled:
            return None
        return (
            f"[fct_summary] session={session_num} remote={remote_addr} "
            f"expected_msgs={self.expected_messages} "
            f"message_size_bytes={self.message_size_bytes} "
            f"expected_bytes={self.expected_bytes} received_bytes={received_bytes} "
            f"fct95_target_msgs={self.target_messages[95]} "
            f"fct95_target_bytes={self.target_bytes[95]} "
            f"fct99_target_msgs={self.target_messages[99]} "
            f"fct99_target_bytes={self.target_bytes[99]} "
            f"fct100_target_msgs={self.target_messages[100]} "
            f"fct100_target_bytes={self.target_bytes[100]} "
            f"fct95_s={self.format_elapsed(95)} "
            f"fct99_s={self.format_elapsed(99)} "
            f"fct100_s={self.format_elapsed(100)} "
            f"method=nearest_rank metric={self.metric} "
            f"clock=monotonic status={self.status(received_bytes)} "
            f"standard={self.standard} start_boundary={self.start_boundary} "
            f"logical_unit={self.logical_unit}"
        )


def receive_start_preamble(conn):
    data = bytearray()
    while len(data) < START_PREAMBLE_SIZE:
        chunk = conn.recv(START_PREAMBLE_SIZE - len(data))
        if not chunk:
            raise ValueError("connection closed before the FCT start preamble")
        data.extend(chunk)
    return decode_start_preamble(bytes(data))


def next_payload_recv_size(total_bytes, milestones, max_size=DEFAULT_RECV_SIZE_BYTES):
    """Stop reads exactly at each pending byte milestone."""
    if max_size <= 0:
        raise ValueError("maximum recv size must be positive")
    if milestones.enabled:
        remaining = [
            target - total_bytes
            for target in milestones.target_bytes.values()
            if target > total_bytes
        ]
        if remaining:
            return min(max_size, min(remaining))
    return max_size


class ThroughputCSVLogger:
    def __init__(self, path):
        self._lock = threading.Lock()
        self._file = open(path, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        self._writer.writerow(
            [
                "elapsed_ms",
                "interval_ms",
                "actual_interval_ms",
                "session",
                "total_msgs",
                "total_bytes",
                "delta_bytes",
                "throughput_mbps",
                "sample_type",
            ]
        )
        self._file.flush()
        self._start_time = time.time()

    def log_session(
        self,
        interval_seconds,
        actual_interval_seconds,
        session_num,
        total_msgs,
        total_bytes,
        delta_bytes,
        mbps,
        sample_type,
    ):
        with self._lock:
            elapsed_ms = (time.time() - self._start_time) * 1000.0
            self._writer.writerow(
                [
                    f"{elapsed_ms:.3f}",
                    int(interval_seconds * 1000.0),
                    f"{actual_interval_seconds * 1000.0:.3f}",
                    session_num,
                    total_msgs,
                    total_bytes,
                    delta_bytes,
                    f"{mbps:.6f}",
                    sample_type,
                ]
            )
            self._file.flush()

    def close(self):
        with self._lock:
            self._file.flush()
            self._file.close()


def maybe_set_tcp_nodelay(sock, protocol):
    if protocol == IPPROTO_MPTCP:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError as exc:
        log_print(f"Warning: TCP_NODELAY is unavailable on this socket: {exc}")


def format_buffer_bytes(value):
    return f"{value} bytes ({value / 1024.0:.1f} KiB)"


def log_socket_buffers(sock, label):
    fields = []
    for name, optname in [("sndbuf", socket.SO_SNDBUF), ("rcvbuf", socket.SO_RCVBUF)]:
        try:
            value = sock.getsockopt(socket.SOL_SOCKET, optname)
            fields.append(f"{name}={format_buffer_bytes(value)}")
        except OSError as exc:
            fields.append(f"{name}=unavailable({exc})")
    log_print(f"{label}: " + " | ".join(fields))


def request_static_socket_buffers(sock, sndbuf, rcvbuf, label):
    requested = []
    if sndbuf > 0:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)
        requested.append(f"sndbuf={format_buffer_bytes(sndbuf)}")
    if rcvbuf > 0:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        requested.append(f"rcvbuf={format_buffer_bytes(rcvbuf)}")
    if requested:
        log_print(f"{label}: " + " | ".join(requested))


def format_time(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


def emit_session_progress(
    session_num,
    total_msgs,
    total_bytes,
    last_log_msgs,
    last_log_bytes,
    last_log_time,
):
    now = time.time()
    delta_msgs = total_msgs - last_log_msgs
    delta_bytes = total_bytes - last_log_bytes
    delta_time = max(now - last_log_time, 0.0001)
    mbps = (delta_bytes * 8.0) / (delta_time * 1000000.0)
    log_print(
        f"[{format_time(now)}] Session #{session_num} | Reads: {total_msgs} "
        f"| Window reads: {delta_msgs} | Window bytes: {delta_bytes} "
        f"| Recent throughput: {mbps:.2f} Mbps"
    )
    return total_msgs, total_bytes, now


def maybe_log_session_samples(
    sample_states,
    logger,
    session_num,
    total_msgs,
    total_bytes,
    now,
    sample_type,
):
    if logger is None:
        return

    for state in sample_states:
        actual_interval = now - state["last_sample_time"]
        if sample_type == "periodic" and actual_interval + 1e-9 < state["interval"]:
            continue

        delta_bytes = total_bytes - state["last_total_bytes"]
        if delta_bytes == 0:
            if sample_type == "periodic":
                state["last_sample_time"] = now
                state["last_total_bytes"] = total_bytes
            continue

        logger.log_session(
            interval_seconds=state["interval"],
            actual_interval_seconds=max(actual_interval, 0.0001),
            session_num=session_num,
            total_msgs=total_msgs,
            total_bytes=total_bytes,
            delta_bytes=delta_bytes,
            mbps=(delta_bytes * 8.0) / (max(actual_interval, 0.0001) * 1000000.0),
            sample_type=sample_type,
        )
        state["last_sample_time"] = now
        state["last_total_bytes"] = total_bytes


def monitor_connection(
    stats_queue,
    session_num,
    remote_addr,
    transport,
    intervals,
    logger,
    session_start_wall,
    session_start_monotonic,
    milestone_start_monotonic_ns,
    milestones,
):
    total_bytes = 0
    total_msgs = 0
    last_log_time = session_start_wall
    last_log_bytes = 0
    last_log_msgs = 0
    sample_states = [
        {
            "interval": interval,
            "last_sample_time": session_start_wall,
            "last_total_bytes": 0,
        }
        for interval in intervals
    ]

    while True:
        next_progress_due = SESSION_LOG_INTERVAL_SECONDS - (time.time() - last_log_time)
        next_sample_due = min(
            state["interval"] - (time.time() - state["last_sample_time"])
            for state in sample_states
        )
        timeout = max(0.0, min(next_progress_due, next_sample_due))

        try:
            item = stats_queue.get(timeout=timeout)
        except queue.Empty:
            item = "tick"

        now = time.time()
        if item is None:
            if total_msgs > last_log_msgs and now - last_log_time > 0:
                last_log_msgs, last_log_bytes, last_log_time = emit_session_progress(
                    session_num,
                    total_msgs,
                    total_bytes,
                    last_log_msgs,
                    last_log_bytes,
                    last_log_time,
                )
            maybe_log_session_samples(
                sample_states,
                logger,
                session_num,
                total_msgs,
                total_bytes,
                now,
                "final",
            )
            fct = time.monotonic() - session_start_monotonic
            log_print(
                f"[{format_time(now)}] Session #{session_num} closed | Remote: {remote_addr} "
                f"| Transport: {transport} | Total: {total_msgs} reads | {total_bytes} bytes "
                f"({total_bytes / 1024.0 / 1024.0:.2f} MB) | FCT: {fct:.4f} s"
            )
            summary = milestones.summary_line(session_num, remote_addr, total_bytes)
            if summary is not None:
                log_print(f"[{format_time(now)}] {summary}")
            return

        if item != "tick":
            received_bytes, received_at_ns = item
            total_bytes += received_bytes
            total_msgs += 1
            milestones.observe(
                total_bytes,
                (received_at_ns - milestone_start_monotonic_ns) / 1_000_000_000.0,
            )
            if total_msgs == 1:
                log_print(
                    f"[{format_time(now)}] Session #{session_num} | "
                    f"First read ({received_bytes} bytes)"
                )

        maybe_log_session_samples(
            sample_states,
            logger,
            session_num,
            total_msgs,
            total_bytes,
            now,
            "periodic",
        )

        if total_msgs > 0 and now - last_log_time >= SESSION_LOG_INTERVAL_SECONDS:
            last_log_msgs, last_log_bytes, last_log_time = emit_session_progress(
                session_num,
                total_msgs,
                total_bytes,
                last_log_msgs,
                last_log_bytes,
                last_log_time,
            )


def handle_connection(
    conn,
    session_num,
    transport,
    intervals,
    logger,
    sndbuf,
    rcvbuf,
    expected_messages,
    expected_bytes,
    message_size_bytes,
    expect_start_preamble,
):
    session_start_wall = time.time()
    session_start_monotonic_ns = time.monotonic_ns()
    session_start_monotonic = session_start_monotonic_ns / 1_000_000_000.0
    remote_addr = f"{conn.getpeername()[0]}:{conn.getpeername()[1]}"
    request_static_socket_buffers(
        conn,
        sndbuf,
        rcvbuf,
        f"Session #{session_num} requested static socket buffers",
    )
    log_socket_buffers(conn, f"Session #{session_num} socket buffers after accept")
    milestone_start_monotonic_ns = session_start_monotonic_ns
    milestone_metadata = {
        "metric": "message_equivalent_byte_completion",
        "standard": "native",
        "start_boundary": "receiver_after_accept",
        "logical_unit": "application_write",
    }
    if expect_start_preamble:
        try:
            sender_start_ns = receive_start_preamble(conn)
        except (OSError, ValueError) as error:
            log_print(f"Warning: session #{session_num} invalid FCT start preamble: {error}")
            conn.close()
            return
        milestone_start_monotonic_ns = sender_start_ns
        preamble_received_monotonic_ns = time.monotonic_ns()
        if milestone_start_monotonic_ns > preamble_received_monotonic_ns:
            log_print(
                f"Warning: session #{session_num} FCT start timestamp is in the future"
            )
            conn.close()
            return
        milestone_metadata = {
            "metric": MARS_FCT_METRIC,
            "standard": MARS_FCT_STANDARD,
            "start_boundary": MARS_START_BOUNDARY,
            "logical_unit": MARS_LOGICAL_UNIT,
        }
        log_print(
            f"Session #{session_num} FCT start preamble accepted: "
            f"sender_monotonic_ns={sender_start_ns} "
            "elapsed_to_receiver_s="
            f"{(preamble_received_monotonic_ns - milestone_start_monotonic_ns) / 1_000_000_000.0:.9f}"
        )

    milestones = FlowFCTMilestones(
        expected_messages,
        expected_bytes,
        message_size_bytes,
        **milestone_metadata,
    )
    stats_queue = queue.Queue(maxsize=1024)
    monitor_thread = threading.Thread(
        target=monitor_connection,
        args=(
            stats_queue,
            session_num,
            remote_addr,
            transport,
            intervals,
            logger,
            session_start_wall,
            session_start_monotonic,
            milestone_start_monotonic_ns,
            milestones,
        ),
        daemon=True,
    )
    monitor_thread.start()

    payload_bytes_received = 0
    try:
        while True:
            recv_size = next_payload_recv_size(
                payload_bytes_received,
                milestones,
            )
            data = conn.recv(recv_size)
            if not data:
                break
            payload_bytes_received += len(data)
            stats_queue.put((len(data), time.monotonic_ns()))
    except OSError as exc:
        log_print(f"Warning: session #{session_num} read error: {exc}")
    finally:
        stats_queue.put(None)
        monitor_thread.join()
        log_socket_buffers(conn, f"Session #{session_num} socket buffers before close")
        conn.close()


def install_signal_handlers(stop_event):
    def _handle_signal(signum, _frame):
        log_print(f"Received signal {signum}, shutting down receiver")
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)


def main():
    parser = argparse.ArgumentParser(description="MPTCP receiver")
    parser.add_argument("-port", "--port", default="6121", help="Listen port")
    parser.add_argument(
        "-multipath",
        "--multipath",
        type=parse_bool,
        default=True,
        help="Enable MPTCP on the listener",
    )
    parser.add_argument("-csv", "--csv", default="", help="Path to per-flow throughput CSV")
    parser.add_argument(
        "-sndbuf",
        "--sndbuf",
        type=int,
        default=0,
        help="Static SO_SNDBUF request in bytes (0 keeps kernel default/autotune)",
    )
    parser.add_argument(
        "-rcvbuf",
        "--rcvbuf",
        type=int,
        default=0,
        help="Static SO_RCVBUF request in bytes (0 keeps kernel default/autotune)",
    )
    parser.add_argument(
        "-csv-intervals",
        "--csv-intervals",
        default="50,100",
        help="Comma-separated throughput sampling intervals in ms",
    )
    parser.add_argument(
        "--expected-messages",
        type=int,
        default=0,
        help="Expected application writes per flow (0 disables exact FCT milestones)",
    )
    parser.add_argument(
        "--expected-bytes",
        type=int,
        default=0,
        help="Expected payload bytes per complete flow",
    )
    parser.add_argument(
        "--message-size-bytes",
        type=int,
        default=0,
        help="Full application write size used to derive FCT95/FCT99 byte targets",
    )
    parser.add_argument(
        "--expect-start-preamble",
        type=parse_bool,
        default=False,
        help="Require the sender's shared-monotonic FCT start preamble",
    )
    args = parser.parse_args()

    try:
        milestone_config = FlowFCTMilestones(
            args.expected_messages,
            args.expected_bytes,
            args.message_size_bytes,
        )
    except ValueError as error:
        parser.error(str(error))
    if args.expect_start_preamble and (
        args.expected_messages != MARS_EXPECTED_MESSAGES
        or args.expected_bytes != MARS_EXPECTED_BYTES
        or args.message_size_bytes != MARS_MESSAGE_SIZE_BYTES
    ):
        parser.error(
            "--expect-start-preamble requires the MARS 3032x6016 workload geometry"
        )

    intervals = parse_csv_intervals(args.csv_intervals)
    logger = ThroughputCSVLogger(args.csv) if args.csv else None
    stop_event = threading.Event()
    worker_threads = []
    install_signal_handlers(stop_event)

    protocol = IPPROTO_MPTCP if args.multipath else socket.IPPROTO_TCP
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM, protocol)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    request_static_socket_buffers(
        listener,
        args.sndbuf,
        args.rcvbuf,
        "Requested static listener socket buffers before listen",
    )
    listener.bind(("0.0.0.0", int(args.port)))
    listener.listen()
    listener.settimeout(1.0)

    log_print(f"MPTCP receiver listening on port {args.port}")
    if milestone_config.enabled:
        log_print(
            "Exact FCT milestones enabled: "
            f"expected_msgs={args.expected_messages} "
            f"expected_bytes={args.expected_bytes} "
            f"message_size_bytes={args.message_size_bytes} "
            f"fct95_target_bytes={milestone_config.target_bytes[95]} "
            f"fct99_target_bytes={milestone_config.target_bytes[99]} "
            f"fct100_target_bytes={milestone_config.target_bytes[100]}"
        )
    if logger is not None:
        log_print(f"Writing per-flow throughput samples to {args.csv}")
    log_print("Receiver ready, waiting for connections...")

    try:
        session_count = 0
        while not stop_event.is_set():
            try:
                conn, addr = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if stop_event.is_set():
                    break
                raise

            session_count += 1
            protocol_value = conn.getsockopt(socket.SOL_SOCKET, SO_PROTOCOL)
            maybe_set_tcp_nodelay(conn, protocol_value)
            transport = "mptcp" if protocol_value == IPPROTO_MPTCP else "tcp"
            log_print(
                f"New connection #{session_count} from {addr[0]}:{addr[1]} transport={transport}"
            )
            thread = threading.Thread(
                target=handle_connection,
                args=(
                    conn,
                    session_count,
                    transport,
                    intervals,
                    logger,
                    args.sndbuf,
                    args.rcvbuf,
                    args.expected_messages,
                    args.expected_bytes,
                    args.message_size_bytes,
                    args.expect_start_preamble,
                ),
                daemon=True,
            )
            thread.start()
            worker_threads.append(thread)
    finally:
        stop_event.set()
        listener.close()
        for thread in worker_threads:
            thread.join(timeout=2.0)
        if logger is not None:
            logger.close()


if __name__ == "__main__":
    main()

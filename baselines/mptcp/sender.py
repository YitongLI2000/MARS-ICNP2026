#!/usr/bin/env python3

import argparse
import socket
import sys
import time

from fct_protocol import (
    MARS_EXPECTED_BYTES,
    MARS_EXPECTED_MESSAGES,
    MARS_MESSAGE_SIZE_BYTES,
    MARS_START_BOUNDARY,
    encode_start_preamble,
)

IPPROTO_MPTCP = getattr(socket, "IPPROTO_MPTCP", 262)
SO_PROTOCOL = getattr(socket, "SO_PROTOCOL", 38)


def parse_bool(value):
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def build_parser():
    parser = argparse.ArgumentParser(description="MPTCP sender")
    parser.add_argument("-addr", "--addr", default="127.0.0.1:6121", help="Receiver address")
    parser.add_argument(
        "-count",
        "--count",
        type=int,
        default=1000,
        help="Chunk count when --total is unset",
    )
    parser.add_argument("-name", "--name", default="sender", help="Sender name for logs")
    parser.add_argument("-bind", "--bind", default="", help="Local IP to bind to")
    parser.add_argument("-size", "--size", type=int, default=1024, help="Chunk size in bytes")
    parser.add_argument("-total", "--total", type=int, default=0, help="Exact total payload bytes to send")
    parser.add_argument(
        "-pipeline",
        "--pipeline",
        type=int,
        default=1,
        help="Ignored for this byte-stream transport",
    )
    parser.add_argument(
        "-rate-mbps",
        "--rate-mbps",
        type=float,
        default=0.0,
        help="Optional application pacing rate",
    )
    parser.add_argument(
        "-require-mptcp",
        "--require-mptcp",
        type=parse_bool,
        default=True,
        help="Exit non-zero if the socket is not using MPTCP",
    )
    parser.add_argument(
        "--send-start-preamble",
        type=parse_bool,
        default=False,
        help="Send the shared-monotonic FCT start preamble before application data",
    )
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
    parser.add_argument("-timeout", "--timeout", type=float, default=30.0, help="Socket timeout in seconds")
    return parser


def write_all(sock, data):
    view = memoryview(data)
    while view:
        sent = sock.send(view)
        if sent <= 0:
            raise RuntimeError("socket send returned zero bytes")
        view = view[sent:]


def maybe_set_tcp_nodelay(sock, protocol):
    if protocol == IPPROTO_MPTCP:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError as exc:
        print(f"Warning: TCP_NODELAY is unavailable on this socket: {exc}")


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
    print(f"{label}: " + " | ".join(fields))


def request_static_socket_buffers(sock, sndbuf, rcvbuf, label):
    requested = []
    if sndbuf > 0:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)
        requested.append(f"sndbuf={format_buffer_bytes(sndbuf)}")
    if rcvbuf > 0:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        requested.append(f"rcvbuf={format_buffer_bytes(rcvbuf)}")
    if requested:
        print(f"{label}: " + " | ".join(requested))


def main():
    args = build_parser().parse_args()

    if args.size <= 0:
        raise SystemExit("chunk size must be positive")
    if args.count <= 0:
        raise SystemExit("count must be positive")
    if args.total < 0:
        raise SystemExit("total must be non-negative")
    if args.rate_mbps < 0:
        raise SystemExit("rate-mbps must be non-negative")

    host, port_text = args.addr.rsplit(":", 1)
    port = int(port_text)

    message_count = args.count
    total_bytes_target = args.count * args.size
    final_chunk_size = args.size
    if args.total > 0:
        message_count = args.total // args.size
        final_chunk_size = args.size
        remainder = args.total % args.size
        if remainder:
            message_count += 1
            final_chunk_size = remainder
        total_bytes_target = args.total

    if args.send_start_preamble and (
        message_count != MARS_EXPECTED_MESSAGES
        or args.size != MARS_MESSAGE_SIZE_BYTES
        or total_bytes_target != MARS_EXPECTED_BYTES
    ):
        raise SystemExit(
            "--send-start-preamble requires the MARS 3032x6016 workload geometry"
        )

    print(f"MPTCP sender [{args.name}] connecting to {args.addr}")
    print(f"Payload target: {total_bytes_target} bytes in {message_count} chunk(s)")
    if args.pipeline != 1:
        print(f"Pipeline flag ({args.pipeline}) is ignored for stream transport")
    if args.rate_mbps > 0:
        print(f"Application pacing: {args.rate_mbps:.3f} Mbps")
    if args.total > 0:
        print(f"Final chunk size: {final_chunk_size} bytes")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, IPPROTO_MPTCP)
    sock.settimeout(args.timeout)
    request_static_socket_buffers(
        sock,
        args.sndbuf,
        args.rcvbuf,
        "Requested static socket buffers before connect",
    )

    if args.bind:
        sock.bind((args.bind, 0))
        print(f"Binding to local IP: {args.bind}")

    sock.connect((host, port))
    protocol = sock.getsockopt(socket.SOL_SOCKET, SO_PROTOCOL)
    maybe_set_tcp_nodelay(sock, protocol)
    print(f"Socket protocol: {protocol} (mptcp={protocol == IPPROTO_MPTCP})")
    if args.require_mptcp and protocol != IPPROTO_MPTCP:
        raise SystemExit("connection is not using MPTCP")
    log_socket_buffers(sock, "Socket buffers after connect")

    payload = bytes((index % 251) + 1 for index in range(args.size))

    next_send_time = time.time()
    rate_bytes_per_second = 0.0
    if args.rate_mbps > 0:
        rate_bytes_per_second = (args.rate_mbps * 1000000.0) / 8.0

    start_monotonic_ns = time.monotonic_ns()
    if args.send_start_preamble:
        write_all(sock, encode_start_preamble(start_monotonic_ns))
        print(
            "FCT start preamble sent: "
            f"start_monotonic_ns={start_monotonic_ns} "
            f"start_boundary={MARS_START_BOUNDARY}"
        )
    total_bytes_sent = 0
    progress_every = max(1, message_count // 10)

    for index in range(message_count):
        chunk = payload
        remaining = total_bytes_target - total_bytes_sent
        if remaining < len(chunk):
            chunk = payload[:remaining]

        if args.rate_mbps > 0:
            sleep_for = next_send_time - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)
            chunk_duration = len(chunk) / rate_bytes_per_second
            next_send_time += chunk_duration

        write_all(sock, chunk)
        total_bytes_sent += len(chunk)

        if message_count >= 10 and (index + 1) % progress_every == 0:
            print(".", end="", flush=True)

    if message_count >= 10:
        print()

    try:
        sock.shutdown(socket.SHUT_WR)
    except OSError as exc:
        print(f"Warning: shutdown failed: {exc}")

    try:
        while sock.recv(65536):
            pass
    except socket.timeout:
        print("Warning: timed out while waiting for receiver close")

    log_socket_buffers(sock, "Socket buffers before close")

    duration = (time.monotonic_ns() - start_monotonic_ns) / 1_000_000_000.0
    throughput_mbps = (total_bytes_sent * 8.0) / (duration * 1000000.0)

    print(f"Completed transfer for {args.name}")
    print(f"Duration: {duration:.6f}s")
    print(f"Transferred: {total_bytes_sent} bytes ({total_bytes_sent / 1024.0 / 1024.0:.2f} MB)")
    print(f"Throughput: {throughput_mbps:.2f} Mbps")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except BrokenPipeError:
        sys.exit(1)

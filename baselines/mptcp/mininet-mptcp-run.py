#!/usr/bin/env python3

import argparse
import collections
import math
import os
import re
import shlex
import statistics
import subprocess
import sys
import time

from fct_protocol import (
    MARS_EXPECTED_BYTES,
    MARS_EXPECTED_MESSAGES,
    MARS_FCT_METRIC,
    MARS_FCT_STANDARD,
    MARS_LOGICAL_UNIT,
    MARS_MESSAGE_SIZE_BYTES,
    MARS_START_BOUNDARY,
)
from mininet.net import Mininet
from mininet.node import Node
from mininet.link import TCLink
from mininet.log import setLogLevel

# --- Configuration ---
NUM_CONSUMERS = 5
NUM_PRODUCERS = 25
NUM_CORES = 5
PRODUCERS_PER_CONSUMER = NUM_PRODUCERS // NUM_CONSUMERS
DEFAULT_LINK_BW_MBPS = 40
# CORE_LINK_MODE_DEFAULT = "homogeneous"
CORE_LINK_MODE_DEFAULT = "heterogeneous"
VALID_CORE_LINK_MODES = ("homogeneous", "heterogeneous")
HETEROGENEOUS_CORE_LINK_BWS_MBPS = [25, 40, 50, 60, 80]

# Loss rate in percentage (0 to 100) for Consumer->Core links
CON_TO_CORE_LOSS = 1
PRO_TO_EDGE_LOSS = 0

# Start delay for each consumer group (in seconds)
# This dictates when the senders targeting a specific consumer will start.
CONSUMER_START_DELAY_PATTERN = [0, 1, 2, 1, 2]
CONSUMER_START_DELAYS = {
    f"con{i}": CONSUMER_START_DELAY_PATTERN[i] for i in range(NUM_CONSUMERS)
}
GROUP_RELEASE_LEAD_SECONDS = 0.5

APP_CHUNK_SIZE_BYTES = MARS_MESSAGE_SIZE_BYTES
APP_TRANSFER_TOTAL_BYTES = MARS_EXPECTED_BYTES
APP_EXPECTED_MESSAGES = MARS_EXPECTED_MESSAGES
APP_PIPELINE_DEPTH = 8
# USE_STATIC_SOCKET_BUFFERS = True
USE_STATIC_SOCKET_BUFFERS = False
AUTOTUNE_MAX_SNDBUF_BYTES = 2 * 1024 * 1024
AUTOTUNE_MAX_RCVBUF_BYTES = 2 * 1024 * 1024
# Linux reports roughly double the requested TCP buffer size via getsockopt();
# request 512 KiB so the effective fixed buffers land near 1 MiB.
STATIC_SOCKET_SNDBUF_BYTES = 512 * 1024 if USE_STATIC_SOCKET_BUFFERS else 0
STATIC_SOCKET_RCVBUF_BYTES = 512 * 1024 if USE_STATIC_SOCKET_BUFFERS else 0

RECEIVER_PORT = 6121
# MPTCP_SUBFLOW_MODE = "single"
MPTCP_SUBFLOW_MODE = "consumer_cores"
MPTCP_PATH_EXPANDED_SUBFLOW_TOTAL = 8
# Valid subflow modes:
# - single: MPTCP-ECMP with one subflow over the ECMP/loopback path model
# - consumer_cores: MPTCP-Oracle with all five consumer-core attachments exposed
if MPTCP_SUBFLOW_MODE == "single":
    MPTCP_EXPERIMENT_MODE = "baseline_ecmp"
    MPTCP_MAX_TOTAL_SUBFLOWS = 1
    MPTCP_ENABLE_PRODUCER_SUBFLOW_ENDPOINTS = False
    MPTCP_ROUTE_PRODUCER_ENDPOINTS = False
    MPTCP_ROUTE_CONSUMER_ENDPOINTS = False
    MPTCP_SIGNAL_CONSUMER_ENDPOINTS = False
    MPTCP_BIND_SENDER_TO_DATA_ENDPOINT = False
elif MPTCP_SUBFLOW_MODE == "consumer_cores":
    MPTCP_EXPERIMENT_MODE = "oracle"
    MPTCP_MAX_TOTAL_SUBFLOWS = max(MPTCP_PATH_EXPANDED_SUBFLOW_TOTAL, 2)
    MPTCP_ENABLE_PRODUCER_SUBFLOW_ENDPOINTS = True
    MPTCP_ROUTE_PRODUCER_ENDPOINTS = True
    MPTCP_ROUTE_CONSUMER_ENDPOINTS = True
    MPTCP_SIGNAL_CONSUMER_ENDPOINTS = True
    MPTCP_BIND_SENDER_TO_DATA_ENDPOINT = True
else:
    raise ValueError(f"Unsupported MPTCP_SUBFLOW_MODE: {MPTCP_SUBFLOW_MODE}")

# Linux counts only additional subflows here; the initial MP_CAPABLE subflow is separate.
MPTCP_ADDITIONAL_SUBFLOW_LIMIT = max(MPTCP_MAX_TOTAL_SUBFLOWS - 1, 0)
MPTCP_ADD_ADDR_ACCEPTED = max(NUM_CORES + 2, MPTCP_MAX_TOTAL_SUBFLOWS)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_BIN = sys.executable or "python3"
RECEIVER_APP = os.path.join(SCRIPT_DIR, "receiver.py")
SENDER_APP = os.path.join(SCRIPT_DIR, "sender.py")
RESULTS_ROOT = os.path.join(SCRIPT_DIR, "results")
RESULTS_PROFILE = "mars_3032"
RESULTS_RUN_ID = os.environ.get("MPTCP_RESULTS_RUN_ID", "").strip()
if RESULTS_RUN_ID and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", RESULTS_RUN_ID):
    raise ValueError(
        "MPTCP_RESULTS_RUN_ID must be 1-64 characters using only letters, "
        "digits, '.', '_', or '-'"
    )
TCP_CONGESTION_CONTROL_PATH = "/proc/sys/net/ipv4/tcp_congestion_control"
REQUIRED_TCP_CONGESTION_CONTROL = "cubic"


def loss_label(loss):
    return str(loss).replace(".", "p")


def normalize_loss_percentage(value):
    try:
        normalized = float(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(f"invalid loss percentage: {value}") from error
    if not math.isfinite(normalized) or not 0 <= normalized <= 100:
        raise argparse.ArgumentTypeError("loss percentage must be between 0 and 100")
    return int(normalized) if normalized.is_integer() else normalized


def normalize_core_link_mode(core_link_mode):
    normalized = str(core_link_mode).strip().lower()
    if normalized not in VALID_CORE_LINK_MODES:
        raise ValueError(f"Unsupported core link mode: {core_link_mode}")
    return normalized


def run_dir_for_mode(core_link_mode):
    core_link_mode = normalize_core_link_mode(core_link_mode)
    profile_root = os.path.join(
        RESULTS_ROOT,
        core_link_mode,
        RESULTS_PROFILE,
    )
    if RESULTS_RUN_ID:
        profile_root = os.path.join(profile_root, "reproductions", RESULTS_RUN_ID)
    return os.path.join(
        profile_root,
        f"con_core_loss_{loss_label(CON_TO_CORE_LOSS)}pct",
    )


RUN_DIR = run_dir_for_mode(CORE_LINK_MODE_DEFAULT)
RUN_FCT_SUMMARY_FILENAME = "run_fct_summary.log"


def prepare_run_dir():
    """Create and clear the output directory for the active loss configuration."""
    if RESULTS_RUN_ID:
        os.makedirs(os.path.dirname(RUN_DIR), exist_ok=True)
        try:
            os.mkdir(RUN_DIR)
        except FileExistsError as error:
            raise RuntimeError(
                f"Refusing to reuse isolated run directory: {RUN_DIR}"
            ) from error
    else:
        os.makedirs(RUN_DIR, exist_ok=True)
        for name in os.listdir(RUN_DIR):
            if name.endswith((".log", ".csv", ".done")):
                os.remove(os.path.join(RUN_DIR, name))

    print(f"Writing run artifacts to {RUN_DIR}")


def require_cubic_congestion_control(path=TCP_CONGESTION_CONTROL_PATH):
    """Require the Linux default used by newly created MPTCP subflows."""
    try:
        with open(path, "r", encoding="ascii") as setting_file:
            current = setting_file.read().strip()
    except OSError as error:
        raise RuntimeError(
            f"cannot read TCP congestion control setting {path}: {error}"
        ) from error

    if current != REQUIRED_TCP_CONGESTION_CONTROL:
        raise RuntimeError(
            "net.ipv4.tcp_congestion_control must be cubic "
            f"(found {current or 'an empty value'})"
        )
    return current


def ensure_binaries_exist():
    """Validate the local sender/receiver scripts."""
    required = [
        RECEIVER_APP,
        SENDER_APP,
        os.path.join(SCRIPT_DIR, "fct_protocol.py"),
    ]
    for path in required:
        if not os.path.isfile(path):
            print(f"Missing application file: {path}")
            return False

    result = subprocess.run(
        [PYTHON_BIN, "-m", "py_compile", RECEIVER_APP, SENDER_APP],
        cwd=SCRIPT_DIR,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        print(result.stdout, end="")
        print(result.stderr, end="")
        return False

    return True


# --- Topology Definition ---
PRODUCERS = [f"pro{i}" for i in range(NUM_PRODUCERS)]
CONSUMERS = [f"con{i}" for i in range(NUM_CONSUMERS)]
EDGES = [f"edge{i}" for i in range(NUM_PRODUCERS)]
CORES = [f"core{i}" for i in range(NUM_CORES)]

NODES = PRODUCERS + CONSUMERS + EDGES + CORES


def build_links(core_link_mode):
    """Create the fixed 25-producer/5-consumer WAN topology."""
    core_link_mode = normalize_core_link_mode(core_link_mode)

    links = []

    for i in range(NUM_PRODUCERS):
        links.append(
            (
                f"pro{i}",
                f"edge{i}",
                DEFAULT_LINK_BW_MBPS,
                "5ms",
                1000,
                PRO_TO_EDGE_LOSS,
            )
        )

    for i in range(NUM_PRODUCERS):
        core_idx = i // PRODUCERS_PER_CONSUMER
        links.append(
            (
                f"edge{i}",
                f"core{core_idx}",
                DEFAULT_LINK_BW_MBPS,
                "5ms",
                1000,
                0,
            )
        )

    for c in range(NUM_CONSUMERS):
        for k in range(NUM_CORES):
            links.append(
                (
                    f"con{c}",
                    f"core{k}",
                    DEFAULT_LINK_BW_MBPS,
                    "5ms",
                    1000,
                    CON_TO_CORE_LOSS,
                )
            )

    heterogeneous_bw_count = len(HETEROGENEOUS_CORE_LINK_BWS_MBPS)
    core_mesh_link_index = 0
    for i in range(NUM_CORES):
        for j in range(i + 1, NUM_CORES):
            if core_link_mode == "heterogeneous":
                core_bw = HETEROGENEOUS_CORE_LINK_BWS_MBPS[
                    core_mesh_link_index % heterogeneous_bw_count
                ]
            else:
                core_bw = DEFAULT_LINK_BW_MBPS
            links.append((f"core{i}", f"core{j}", core_bw, "1ms", 1000, 0))
            core_mesh_link_index += 1

    return links


LINKS = build_links(CORE_LINK_MODE_DEFAULT)


def describe_core_mesh(links):
    descriptions = []
    for src_name, dst_name, bw, _, _, _ in links:
        if src_name.startswith("core") and dst_name.startswith("core"):
            descriptions.append(f"{src_name}-{dst_name}:{bw}Mbps")
    return ", ".join(descriptions)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the MPTCP Mininet emulation with selectable core-mesh bandwidth mode."
    )
    parser.add_argument(
        "--core-link-mode",
        type=normalize_core_link_mode,
        choices=VALID_CORE_LINK_MODES,
        default=CORE_LINK_MODE_DEFAULT,
        help=(
            "How to assign bandwidth to core-core mesh links. "
            "'homogeneous' keeps every core-core link at 40 Mbps. "
            "'heterogeneous' cycles [25, 40, 50, 60, 80] Mbps across core-core links only."
        ),
    )
    parser.add_argument(
        "--con-to-core-loss",
        type=normalize_loss_percentage,
        default=CON_TO_CORE_LOSS,
        help=(
            "Consumer-to-core packet loss percentage. The value also selects "
            "the con_core_loss_<loss>pct result directory."
        ),
    )
    return parser.parse_args()


class RoutingEngine:
    """
    Calculates shortest path next hops and assigns /30 subnets to every link.
    """

    def __init__(self, nodes, links):
        self.nodes = nodes
        self.links = links
        self.adj = collections.defaultdict(list)
        self.link_subnets = {}
        self._build_graph_and_subnets()

    def _build_graph_and_subnets(self):
        subnet_counter = 0
        for link in self.links:
            u, v = link[0], link[1]

            self.adj[u].append(v)
            self.adj[v].append(u)

            base_ip = subnet_counter * 4
            octet3 = base_ip // 256
            octet4 = base_ip % 256

            ip_u = f"172.16.{octet3}.{octet4 + 1}"
            ip_v = f"172.16.{octet3}.{octet4 + 2}"

            self.link_subnets[(u, v)] = {"u_ip": ip_u, "v_ip": ip_v}
            self.link_subnets[(v, u)] = {"u_ip": ip_v, "v_ip": ip_u}
            subnet_counter += 1

    def get_link_ip(self, node, neighbor):
        if (node, neighbor) in self.link_subnets:
            return self.link_subnets[(node, neighbor)]["u_ip"]
        return None

    def calculate_ecmp_routes(self, src, dst):
        """Breadth-first search to find all next hops on shortest paths."""
        if src == dst:
            return []

        queue = collections.deque([(src, 0)])
        visited = {src: 0}
        parents = collections.defaultdict(set)

        while queue:
            curr, dist = queue.popleft()
            if curr == dst:
                continue

            for neighbor in self.adj[curr]:
                new_dist = dist + 1
                if neighbor not in visited:
                    visited[neighbor] = new_dist
                    parents[neighbor].add(curr)
                    queue.append((neighbor, new_dist))
                elif visited[neighbor] == new_dist:
                    parents[neighbor].add(curr)

        if dst not in parents:
            return []

        valid_next_hops = set()

        def backtrack(node):
            if node == src:
                return
            for parent in parents[node]:
                if parent == src:
                    valid_next_hops.add(node)
                backtrack(parent)

        backtrack(dst)
        return list(valid_next_hops)


class LinuxRouter(Node):
    """A node with IP forwarding enabled."""

    def config(self, **params):
        super().config(**params)
        self.cmd("sysctl -w net.ipv4.ip_forward=1")


def run_checked(node, command):
    marker = "__CMD_RC__"
    wrapped = f"{command}; printf '{marker}%s' $?"
    result = node.cmd(f"bash -lc {shlex.quote(wrapped)}")
    body, _, rc_text = result.rpartition(marker)
    try:
        rc = int(rc_text.strip())
    except ValueError:
        rc = 1
    return rc, body.strip()


def build_delayed_background_command(wait_python_bin, sender_cmd, done_path, release_time):
    wait_snippet = (
        "import time; "
        f"target={release_time:.6f}; "
        "delay=target-time.time(); "
        "time.sleep(delay if delay > 0 else 0.0)"
    )
    wait_cmd = f"{shlex.quote(wait_python_bin)} -c {shlex.quote(wait_snippet)}"
    inner_cmd = f"{wait_cmd} && {sender_cmd}; echo $? > {shlex.quote(done_path)}"
    return f"bash -lc {shlex.quote(inner_cmd)} &"


def build_host_data_endpoints(engine):
    producer_endpoints = {}
    consumer_endpoints = {}

    for i in range(NUM_PRODUCERS):
        name = f"pro{i}"
        edge = f"edge{i}"
        producer_endpoints[name] = {
            "ip": engine.get_link_ip(name, edge),
            "attachment": edge,
            "intf": f"{name}-{edge}",
        }

    for i in range(NUM_CONSUMERS):
        name = f"con{i}"
        endpoints = []
        for k in range(NUM_CORES):
            core = f"core{k}"
            endpoints.append(
                {
                    "ip": engine.get_link_ip(name, core),
                    "attachment": core,
                    "intf": f"{name}-{core}",
                }
            )
        consumer_endpoints[name] = endpoints

    return producer_endpoints, consumer_endpoints


def build_route_targets(
    con_ips,
    pro_ips,
    producer_endpoints,
    consumer_endpoints,
    include_producer_endpoints=False,
    include_consumer_endpoints=False,
):
    targets = []

    for name, ip in con_ips.items():
        targets.append({"label": name, "ip": ip, "owner": name})

    for name, ip in pro_ips.items():
        targets.append({"label": name, "ip": ip, "owner": name})

    if include_producer_endpoints:
        for name, endpoint in producer_endpoints.items():
            targets.append(
                {
                    "label": f"{name}-data",
                    "ip": endpoint["ip"],
                    "owner": name,
                    "attachment": endpoint["attachment"],
                }
            )

    if include_consumer_endpoints:
        for name, endpoints in consumer_endpoints.items():
            for index, endpoint in enumerate(endpoints):
                targets.append(
                    {
                        "label": f"{name}-path{index}",
                        "ip": endpoint["ip"],
                        "owner": name,
                        "attachment": endpoint["attachment"],
                    }
                )

    return targets


def calculate_target_next_hops(engine, current_node_name, target):
    owner = target["owner"]
    attachment = target.get("attachment")

    if current_node_name == owner:
        return []

    if not attachment:
        return engine.calculate_ecmp_routes(current_node_name, owner)

    if current_node_name == attachment:
        return [owner]

    return engine.calculate_ecmp_routes(current_node_name, attachment)


def apply_computed_routes(nodes_obj, engine, route_targets):
    print("*** Applying computed routes")
    for current_node_name in NODES:
        current_node = nodes_obj[current_node_name]

        for target in route_targets:
            if current_node_name == target["owner"]:
                continue

            next_hops = calculate_target_next_hops(engine, current_node_name, target)
            if not next_hops:
                continue

            route_cmd = f"ip route add {target['ip']}/32 "
            if len(next_hops) == 1:
                next_hop = next_hops[0]
                gateway = engine.get_link_ip(next_hop, current_node_name)
                route_cmd += f"via {gateway}"
            else:
                for next_hop in next_hops:
                    gateway = engine.get_link_ip(next_hop, current_node_name)
                    route_cmd += f"nexthop via {gateway} weight 1 "

            current_node.cmd(route_cmd)


def configure_network_settings(net):
    """Optimize buffers, enable MPTCP, and disable reverse path filtering."""
    print("*** Tuning host settings")
    for host in net.hosts:
        if USE_STATIC_SOCKET_BUFFERS:
            host.cmd("sysctl -w net.core.rmem_max=16777216 2>/dev/null || true")
            host.cmd("sysctl -w net.core.wmem_max=16777216 2>/dev/null || true")
        else:
            host.cmd(f"sysctl -w net.core.rmem_max={AUTOTUNE_MAX_RCVBUF_BYTES} 2>/dev/null || true")
            host.cmd(f"sysctl -w net.core.wmem_max={AUTOTUNE_MAX_SNDBUF_BYTES} 2>/dev/null || true")
            host.cmd(
                "sysctl -w "
                f"net.ipv4.tcp_rmem='4096 87380 {AUTOTUNE_MAX_RCVBUF_BYTES}' "
                "2>/dev/null || true"
            )
            host.cmd(
                "sysctl -w "
                f"net.ipv4.tcp_wmem='4096 65536 {AUTOTUNE_MAX_SNDBUF_BYTES}' "
                "2>/dev/null || true"
            )
        host.cmd("sysctl -w net.mptcp.enabled=1 2>/dev/null || true")
        host.cmd("sysctl -w net.mptcp.allow_join_initial_addr_port=1 2>/dev/null || true")
        if STATIC_SOCKET_RCVBUF_BYTES > 0:
            host.cmd("sysctl -w net.ipv4.tcp_moderate_rcvbuf=0 2>/dev/null || true")
        else:
            host.cmd("sysctl -w net.ipv4.tcp_moderate_rcvbuf=1 2>/dev/null || true")
        host.cmd("sysctl -w net.ipv4.conf.all.rp_filter=0")
        host.cmd("sysctl -w net.ipv4.conf.default.rp_filter=0")
        host.cmd("for i in $(ls /sys/class/net/); do sysctl -w net.ipv4.conf.$i.rp_filter=0; done")


def configure_mptcp_endpoints(net, producer_endpoints, consumer_endpoints):
    """Reset path-manager state and configure the kernel PM for multi-subflow MPTCP."""
    print("*** Configuring MPTCP path-manager state")

    for host_name in PRODUCERS + CONSUMERS:
        node = net.get(host_name)
        for command, description in [
            ("ip mptcp endpoint flush", "flush endpoints"),
            (
                f"ip mptcp limits set subflows {MPTCP_ADDITIONAL_SUBFLOW_LIMIT} "
                f"add_addr_accepted {MPTCP_ADD_ADDR_ACCEPTED}",
                "set limits",
            ),
        ]:
            rc, output = run_checked(node, command)
            if rc != 0:
                print(f"WARNING: {host_name}: failed to {description}: {output}")

    if MPTCP_ENABLE_PRODUCER_SUBFLOW_ENDPOINTS:
        print(
            "*** Enabling producer-side MPTCP subflow endpoints "
            f"(target total subflows per connection: {MPTCP_MAX_TOTAL_SUBFLOWS})"
        )
        for producer_name, endpoint in producer_endpoints.items():
            node = net.get(producer_name)
            rc, output = run_checked(
                node,
                f"ip mptcp endpoint add {endpoint['ip']} "
                f"dev {endpoint['intf']} id 1 subflow",
            )
            if rc != 0:
                print(
                    f"WARNING: {producer_name}: failed to configure subflow endpoint: "
                    f"{output}"
                )
    else:
        print("*** Producer subflow endpoints disabled; single-subflow baseline")

    if not MPTCP_SIGNAL_CONSUMER_ENDPOINTS:
        print("*** Consumer endpoint signaling disabled; preserving loopback-only destinations")
        return

    print("*** Enabling consumer endpoint signaling across all core-facing interfaces")

    for consumer_name, endpoints in consumer_endpoints.items():
        node = net.get(consumer_name)
        for endpoint_id, endpoint in enumerate(endpoints, start=1):
            command = (
                f"ip mptcp endpoint add {endpoint['ip']} "
                f"dev {endpoint['intf']} id {endpoint_id} signal"
            )
            rc, output = run_checked(node, command)
            if rc != 0:
                print(
                    f"WARNING: {consumer_name}: failed to advertise "
                    f"{endpoint['ip']} on {endpoint['intf']}: {output}"
                )


def check_connectivity(net, primary_target, alternate_target=None):
    """Check routing to the loopback service IP and a non-shortest consumer endpoint."""
    checks = [
        ("pro0", primary_target, "service"),
        (f"pro{NUM_PRODUCERS - 1}", primary_target, "service"),
    ]
    if alternate_target:
        checks.append(("pro0", alternate_target, "alternate endpoint"))

    success = True
    for sender_name, target_ip, label in checks:
        sender = net.get(sender_name)
        print(f"Checking {sender.name} -> {target_ip} ({label})...", end=" ")
        result = sender.cmd(f"ping -c1 -W1 {target_ip}")
        if "0% packet loss" in result:
            print("OK")
        else:
            print("FAIL")
            success = False

    return success


def cleanup_processes(net):
    """Kill sender/receiver processes on all nodes."""
    print("\nCleaning up background processes...")

    for i in range(NUM_CONSUMERS):
        net.get(f"con{i}").cmd(f"pkill -f {shlex.quote(RECEIVER_APP)} 2>/dev/null")

    for i in range(NUM_PRODUCERS):
        net.get(f"pro{i}").cmd(f"pkill -f {shlex.quote(SENDER_APP)} 2>/dev/null")

    print("Done.")


def show_experiment_summary(net):
    """Print the last few lines of logs for all receivers."""
    print("\n" + "=" * 60)
    print("EXPERIMENT SUMMARY (All Receivers)")
    print("=" * 60)

    for i in range(NUM_CONSUMERS):
        c_name = f"con{i}"
        c_node = net.get(c_name)
        log_path = os.path.join(RUN_DIR, f"{c_name}.log")

        print(f"\n--- Receiver: {c_name} ---")
        if c_node.cmd(f"test -f {shlex.quote(log_path)}; echo $?").strip() != "0":
            print("   [No Log File Found]")
        else:
            log_tail = c_node.cmd(f"tail -n 3 {shlex.quote(log_path)}").strip()
            print(log_tail if log_tail else "   [Log File Empty]")

    print("\n" + "=" * 60)


LEGACY_FCT_RE = re.compile(
    r"Session #(?P<session>\d+) closed"
    r"(?: \| Remote: (?P<remote>[^|]+))?"
    r".*?\|\s*(?P<bytes>\d+)\s+bytes\b"
    r".*?FCT:\s*(?P<fct>[0-9.]+)\s*s"
)

FCT_SUMMARY_RE = re.compile(
    r"\[fct_summary\]\s+"
    r"session=(?P<session>\d+)\s+"
    r"remote=(?P<remote>\S+)\s+"
    r"expected_msgs=(?P<expected_msgs>\d+)\s+"
    r"message_size_bytes=(?P<message_size_bytes>\d+)\s+"
    r"expected_bytes=(?P<expected_bytes>\d+)\s+"
    r"received_bytes=(?P<received_bytes>\d+)\s+"
    r"fct95_target_msgs=(?P<fct95_target_msgs>\d+)\s+"
    r"fct95_target_bytes=(?P<fct95_target_bytes>\d+)\s+"
    r"fct99_target_msgs=(?P<fct99_target_msgs>\d+)\s+"
    r"fct99_target_bytes=(?P<fct99_target_bytes>\d+)\s+"
    r"fct100_target_msgs=(?P<fct100_target_msgs>\d+)\s+"
    r"fct100_target_bytes=(?P<fct100_target_bytes>\d+)\s+"
    r"fct95_s=(?P<fct95>[0-9.]+|NA)\s+"
    r"fct99_s=(?P<fct99>[0-9.]+|NA)\s+"
    r"fct100_s=(?P<fct100>[0-9.]+|NA)\s+"
    r"method=nearest_rank\s+"
    r"metric=(?P<metric>\S+)\s+"
    r"clock=monotonic\s+status=(?P<status>[a-z_]+)\s+"
    r"standard=(?P<standard>\S+)\s+"
    r"start_boundary=(?P<start_boundary>\S+)\s+"
    r"logical_unit=(?P<logical_unit>\S+)"
)


def percentile_nearest_rank(values, percentile):
    if not values:
        return None
    sorted_values = sorted(values)
    idx = max(0, math.ceil((percentile / 100.0) * len(sorted_values)) - 1)
    return sorted_values[min(idx, len(sorted_values) - 1)]


def expected_fct_targets():
    target_messages = {
        percentile: (APP_EXPECTED_MESSAGES * percentile + 99) // 100
        for percentile in (95, 99)
    }
    target_messages[100] = APP_EXPECTED_MESSAGES
    target_bytes = {
        percentile: min(
            target_messages[percentile] * APP_CHUNK_SIZE_BYTES,
            APP_TRANSFER_TOTAL_BYTES,
        )
        for percentile in (95, 99)
    }
    target_bytes[100] = APP_TRANSFER_TOTAL_BYTES
    return target_messages, target_bytes


def collect_receiver_fct_samples(run_dir=None):
    """Collect exact receiver-side FCT95/FCT99/FCT100 milestones."""
    if run_dir is None:
        run_dir = RUN_DIR

    target_messages, target_bytes = expected_fct_targets()
    samples = []
    for i in range(NUM_CONSUMERS):
        c_name = f"con{i}"
        log_path = os.path.join(run_dir, f"{c_name}.log")
        if not os.path.exists(log_path):
            continue

        with open(log_path, "r", encoding="utf-8", errors="replace") as log_file:
            for match in FCT_SUMMARY_RE.finditer(log_file.read()):
                if match.group("status") != "complete":
                    continue
                if (
                    match.group("metric") != MARS_FCT_METRIC
                    or match.group("standard") != MARS_FCT_STANDARD
                    or match.group("start_boundary") != MARS_START_BOUNDARY
                    or match.group("logical_unit") != MARS_LOGICAL_UNIT
                ):
                    continue
                geometry = {
                    "expected_msgs": APP_EXPECTED_MESSAGES,
                    "message_size_bytes": APP_CHUNK_SIZE_BYTES,
                    "expected_bytes": APP_TRANSFER_TOTAL_BYTES,
                    "received_bytes": APP_TRANSFER_TOTAL_BYTES,
                    "fct95_target_msgs": target_messages[95],
                    "fct95_target_bytes": target_bytes[95],
                    "fct99_target_msgs": target_messages[99],
                    "fct99_target_bytes": target_bytes[99],
                    "fct100_target_msgs": target_messages[100],
                    "fct100_target_bytes": target_bytes[100],
                }
                if any(
                    int(match.group(field)) != expected
                    for field, expected in geometry.items()
                ):
                    continue
                try:
                    fct95 = float(match.group("fct95"))
                    fct99 = float(match.group("fct99"))
                    fct100 = float(match.group("fct100"))
                except ValueError:
                    continue
                if not 0 <= fct95 <= fct99 <= fct100:
                    continue
                samples.append(
                    {
                        "consumer": c_name,
                        "session": int(match.group("session")),
                        "remote": match.group("remote"),
                        "fct95": fct95,
                        "fct99": fct99,
                        "fct100": fct100,
                    }
                )
    return samples


def receiver_fct_sample_counts(samples):
    return collections.Counter(sample["consumer"] for sample in samples)


def has_complete_receiver_fct_set(samples):
    """Require exactly five distinct exact samples at every consumer."""
    counts = receiver_fct_sample_counts(samples)
    session_ids = collections.defaultdict(set)
    for sample in samples:
        session_ids[sample["consumer"]].add(sample["session"])

    return (
        len(samples) == NUM_PRODUCERS
        and all(
            counts[f"con{i}"] == PRODUCERS_PER_CONSUMER
            and len(session_ids[f"con{i}"]) == PRODUCERS_PER_CONSUMER
            for i in range(NUM_CONSUMERS)
        )
    )


def consumer_fct_statistics(samples):
    """Average the five exact flow milestones within each complete consumer."""
    grouped = collections.defaultdict(list)
    for sample in samples:
        grouped[sample["consumer"]].append(sample)

    results = []
    for i in range(NUM_CONSUMERS):
        consumer = f"con{i}"
        consumer_samples = grouped[consumer]
        session_ids = {sample["session"] for sample in consumer_samples}
        if (
            len(consumer_samples) != PRODUCERS_PER_CONSUMER
            or len(session_ids) != PRODUCERS_PER_CONSUMER
        ):
            continue
        results.append(
            {
                "consumer": consumer,
                "flows": len(consumer_samples),
                "mean_fct95": statistics.fmean(
                    sample["fct95"] for sample in consumer_samples
                ),
                "mean_fct99": statistics.fmean(
                    sample["fct99"] for sample in consumer_samples
                ),
                "mean_fct100": statistics.fmean(
                    sample["fct100"] for sample in consumer_samples
                ),
                "max_flow_fct100": max(
                    sample["fct100"] for sample in consumer_samples
                ),
            }
        )
    return results


def cross_consumer_fct_statistics(consumer_stats):
    """Calculate median and max across the five consumer-level means."""
    if len(consumer_stats) != NUM_CONSUMERS:
        return None

    result = {}
    for percentile in (95, 99, 100):
        values = [item[f"mean_fct{percentile}"] for item in consumer_stats]
        result[f"median_fct{percentile}"] = statistics.median(values)
        result[f"max_fct{percentile}"] = max(values)
    return result


def cross_flow_fct_statistics(samples):
    """Calculate the standardized median and max directly across 25 flows."""
    if not has_complete_receiver_fct_set(samples):
        return None

    result = {}
    for percentile in (95, 99, 100):
        values = [sample[f"fct{percentile}"] for sample in samples]
        result[f"median_fct{percentile}"] = statistics.median(values)
        result[f"max_fct{percentile}"] = max(values)
    return result


def wait_for_receiver_fct_samples(timeout=30, poll_interval=0.25):
    """Wait for all exact receiver-side FCT95/FCT99/FCT100 records."""
    print(f"Waiting for all {NUM_PRODUCERS} exact receiver FCT samples...")
    deadline = time.time() + timeout
    samples = []

    while True:
        samples = collect_receiver_fct_samples()
        if has_complete_receiver_fct_set(samples):
            print(f"   All {NUM_PRODUCERS} receiver FCT samples are available")
            return True

        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(poll_interval, remaining))

    counts = receiver_fct_sample_counts(samples)
    count_text = ", ".join(
        f"con{i}={counts[f'con{i}']}" for i in range(NUM_CONSUMERS)
    )
    print(f"WARNING: timed out waiting for receiver FCT samples ({count_text})")
    return False


def write_run_fct_summary(run_dir=None):
    """Write exact per-flow, per-consumer, and cross-consumer summaries."""
    if run_dir is None:
        run_dir = RUN_DIR

    samples = collect_receiver_fct_samples(run_dir)
    complete = has_complete_receiver_fct_set(samples)
    counts = receiver_fct_sample_counts(samples)
    summary_path = os.path.join(run_dir, RUN_FCT_SUMMARY_FILENAME)
    sorted_samples = sorted(
        samples,
        key=lambda sample: (sample["consumer"], sample["session"]),
    )

    summary_lines = [
        "# Standardized MARS-compatible logical-chunk FCT milestones.\n",
        f"[run_summary] per_flow_fct_begin flows={len(samples)} "
        f"expected_flows={NUM_PRODUCERS} method=nearest_rank "
        f"metric={MARS_FCT_METRIC} clock=monotonic "
        f"standard={MARS_FCT_STANDARD} start_boundary={MARS_START_BOUNDARY} "
        f"logical_unit={MARS_LOGICAL_UNIT}\n",
    ]
    for sample in sorted_samples:
        summary_lines.append(
            f"[run_summary] consumer={sample['consumer']} "
            f"session={sample['session']} remote={sample['remote']} "
            f"fct95_s={sample['fct95']:.9f} "
            f"fct99_s={sample['fct99']:.9f} "
            f"fct100_s={sample['fct100']:.9f} status=complete\n"
        )

    if complete:
        consumer_stats = consumer_fct_statistics(samples)
        cross_stats = cross_consumer_fct_statistics(consumer_stats)
        cross_flow_stats = cross_flow_fct_statistics(samples)
        if cross_stats is None or cross_flow_stats is None:
            raise RuntimeError("complete flow set did not produce complete aggregates")

        summary_lines.append(
            f"[run_summary] per_consumer_fct_begin consumers={len(consumer_stats)} "
            f"expected_consumers={NUM_CONSUMERS}\n"
        )
        for item in consumer_stats:
            summary_lines.append(
                f"[consumer_summary] consumer={item['consumer']} "
                f"mean_fct95_s={item['mean_fct95']:.9f} "
                f"mean_fct99_s={item['mean_fct99']:.9f} "
                f"mean_fct100_s={item['mean_fct100']:.9f} "
                f"max_flow_fct100_s={item['max_flow_fct100']:.9f} "
                f"flows={item['flows']} aggregation=arithmetic_mean "
                "status=complete\n"
            )
        summary_lines.append(
            f"[cross_consumer] median_fct95_s={cross_stats['median_fct95']:.9f} "
            f"max_fct95_s={cross_stats['max_fct95']:.9f} "
            f"median_fct99_s={cross_stats['median_fct99']:.9f} "
            f"max_fct99_s={cross_stats['max_fct99']:.9f} "
            f"median_fct100_s={cross_stats['median_fct100']:.9f} "
            f"max_fct100_s={cross_stats['max_fct100']:.9f} "
            f"consumers={len(consumer_stats)} "
            "aggregation=consumer_flow_means status=complete\n"
        )
        summary_lines.append(
            f"[cross_flow] median_flow_fct95_s={cross_flow_stats['median_fct95']:.9f} "
            f"max_flow_fct95_s={cross_flow_stats['max_fct95']:.9f} "
            f"median_flow_fct99_s={cross_flow_stats['median_fct99']:.9f} "
            f"max_flow_fct99_s={cross_flow_stats['max_fct99']:.9f} "
            f"median_flow_fct100_s={cross_flow_stats['median_fct100']:.9f} "
            f"max_flow_fct100_s={cross_flow_stats['max_fct100']:.9f} "
            f"flows={len(samples)} aggregation=all_flows "
            f"standard={MARS_FCT_STANDARD} status=complete\n"
        )

        fct100_values = [sample["fct100"] for sample in samples]
        rank = math.ceil(0.95 * len(fct100_values))
        p95_fct = percentile_nearest_rank(fct100_values, 95)
        # Legacy nearest-rank p95(FCT100) log output is intentionally disabled.
        # summary_lines.append(
        #     f"[run_summary] all_flows_p95_fct_s={p95_fct:.9f} "
        #     f"all_flows_max_fct_s={max(fct100_values):.9f} "
        #     f"flows={len(fct100_values)} "
        #     f"expected_flows={NUM_PRODUCERS} rank={rank} percentile=95 "
        #     "method=nearest_rank metric=fct100 status=complete\n"
        # )
        print(
            "Standardized 25-flow FCT: "
            f"median95={cross_flow_stats['median_fct95']:.4f}s, "
            f"median99={cross_flow_stats['median_fct99']:.4f}s, "
            f"median100={cross_flow_stats['median_fct100']:.4f}s"
        )
        print(
            "Compatibility consumer-mean FCT: "
            f"median95={cross_stats['median_fct95']:.4f}s, "
            f"median99={cross_stats['median_fct99']:.4f}s, "
            f"median100={cross_stats['median_fct100']:.4f}s"
        )
    else:
        count_text = ",".join(
            f"con{i}:{counts[f'con{i}']}" for i in range(NUM_CONSUMERS)
        )
        # Legacy nearest-rank p95(FCT100) log output is intentionally disabled.
        # summary_lines.append(
        #     f"[run_summary] all_flows_p95_fct_s=NA flows={len(samples)} "
        #     f"expected_flows={NUM_PRODUCERS} percentile=95 "
        #     f"method=nearest_rank metric=fct100 status=incomplete "
        #     f"consumer_counts={count_text}\n"
        # )
        summary_lines.append(
            "[cross_consumer] median_fct95_s=NA max_fct95_s=NA "
            "median_fct99_s=NA max_fct99_s=NA median_fct100_s=NA "
            f"max_fct100_s=NA consumers=0 expected_consumers={NUM_CONSUMERS} "
            "aggregation=consumer_flow_means status=incomplete\n"
        )
        summary_lines.append(
            "[cross_flow] median_flow_fct95_s=NA max_flow_fct95_s=NA "
            "median_flow_fct99_s=NA max_flow_fct99_s=NA "
            "median_flow_fct100_s=NA max_flow_fct100_s=NA "
            f"flows={len(samples)} expected_flows={NUM_PRODUCERS} "
            f"aggregation=all_flows standard={MARS_FCT_STANDARD} "
            "status=incomplete\n"
        )
        print(
            f"WARNING: exact standardized FCT aggregate is unavailable: found "
            f"{len(samples)}/{NUM_PRODUCERS} exact complete-flow samples"
        )

    with open(summary_path, "w", encoding="utf-8") as summary_file:
        summary_file.writelines(summary_lines)
    print(f"   Run FCT summary: {summary_path}")
    return summary_path


def append_fct_summaries(run_dir=None):
    """Append legacy and exact per-consumer summaries to receiver logs."""
    if run_dir is None:
        run_dir = RUN_DIR

    print("Appending legacy and exact FCT summaries to receiver logs...")
    exact_samples = collect_receiver_fct_samples(run_dir)
    for i in range(NUM_CONSUMERS):
        c_name = f"con{i}"
        log_path = os.path.join(run_dir, f"{c_name}.log")
        if not os.path.exists(log_path):
            print(f"   WARNING: {c_name}: no log found")
            continue

        with open(log_path, "r", encoding="utf-8", errors="replace") as log_file:
            sessions = []
            for match in LEGACY_FCT_RE.finditer(log_file.read()):
                transferred_bytes = int(match.group("bytes"))
                if transferred_bytes != APP_TRANSFER_TOTAL_BYTES:
                    continue
                sessions.append(
                    {
                        "session": int(match.group("session")),
                        "remote": (match.group("remote") or "unknown").strip(),
                        "fct": float(match.group("fct")),
                    }
                )

        if not sessions:
            summary = (
                f"[summary] {c_name} per_session_fct_begin flows=0\n"
                # Legacy per-consumer p95(FCT100) log output is disabled.
                # f"[summary] {c_name} overall_p95_fct_s=NA flows=0 "
                # "method=nearest_rank\n"
            )
            print(f"   WARNING: {c_name}: no FCT samples")
        else:
            fcts = [item["fct"] for item in sessions]
            rank = math.ceil(0.95 * len(fcts))
            overall_p95 = percentile_nearest_rank(fcts, 95)
            summary_lines = [
                f"[summary] {c_name} per_session_fct_begin flows={len(sessions)}\n"
            ]
            for item in sorted(sessions, key=lambda value: value["session"]):
                summary_lines.append(
                    f"[summary] {c_name} session={item['session']} remote={item['remote']} "
                    f"fct_s={item['fct']:.4f} samples=1\n"
                )

            # Legacy per-consumer p95(FCT100) log output is disabled.
            # summary_lines.append(
            #     f"[summary] {c_name} overall_p95_fct_s={overall_p95:.4f} "
            #     f"overall_max_fct_s={max(fcts):.4f} flows={len(fcts)} "
            #     f"rank={rank} percentile=95 method=nearest_rank\n"
            # )
            summary = "".join(summary_lines)
            # print(
            #     f"   {c_name}: p95 FCT {overall_p95:.4f}s "
            #     f"across {len(sessions)} flows"
            # )

        consumer_exact_samples = [
            sample for sample in exact_samples if sample["consumer"] == c_name
        ]
        stats = consumer_fct_statistics(consumer_exact_samples)
        if len(stats) != 1:
            exact_summary = (
                f"[summary] {c_name} mean_fct95_s=NA mean_fct99_s=NA "
                "mean_fct100_s=NA max_flow_fct100_s=NA "
                f"flows={len(consumer_exact_samples)} "
                f"expected_flows={PRODUCERS_PER_CONSUMER} "
                "aggregation=arithmetic_mean status=incomplete\n"
            )
            print(
                f"   WARNING: {c_name}: found "
                f"{len(consumer_exact_samples)} exact FCT samples"
            )
        else:
            item = stats[0]
            exact_summary_lines = [
                f"[summary] {c_name} exact_per_flow_fct_begin "
                f"flows={len(consumer_exact_samples)}\n"
            ]
            for sample in sorted(
                consumer_exact_samples,
                key=lambda value: value["session"],
            ):
                exact_summary_lines.append(
                    f"[summary] {c_name} session={sample['session']} "
                    f"remote={sample['remote']} "
                    f"fct95_s={sample['fct95']:.9f} "
                    f"fct99_s={sample['fct99']:.9f} "
                    f"fct100_s={sample['fct100']:.9f} status=complete\n"
                )
            exact_summary_lines.append(
                f"[summary] {c_name} mean_fct95_s={item['mean_fct95']:.9f} "
                f"mean_fct99_s={item['mean_fct99']:.9f} "
                f"mean_fct100_s={item['mean_fct100']:.9f} "
                f"max_flow_fct100_s={item['max_flow_fct100']:.9f} "
                f"flows={item['flows']} aggregation=arithmetic_mean "
                "status=complete\n"
            )
            exact_summary = "".join(exact_summary_lines)
            print(
                f"   {c_name}: mean exact FCT95/FCT99/FCT100 = "
                f"{item['mean_fct95']:.4f}/"
                f"{item['mean_fct99']:.4f}/"
                f"{item['mean_fct100']:.4f}s"
            )

        with open(log_path, "a", encoding="utf-8") as log_file:
            log_file.write(summary)
            log_file.write(exact_summary)


def wait_for_sender_completion(net, sender_done_paths, timeout=900, poll_interval=1):
    """Wait until every sender writes its done marker."""
    print(f"Waiting for all {len(sender_done_paths)} sender flows to finish...")
    pending = dict(sender_done_paths)
    failed = {}
    start_time = time.time()
    last_report = start_time

    while pending:
        finished = []
        for s_name, done_path in pending.items():
            s_node = net.get(s_name)
            if s_node.cmd(f"test -f {shlex.quote(done_path)}; echo $?").strip() == "0":
                exit_code = s_node.cmd(f"cat {shlex.quote(done_path)}").strip()
                print(f"   {s_name} finished with exit code {exit_code}")
                if exit_code != "0":
                    failed[s_name] = exit_code
                finished.append(s_name)

        for s_name in finished:
            pending.pop(s_name, None)

        if not pending:
            if failed:
                print(
                    "Sender failures: "
                    + ", ".join(f"{name}={code}" for name, code in sorted(failed.items()))
                )
                return False
            return True

        now = time.time()
        if now - start_time > timeout:
            print(f"Timeout waiting for senders: {', '.join(sorted(pending))}")
            return False

        if now - last_report >= 10:
            print(f"   ... {len(pending)} sender flows still running")
            last_report = now

        time.sleep(poll_interval)


def run_many_to_few_experiment(net, con_ips, pro_ips, producer_endpoints):
    """
    Automatically starts 5 receivers and 25 senders with staggered starts.
    Mapping:
      con0 <- pro0..4
      con1 <- pro5..9
      con2 <- pro10..14
      con3 <- pro15..19
      con4 <- pro20..24
    """
    print("\n" + "=" * 60)
    print(f"MANY-TO-FEW EXPERIMENT ({NUM_PRODUCERS} Senders -> {NUM_CONSUMERS} Receivers)")
    print("=" * 60)

    for i in range(NUM_CONSUMERS):
        c_name = f"con{i}"
        c_node = net.get(c_name)
        c_ip = con_ips[c_name]
        c_log = os.path.join(RUN_DIR, f"{c_name}.log")
        c_csv = os.path.join(RUN_DIR, f"{c_name}_throughput.csv")
        print(f"Starting receiver on {c_name} ({c_ip})...")
        c_node.cmd(
            f"{shlex.quote(PYTHON_BIN)} {shlex.quote(RECEIVER_APP)} "
            f"--port={RECEIVER_PORT} --multipath=true "
            f"--sndbuf={STATIC_SOCKET_SNDBUF_BYTES} "
            f"--rcvbuf={STATIC_SOCKET_RCVBUF_BYTES} "
            f"--expected-messages={APP_EXPECTED_MESSAGES} "
            f"--expected-bytes={APP_TRANSFER_TOTAL_BYTES} "
            f"--message-size-bytes={APP_CHUNK_SIZE_BYTES} "
            "--expect-start-preamble=true "
            f"--csv={shlex.quote(c_csv)} --csv-intervals=50,100 > "
            f"{shlex.quote(c_log)} 2>&1 &"
        )

    time.sleep(2)

    print("Starting senders with staggered consumer-group delays...")
    group_release_base = time.time() + GROUP_RELEASE_LEAD_SECONDS
    print(
        f"   Using a {GROUP_RELEASE_LEAD_SECONDS:.2f}s launch lead so each consumer group "
        "can release its 5 senders together."
    )

    sender_done_paths = {}
    grouped_senders = collections.defaultdict(list)
    for i in range(NUM_PRODUCERS):
        grouped_senders[i // PRODUCERS_PER_CONSUMER].append(i)

    group_schedule = []
    for target_idx, sender_indices in grouped_senders.items():
        target_con_name = f"con{target_idx}"
        delay = CONSUMER_START_DELAYS.get(target_con_name, 0)
        group_schedule.append((delay, target_idx, sorted(sender_indices)))

    for delay, target_idx, sender_indices in sorted(group_schedule):
        target_con_name = f"con{target_idx}"
        target_ip = con_ips[target_con_name]
        release_time = group_release_base + delay
        print(
            f"   Scheduling {len(sender_indices)} senders for {target_con_name} "
            f"at +{delay:.2f}s"
        )

        for i in sender_indices:
            s_name = f"pro{i}"
            s_node = net.get(s_name)
            if MPTCP_BIND_SENDER_TO_DATA_ENDPOINT:
                s_bind_ip = producer_endpoints[s_name]["ip"]
            else:
                s_bind_ip = pro_ips[s_name]

            print(f"      {s_name} ({s_bind_ip}) -> {target_con_name} ({target_ip})")
            s_log = os.path.join(RUN_DIR, f"{s_name}.log")
            done_path = os.path.join(RUN_DIR, f"{s_name}.done")
            s_node.cmd(f"rm -f {shlex.quote(done_path)}")

            # Standardized cross-solution workload:
            # 18,240,512 bytes = 3032 complete 6016-byte logical chunks.
            sender_cmd = (
                f"{shlex.quote(PYTHON_BIN)} {shlex.quote(SENDER_APP)} "
                f"--addr={target_ip}:{RECEIVER_PORT} --bind={s_bind_ip} "
                f"--name={shlex.quote(s_name)} "
                f"--sndbuf={STATIC_SOCKET_SNDBUF_BYTES} "
                f"--rcvbuf={STATIC_SOCKET_RCVBUF_BYTES} "
                f"--size={APP_CHUNK_SIZE_BYTES} --total={APP_TRANSFER_TOTAL_BYTES} "
                f"--pipeline={APP_PIPELINE_DEPTH} "
                "--send-start-preamble=true "
                f"> {shlex.quote(s_log)} 2>&1"
            )
            wrapped_cmd = build_delayed_background_command(
                PYTHON_BIN,
                sender_cmd,
                done_path,
                release_time,
            )

            s_node.cmd(wrapped_cmd)
            sender_done_paths[s_name] = done_path

    all_finished = wait_for_sender_completion(net, sender_done_paths)
    wait_for_receiver_fct_samples(timeout=30 if all_finished else 2)
    append_fct_summaries()
    write_run_fct_summary()
    show_experiment_summary(net)

    if all_finished:
        print("Experiment complete. All sender flows finished; exiting Mininet.")
    else:
        print("Experiment ended before every sender flow finished. Logs saved to results.")


def create_wan_topology(core_link_mode):
    print(
        f"Starting L3 ECMP topology "
        f"({NUM_PRODUCERS} producers, {NUM_CONSUMERS} consumers)"
    )
    print(f"Configured Loss (Consumer->Core): {CON_TO_CORE_LOSS}%")
    print(f"Core-link mode: {core_link_mode}")
    if core_link_mode == "heterogeneous":
        print(
            "Heterogeneous core-core bandwidth pool (Mbps): "
            f"{HETEROGENEOUS_CORE_LINK_BWS_MBPS}"
        )
    print(f"Core mesh links: {describe_core_mesh(LINKS)}")
    print(
        "MPTCP transport: "
        f"subflow_mode={MPTCP_SUBFLOW_MODE}, "
        f"mode={MPTCP_EXPERIMENT_MODE}, "
        f"max_total_subflows={MPTCP_MAX_TOTAL_SUBFLOWS}, "
        f"additional_limit={MPTCP_ADDITIONAL_SUBFLOW_LIMIT}, "
        f"add_addr_accepted={MPTCP_ADD_ADDR_ACCEPTED}, "
        f"producer_subflow_endpoints={'on' if MPTCP_ENABLE_PRODUCER_SUBFLOW_ENDPOINTS else 'off'}, "
        f"consumer_signal_endpoints={'on' if MPTCP_SIGNAL_CONSUMER_ENDPOINTS else 'off'}, "
        f"sender_bind={'data_endpoint' if MPTCP_BIND_SENDER_TO_DATA_ENDPOINT else 'loopback'}, "
        f"static_buffers={'on' if USE_STATIC_SOCKET_BUFFERS else 'off'}, "
        f"static_sndbuf={STATIC_SOCKET_SNDBUF_BYTES}, "
        f"static_rcvbuf={STATIC_SOCKET_RCVBUF_BYTES}, "
        f"autotune_max_sndbuf={AUTOTUNE_MAX_SNDBUF_BYTES}, "
        f"autotune_max_rcvbuf={AUTOTUNE_MAX_RCVBUF_BYTES}"
    )
    print("Pre-calculating routes...")

    engine = RoutingEngine(NODES, LINKS)
    producer_endpoints, consumer_endpoints = build_host_data_endpoints(engine)

    net = Mininet(topo=None, build=False, link=TCLink)

    print("*** Adding nodes")
    nodes_obj = {}
    for name in NODES:
        if name.startswith("pro") or name.startswith("con"):
            nodes_obj[name] = net.addHost(name, ip=None)
        else:
            nodes_obj[name] = net.addHost(name, cls=LinuxRouter, ip=None)

    print("*** Adding links and configuring interfaces")
    for src_name, dst_name, bw, delay, queue, loss in LINKS:
        src = nodes_obj[src_name]
        dst = nodes_obj[dst_name]

        net.addLink(
            src,
            dst,
            bw=bw,
            delay=delay,
            max_queue_size=queue,
            loss=loss,
            intfName1=f"{src_name}-{dst_name}",
            intfName2=f"{dst_name}-{src_name}",
        )

        src_ip = engine.get_link_ip(src_name, dst_name)
        dst_ip = engine.get_link_ip(dst_name, src_name)

        src.setIP(src_ip, prefixLen=30, intf=f"{src_name}-{dst_name}")
        dst.setIP(dst_ip, prefixLen=30, intf=f"{dst_name}-{src_name}")

    print("*** Starting network")
    net.start()

    print("*** Configuring loopback service IPs")
    con_ips = {}
    pro_ips = {}

    for i in range(NUM_CONSUMERS):
        name = f"con{i}"
        ip = f"10.0.0.{20 + i}"
        nodes_obj[name].cmd(f"ip addr add {ip}/32 dev lo")
        con_ips[name] = ip

    for i in range(NUM_PRODUCERS):
        name = f"pro{i}"
        ip = f"10.0.0.{100 + i}"
        nodes_obj[name].cmd(f"ip addr add {ip}/32 dev lo")
        pro_ips[name] = ip

    route_targets = build_route_targets(
        con_ips,
        pro_ips,
        producer_endpoints,
        consumer_endpoints,
        include_producer_endpoints=MPTCP_ROUTE_PRODUCER_ENDPOINTS,
        include_consumer_endpoints=MPTCP_ROUTE_CONSUMER_ENDPOINTS,
    )
    apply_computed_routes(nodes_obj, engine, route_targets)
    configure_network_settings(net)
    configure_mptcp_endpoints(net, producer_endpoints, consumer_endpoints)

    alternate_target = None
    if MPTCP_ROUTE_CONSUMER_ENDPOINTS:
        alternate_target = consumer_endpoints["con0"][-1]["ip"]
    print("*** Checking connectivity")
    success = check_connectivity(net, con_ips["con0"], alternate_target)

    try:
        if success:
            print("Network is ready.")
            run_many_to_few_experiment(net, con_ips, pro_ips, producer_endpoints)
        else:
            print("Network failed validation.")
    finally:
        cleanup_processes(net)
        net.stop()


if __name__ == "__main__":
    args = parse_args()
    CON_TO_CORE_LOSS = args.con_to_core_loss
    LINKS = build_links(args.core_link_mode)
    RUN_DIR = run_dir_for_mode(args.core_link_mode)
    setLogLevel("info")
    try:
        require_cubic_congestion_control()
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2) from error
    if not ensure_binaries_exist():
        raise SystemExit(1)
    prepare_run_dir()
    subprocess.run(
        ["mn", "-c"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    create_wan_topology(args.core_link_mode)

#!/usr/bin/env python3

from mininet.net import Mininet
from mininet.node import Node, Host
from mininet.link import TCLink
from mininet.log import setLogLevel, info
import time
import collections
import math
import os
import re
import shlex
import statistics

# --- Configuration ---
NUM_CONSUMERS = 5
NUM_PRODUCERS = 25
NUM_CORES = 5
PRODUCERS_PER_CONSUMER = NUM_PRODUCERS // NUM_CONSUMERS
APP_CHUNK_SIZE_BYTES = 6006
APP_TRANSFER_TOTAL_BYTES = 18240512
APP_EXPECTED_MESSAGES = (
    APP_TRANSFER_TOTAL_BYTES + APP_CHUNK_SIZE_BYTES - 1
) // APP_CHUNK_SIZE_BYTES

# Loss rate in percentage (0 to 100) for Consumer->Core links
try:
    CON_TO_CORE_LOSS = float(os.environ.get("MPQUIC_CON_TO_CORE_LOSS", "1"))
except ValueError as error:
    raise ValueError("MPQUIC_CON_TO_CORE_LOSS must be a number from 0 to 100") from error
if not math.isfinite(CON_TO_CORE_LOSS) or not 0 <= CON_TO_CORE_LOSS <= 100:
    raise ValueError("MPQUIC_CON_TO_CORE_LOSS must be a number from 0 to 100")
if CON_TO_CORE_LOSS.is_integer():
    CON_TO_CORE_LOSS = int(CON_TO_CORE_LOSS)
PRO_TO_EDGE_LOSS = 0

# Core-mesh bandwidth mode:
# - "homogeneous": all core-core links stay at 40 Mbps.
# - "heterogeneous": only core-core links vary, cycling through the configured
#   bandwidth list in sorted core-pair order.
CORE_MESH_MODE = "heterogeneous"
HETEROGENEOUS_CORE_LINK_BWS_MBPS = [25, 40, 50, 60, 80]
ADDITIONAL_PATH_CONGESTION_CONTROL = "olia"

# Start delay for each consumer group (in seconds)
# This dictates when the senders targeting a specific consumer will start.
CONSUMER_START_DELAY_PATTERN = [0, 1, 2, 1, 2]
CONSUMER_START_DELAYS = {
    f'con{i}': CONSUMER_START_DELAY_PATTERN[i] for i in range(NUM_CONSUMERS)
}

ROUTE_INSTALL_MODE = os.environ.get(
    "MPQUIC_ROUTE_INSTALL_MODE", "ecmp"
).strip().lower()
VALID_ROUTE_INSTALL_MODES = {"ecmp", "consumer_cores"}
if ROUTE_INSTALL_MODE not in VALID_ROUTE_INSTALL_MODES:
    raise ValueError(
        f"Unsupported ROUTE_INSTALL_MODE {ROUTE_INSTALL_MODE!r}; "
        f"expected one of {sorted(VALID_ROUTE_INSTALL_MODES)}"
    )

VALID_CORE_MESH_MODES = {"homogeneous", "heterogeneous"}
if CORE_MESH_MODE not in VALID_CORE_MESH_MODES:
    raise ValueError(
        f"Unsupported CORE_MESH_MODE {CORE_MESH_MODE!r}; "
        f"expected one of {sorted(VALID_CORE_MESH_MODES)}"
    )
if not HETEROGENEOUS_CORE_LINK_BWS_MBPS:
    raise ValueError("HETEROGENEOUS_CORE_LINK_BWS_MBPS must not be empty")

def mpquic_env_prefix():
    parts = []
    if ROUTE_INSTALL_MODE == "consumer_cores":
        parts.extend(["MPQUIC_MININET_MULTIPATH=1", "MPQUIC_TRACE_PATHS=1"])
    if parts:
        return " ".join(parts) + " "
    return ""


def mpquic_sender_extra_args():
    if ROUTE_INSTALL_MODE == "consumer_cores":
        return "-force-multipath=true "
    return ""


def sender_pipeline_depth():
    if ROUTE_INSTALL_MODE == "consumer_cores":
        return 24
    return 8


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_DIR = os.path.dirname(SCRIPT_DIR)
RECEIVER_BIN = os.path.join(EXAMPLE_DIR, 'receiver')
SENDER_BIN = os.path.join(EXAMPLE_DIR, 'sender')


def resolve_results_root():
    preferred = os.path.join(EXAMPLE_DIR, 'results')
    fallback = os.path.join(EXAMPLE_DIR, 'results_local')

    for candidate in (preferred, fallback):
        try:
            os.makedirs(candidate, exist_ok=True)
        except OSError:
            continue
        if os.access(candidate, os.W_OK | os.X_OK):
            return candidate

    raise RuntimeError("No writable results directory available")


RESULTS_ROOT = resolve_results_root()
RESULTS_RUN_ID = os.environ.get("MPQUIC_RESULTS_RUN_ID", "").strip()
if RESULTS_RUN_ID and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", RESULTS_RUN_ID):
    raise ValueError(
        "MPQUIC_RESULTS_RUN_ID must be 1-64 characters using only letters, "
        "digits, '.', '_', or '-'"
    )

def loss_label(loss):
    return str(loss).replace('.', 'p')

RUN_PARENT = RESULTS_ROOT
if RESULTS_RUN_ID:
    RUN_PARENT = os.path.join(RESULTS_ROOT, 'reproductions', RESULTS_RUN_ID)
RUN_DIR = os.path.join(RUN_PARENT, f'con_core_loss_{loss_label(CON_TO_CORE_LOSS)}pct')
RUN_FCT_SUMMARY_PATH = os.path.join(RUN_DIR, 'run_fct_summary.log')

def prepare_run_dir():
    """Create and clear the output directory for the active loss configuration."""
    if RESULTS_RUN_ID:
        os.makedirs(RUN_PARENT, exist_ok=True)
        try:
            os.mkdir(RUN_DIR)
        except FileExistsError as error:
            raise RuntimeError(
                f"Refusing to reuse isolated run directory: {RUN_DIR}"
            ) from error
    else:
        os.makedirs(RUN_DIR, exist_ok=True)
    os.chmod(RESULTS_ROOT, 0o777)
    os.chmod(RUN_DIR, 0o777)
    if not RESULTS_RUN_ID:
        for name in os.listdir(RUN_DIR):
            if name.endswith(('.log', '.csv', '.done')):
                os.remove(os.path.join(RUN_DIR, name))

    print(f"🗂️  Writing run artifacts to {RUN_DIR}")

def ensure_binaries_exist():
    """Validate sender/receiver binaries before building topology."""
    missing = []
    for binary in [RECEIVER_BIN, SENDER_BIN]:
        if not os.path.isfile(binary):
            missing.append(f"missing file: {binary}")
        elif not os.access(binary, os.X_OK):
            missing.append(f"not executable: {binary}")

    if missing:
        print("❌ Required binaries are unavailable:")
        for item in missing:
            print(f"   - {item}")
        print("   Build binaries first with:")
        print(
            f"   cd {EXAMPLE_DIR} && "
            "go build -o receiver receiver.go && "
            "go build -o sender sender.go"
        )
        return False
    return True

# --- Topology Definition ---
PRODUCERS = [f'pro{i}' for i in range(NUM_PRODUCERS)]
CONSUMERS = [f'con{i}' for i in range(NUM_CONSUMERS)]
EDGES = [f'edge{i}' for i in range(NUM_PRODUCERS)]
CORES = [f'core{i}' for i in range(NUM_CORES)]

NODES = PRODUCERS + CONSUMERS + EDGES + CORES

def build_links():
    """Create the fixed 25-producer/5-consumer WAN topology."""
    links = []

    # Producers to dedicated edges.
    for i in range(NUM_PRODUCERS):
        links.append((f'pro{i}', f'edge{i}', 40, '5ms', 1000, PRO_TO_EDGE_LOSS))

    # Groups of five edges attach to one core.
    for i in range(NUM_PRODUCERS):
        core_idx = i // PRODUCERS_PER_CONSUMER
        links.append((f'edge{i}', f'core{core_idx}', 40, '5ms', 1000, 0))

    # Every consumer connects to every core.
    for c in range(NUM_CONSUMERS):
        for k in range(NUM_CORES):
            links.append((f'con{c}', f'core{k}', 40, '5ms', 1000, CON_TO_CORE_LOSS))

    # Full mesh among cores. In heterogeneous mode, only these links vary.
    core_mesh_pair_index = 0
    for i in range(NUM_CORES):
        for j in range(i + 1, NUM_CORES):
            core_bw = 40
            if CORE_MESH_MODE == "heterogeneous":
                core_bw = HETEROGENEOUS_CORE_LINK_BWS_MBPS[
                    core_mesh_pair_index % len(HETEROGENEOUS_CORE_LINK_BWS_MBPS)
                ]
            links.append((f'core{i}', f'core{j}', core_bw, '1ms', 1000, 0))
            core_mesh_pair_index += 1

    return links

LINKS = build_links()


# --- 1. Simulated Routing Engine ---
class RoutingEngine:
    """
    Calculates Shortest Path(s) and assigns subnets.
    Supports ECMP (Equal-Cost Multi-Path).
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

            # Build Graph
            self.adj[u].append(v)
            self.adj[v].append(u)

            # Assign /30 Subnet (172.16.X.Y)
            # Subnet 0: 172.16.0.0/30 -> .1, .2
            base_ip = subnet_counter * 4
            octet3 = base_ip // 256
            octet4 = base_ip % 256

            ip_u = f"172.16.{octet3}.{octet4 + 1}"
            ip_v = f"172.16.{octet3}.{octet4 + 2}"

            self.link_subnets[(u, v)] = {'u_ip': ip_u, 'v_ip': ip_v}
            self.link_subnets[(v, u)] = {'u_ip': ip_v, 'v_ip': ip_u}
            subnet_counter += 1

    def get_link_ip(self, node, neighbor):
        if (node, neighbor) in self.link_subnets:
            return self.link_subnets[(node, neighbor)]['u_ip']
        return None

    def calculate_ecmp_routes(self, src, dst):
        """BFS to find all next-hops on shortest paths."""
        if src == dst: return []

        queue = collections.deque([(src, 0)])
        visited = {src: 0}
        parents = collections.defaultdict(set)

        while queue:
            curr, dist = queue.popleft()
            if curr == dst: continue

            for neighbor in self.adj[curr]:
                new_dist = dist + 1
                if neighbor not in visited:
                    visited[neighbor] = new_dist
                    parents[neighbor].add(curr)
                    queue.append((neighbor, new_dist))
                elif visited[neighbor] == new_dist:
                    parents[neighbor].add(curr)

        if dst not in parents: return []

        # Backtrack from dst to src to find valid next hops from src
        valid_next_hops = set()
        def backtrack(node):
            if node == src: return
            for parent in parents[node]:
                if parent == src: valid_next_hops.add(node)
                backtrack(parent)

        backtrack(dst)
        return list(valid_next_hops)


def build_host_data_endpoints(engine):
    producer_endpoints = {}
    for i in range(NUM_PRODUCERS):
        name = f'pro{i}'
        edge = f'edge{i}'
        producer_endpoints[name] = {
            'ip': engine.get_link_ip(name, edge),
            'attachment': edge,
            'intf': f'{name}-{edge}',
        }

    consumer_endpoints = {}
    for i in range(NUM_CONSUMERS):
        name = f'con{i}'
        endpoints = []
        for k in range(NUM_CORES):
            core = f'core{k}'
            endpoints.append(
                {
                    'ip': engine.get_link_ip(name, core),
                    'attachment': core,
                    'intf': f'{name}-{core}',
                }
            )
        consumer_endpoints[name] = endpoints
    return producer_endpoints, consumer_endpoints


def build_route_targets(con_ips, pro_ips, producer_endpoints, consumer_endpoints):
    targets = []

    for name, ip in con_ips.items():
        targets.append({'label': name, 'ip': ip, 'owner': name})

    for name, ip in pro_ips.items():
        targets.append({'label': name, 'ip': ip, 'owner': name})

    if ROUTE_INSTALL_MODE == "consumer_cores":
        for name, endpoint in producer_endpoints.items():
            targets.append(
                {
                    'label': f'{name}-data',
                    'ip': endpoint['ip'],
                    'owner': name,
                    'attachment': endpoint['attachment'],
                }
            )
        for name, endpoints in consumer_endpoints.items():
            for index, endpoint in enumerate(endpoints):
                targets.append(
                    {
                        'label': f'{name}-path{index}',
                        'ip': endpoint['ip'],
                        'owner': name,
                        'attachment': endpoint['attachment'],
                    }
                )

    return targets


def calculate_target_next_hops(engine, current_node_name, target):
    owner = target['owner']
    attachment = target.get('attachment')

    if current_node_name == owner:
        return []

    if not attachment:
        return engine.calculate_ecmp_routes(current_node_name, owner)

    if current_node_name == attachment:
        return [owner]

    return engine.calculate_ecmp_routes(current_node_name, attachment)


def apply_computed_routes(nodes_obj, engine, route_targets):
    print(f"*** Applying Computed Routes (mode={ROUTE_INSTALL_MODE})")

    for current_node_name in NODES:
        current_node = nodes_obj[current_node_name]

        for target in route_targets:
            if current_node_name == target['owner']:
                continue

            next_hops = calculate_target_next_hops(engine, current_node_name, target)
            if not next_hops:
                continue

            cmd = f"ip route add {target['ip']}/32 "
            if len(next_hops) == 1:
                nh = next_hops[0]
                gw = engine.get_link_ip(nh, current_node_name)
                cmd += f"via {gw}"
            else:
                for nh in next_hops:
                    gw = engine.get_link_ip(nh, current_node_name)
                    cmd += f"nexthop via {gw} weight 1 "

            current_node.cmd(cmd)

# --- 2. Mininet Node Classes ---
class LinuxRouter(Node):
    """A Node with IP forwarding enabled."""
    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        self.cmd('sysctl -w net.ipv4.ip_forward=1')

def create_wan_topology():
    print(f"🚀 Starting L3 Topology ({NUM_PRODUCERS} Senders, {NUM_CONSUMERS} Receivers)")
    print(f"📉 Configured Loss (Consumer->Core): {CON_TO_CORE_LOSS}%")
    print(f"🕸️  Core Mesh Mode: {CORE_MESH_MODE}")
    if CORE_MESH_MODE == "heterogeneous":
        print(f"   Core-Core BWs (cycled): {HETEROGENEOUS_CORE_LINK_BWS_MBPS} Mbps")
    print(f"🛣️  Route Installation Mode: {ROUTE_INSTALL_MODE}")
    print(f"🚦 MPQUIC Congestion Control: {ADDITIONAL_PATH_CONGESTION_CONTROL}")
    print("🧮 Pre-calculating Routes...")

    engine = RoutingEngine(NODES, LINKS)
    producer_endpoints, consumer_endpoints = build_host_data_endpoints(engine)

    net = Mininet(topo=None, build=False, link=TCLink)

    print("*** Adding Nodes")
    nodes_obj = {}

    # Create Nodes
    for n in NODES:
        if n.startswith('pro') or n.startswith('con'):
            # End Hosts (Senders/Receiver)
            nodes_obj[n] = net.addHost(n, ip=None)
        else:
            # Routers (Edge/Core)
            nodes_obj[n] = net.addHost(n, cls=LinuxRouter, ip=None)

    print("*** Adding Links & Configuring Interfaces")
    for src_name, dst_name, bw, delay, queue, loss in LINKS:
        src = nodes_obj[src_name]
        dst = nodes_obj[dst_name]

        # Create link with predictable interface names and LOSS parameter
        net.addLink(src, dst, bw=bw, delay=delay, max_queue_size=queue, loss=loss,
                    intfName1=f"{src_name}-{dst_name}",
                    intfName2=f"{dst_name}-{src_name}")

        # Assign IP to the specific interfaces based on Engine calculation
        src_ip = engine.get_link_ip(src_name, dst_name)
        dst_ip = engine.get_link_ip(dst_name, src_name)

        src.setIP(src_ip, prefixLen=30, intf=f"{src_name}-{dst_name}")
        dst.setIP(dst_ip, prefixLen=30, intf=f"{dst_name}-{src_name}")

    print("*** Starting network")
    net.start()

    # --- 3. Configure Loopbacks (Stable Destination IPs) ---
    print("*** Configuring Loopback IPs (Targets)")

    # Receiver IPs: 10.0.0.20 - 10.0.0.24 (con0 - con4)
    con_ips = {}
    for i in range(NUM_CONSUMERS):
        name = f'con{i}'
        ip = f'10.0.0.{20+i}'
        nodes_obj[name].cmd(f"ip addr add {ip}/32 dev lo")
        con_ips[name] = ip

    # Sender IPs: 10.0.0.100 - 10.0.0.124 (pro0 - pro24)
    pro_ips = {}
    for i in range(NUM_PRODUCERS):
        name = f'pro{i}'
        ip = f'10.0.0.{100+i}'
        nodes_obj[name].cmd(f"ip addr add {ip}/32 dev lo")
        pro_ips[name] = ip

    route_targets = build_route_targets(con_ips, pro_ips, producer_endpoints, consumer_endpoints)
    apply_computed_routes(nodes_obj, engine, route_targets)

    # Configure buffers and DISABLE RP_FILTER
    configure_network_settings(net)

    # Check the service address and one non-shortest endpoint through con0.
    print("*** Checking connectivity (Sample: con0)")
    alternate_target = None
    if ROUTE_INSTALL_MODE == "consumer_cores":
        alternate_target = consumer_endpoints['con0'][-1]['ip']
    success = check_connectivity(net, con_ips['con0'], alternate_target)

    try:
        if success:
            print("✅ Network is ready!")

            run_many_to_few_experiment(net, con_ips, pro_ips, producer_endpoints)
        else:
            print("❌ Network failed - check debug info")
    finally:
        # Ensure cleanup happens if the experiment exits early.
        cleanup_processes(net)
        net.stop()

def configure_network_settings(net):
    """Optimize buffers and Disable Reverse Path Filtering."""
    print("*** Tuning TCP/UDP buffers and Disabling RP Filter...")
    for host in net.hosts:
        # Increase Buffers
        host.cmd('sysctl -w net.core.rmem_max=16777216 2>/dev/null || true')
        host.cmd('sysctl -w net.core.wmem_max=16777216 2>/dev/null || true')

        # DISABLE RP_FILTER (Critical for Asymmetric Routing/ECMP)
        host.cmd('sysctl -w net.ipv4.conf.all.rp_filter=0')
        host.cmd('sysctl -w net.ipv4.conf.default.rp_filter=0')
        host.cmd('for i in $(ls /sys/class/net/); do sysctl -w net.ipv4.conf.$i.rp_filter=0; done')

def check_connectivity(net, target_ip, alternate_target=None):
    """Check reachability from selected senders to the receiver endpoints."""
    checks = [
        ('pro0', target_ip, 'service'),
        (f'pro{NUM_PRODUCERS - 1}', target_ip, 'service'),
    ]
    if alternate_target:
        checks.append(('pro0', alternate_target, 'consumer-core endpoint'))

    success = True

    print(f"Target Receiver: {target_ip}")

    for sender_name, destination_ip, label in checks:
        sender = net.get(sender_name)
        print(f"Checking {sender.name} -> {destination_ip} ({label})...", end=" ")
        result = sender.cmd(f'ping -c1 -W1 {destination_ip}')
        if '0% packet loss' in result:
            print("✅ OK")
        else:
            print("❌ Fail")
            success = False
    return success

def cleanup_processes(net):
    """Kill sender/receiver processes on ALL nodes."""
    print("\n🧹 Cleaning up background processes...")

    # 1. Clean Receivers
    for i in range(NUM_CONSUMERS):
        node = net.get(f'con{i}')
        node.cmd('killall -9 receiver 2>/dev/null')

    # 2. Clean Senders
    for i in range(NUM_PRODUCERS):
        node = net.get(f'pro{i}')
        node.cmd('killall -9 sender 2>/dev/null')

    print("   Done.")

def show_experiment_summary(net):
    """Print the last few lines of logs for all receivers."""
    print("\n" + "="*60)
    print("📊 EXPERIMENT SUMMARY (All Receivers)")
    print("="*60)

    for i in range(NUM_CONSUMERS):
        c_name = f'con{i}'
        c_node = net.get(c_name)
        log_path = os.path.join(RUN_DIR, f'{c_name}.log')

        print(f"\n--- Receiver: {c_name} ---")
        # Check if log exists
        if c_node.cmd(f'test -f {shlex.quote(log_path)}; echo $?').strip() != "0":
            print("   [No Log File Found]")
        else:
            # Print last 3 lines
            log_tail = c_node.cmd(f'tail -n 3 {shlex.quote(log_path)}').strip()
            print(log_tail if log_tail else "   [Log File Empty]")

    print("\n" + "="*60)

FCT_SUMMARY_RE = re.compile(
    r"\[fct_summary\]\s+"
    r"session=(?P<session>\d+)\s+"
    r"remote=(?P<remote>\S+)\s+"
    r"expected_msgs=(?P<expected_msgs>\d+)\s+"
    r"received_msgs=(?P<received_msgs>\d+)\s+"
    r"expected_bytes=(?P<expected_bytes>\d+)\s+"
    r"received_bytes=(?P<received_bytes>\d+)\s+"
    r"fct95_s=(?P<fct95>[0-9.]+|NA)\s+"
    r"fct99_s=(?P<fct99>[0-9.]+|NA)\s+"
    r"fct100_s=(?P<fct100>[0-9.]+|NA)\s+"
    r"method=nearest_rank\s+metric=message_completion\s+"
    r"clock=monotonic\s+status=(?P<status>[a-z_]+)"
)

def percentile_nearest_rank(values, percentile):
    if not values:
        return None
    sorted_values = sorted(values)
    idx = max(0, math.ceil((percentile / 100.0) * len(sorted_values)) - 1)
    return sorted_values[min(idx, len(sorted_values) - 1)]

def collect_receiver_fct_samples(run_dir=RUN_DIR):
    """Collect exact receiver-side FCT milestones for complete flows."""
    samples = []
    for i in range(NUM_CONSUMERS):
        c_name = f'con{i}'
        log_path = os.path.join(run_dir, f'{c_name}.log')
        if not os.path.exists(log_path):
            continue

        with open(log_path, "r", encoding="utf-8", errors="replace") as log_file:
            for match in FCT_SUMMARY_RE.finditer(log_file.read()):
                if match.group("status") != "complete":
                    continue
                expected_messages = int(match.group("expected_msgs"))
                received_messages = int(match.group("received_msgs"))
                expected_bytes = int(match.group("expected_bytes"))
                received_bytes = int(match.group("received_bytes"))
                if (
                    expected_messages != APP_EXPECTED_MESSAGES
                    or received_messages != APP_EXPECTED_MESSAGES
                    or expected_bytes != APP_TRANSFER_TOTAL_BYTES
                    or received_bytes != APP_TRANSFER_TOTAL_BYTES
                ):
                    continue
                fct95 = float(match.group("fct95"))
                fct99 = float(match.group("fct99"))
                fct100 = float(match.group("fct100"))
                if not 0 <= fct95 <= fct99 <= fct100:
                    continue
                samples.append({
                    "consumer": c_name,
                    "session": int(match.group("session")),
                    "remote": match.group("remote"),
                    "expected_messages": expected_messages,
                    "received_messages": received_messages,
                    "expected_bytes": expected_bytes,
                    "received_bytes": received_bytes,
                    "fct95": fct95,
                    "fct99": fct99,
                    "fct100": fct100,
                })
    return samples

def receiver_fct_sample_counts(samples):
    return collections.Counter(sample["consumer"] for sample in samples)

def has_complete_receiver_fct_set(samples):
    counts = receiver_fct_sample_counts(samples)
    session_ids = collections.defaultdict(set)
    for sample in samples:
        session_ids[sample["consumer"]].add(sample["session"])
    return (
        len(samples) == NUM_PRODUCERS
        and all(
            counts[f'con{i}'] == PRODUCERS_PER_CONSUMER
            and len(session_ids[f'con{i}']) == PRODUCERS_PER_CONSUMER
            for i in range(NUM_CONSUMERS)
        )
    )

def consumer_fct_statistics(samples):
    """Average exact per-flow milestones within each complete consumer."""
    grouped = collections.defaultdict(list)
    for sample in samples:
        grouped[sample["consumer"]].append(sample)

    results = []
    for i in range(NUM_CONSUMERS):
        consumer = f'con{i}'
        consumer_samples = grouped[consumer]
        session_ids = {sample["session"] for sample in consumer_samples}
        if (
            len(consumer_samples) != PRODUCERS_PER_CONSUMER
            or len(session_ids) != PRODUCERS_PER_CONSUMER
        ):
            continue
        results.append({
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
        })
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

def wait_for_receiver_fct_samples(timeout=30, poll_interval=0.25):
    """Wait for all exact receiver-side FCT95/FCT99/FCT100 records."""
    print(f"⏳ Waiting for all {NUM_PRODUCERS} exact receiver FCT samples...")
    deadline = time.time() + timeout
    samples = []

    while time.time() < deadline:
        samples = collect_receiver_fct_samples()
        if has_complete_receiver_fct_set(samples):
            print(f"   ✅ All {NUM_PRODUCERS} receiver FCT samples are available")
            return True
        time.sleep(poll_interval)

    counts = receiver_fct_sample_counts(samples)
    count_text = ", ".join(
        f"con{i}={counts[f'con{i}']}" for i in range(NUM_CONSUMERS)
    )
    print(f"⚠️  Timed out waiting for receiver FCT samples ({count_text})")
    return False

def write_run_fct_summary(run_dir=RUN_DIR):
    """Write exact per-flow, per-consumer, and cross-consumer FCT summaries."""
    samples = collect_receiver_fct_samples(run_dir)
    complete = has_complete_receiver_fct_set(samples)
    summary_path = os.path.join(run_dir, os.path.basename(RUN_FCT_SUMMARY_PATH))
    counts = receiver_fct_sample_counts(samples)
    sorted_samples = sorted(
        samples,
        key=lambda sample: (sample["consumer"], sample["session"]),
    )

    summary_lines = [
        "# Exact receiver-side message-completion FCT milestones.\n",
        f"[run_summary] per_flow_fct_begin flows={len(samples)} "
        f"expected_flows={NUM_PRODUCERS} method=nearest_rank "
        "clock=monotonic\n",
    ]
    for sample in sorted_samples:
        summary_lines.append(
            f"[run_summary] consumer={sample['consumer']} "
            f"session={sample['session']} remote={sample['remote']} "
            f"fct95_s={sample['fct95']:.6f} "
            f"fct99_s={sample['fct99']:.6f} "
            f"fct100_s={sample['fct100']:.6f} status=complete\n"
        )

    if complete:
        consumer_stats = consumer_fct_statistics(samples)
        cross_stats = cross_consumer_fct_statistics(consumer_stats)
        if cross_stats is None:
            raise RuntimeError("complete flow set did not produce five consumer summaries")

        summary_lines.append(
            f"[run_summary] per_consumer_fct_begin consumers={len(consumer_stats)} "
            f"expected_consumers={NUM_CONSUMERS}\n"
        )
        for item in consumer_stats:
            summary_lines.append(
                f"[consumer_summary] consumer={item['consumer']} "
                f"mean_fct95_s={item['mean_fct95']:.6f} "
                f"mean_fct99_s={item['mean_fct99']:.6f} "
                f"mean_fct100_s={item['mean_fct100']:.6f} "
                f"max_flow_fct100_s={item['max_flow_fct100']:.6f} "
                f"flows={item['flows']} aggregation=arithmetic_mean "
                "status=complete\n"
            )
        summary_lines.append(
            f"[cross_consumer] median_fct95_s={cross_stats['median_fct95']:.6f} "
            f"max_fct95_s={cross_stats['max_fct95']:.6f} "
            f"median_fct99_s={cross_stats['median_fct99']:.6f} "
            f"max_fct99_s={cross_stats['max_fct99']:.6f} "
            f"median_fct100_s={cross_stats['median_fct100']:.6f} "
            f"max_fct100_s={cross_stats['max_fct100']:.6f} "
            f"consumers={len(consumer_stats)} "
            "aggregation=consumer_flow_means status=complete\n"
        )

        fct100_values = [sample["fct100"] for sample in samples]
        rank = math.ceil(0.95 * len(fct100_values))
        p95_fct100 = percentile_nearest_rank(fct100_values, 95)
        # Legacy nearest-rank p95(FCT100) log output is intentionally disabled.
        # summary_lines.append(
        #     f"[run_summary] all_flows_p95_fct_s={p95_fct100:.6f} "
        #     f"all_flows_max_fct_s={max(fct100_values):.6f} "
        #     f"flows={len(fct100_values)} "
        #     f"expected_flows={NUM_PRODUCERS} rank={rank} percentile=95 "
        #     "method=nearest_rank metric=fct100 status=complete\n"
        # )
        print(
            "📈 Cross-consumer exact FCT means: "
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
        print(
            f"⚠️  Exact cross-consumer FCT is unavailable: found "
            f"{len(samples)}/{NUM_PRODUCERS} exact complete-flow samples"
        )

    with open(summary_path, "w", encoding="utf-8") as summary_file:
        summary_file.writelines(summary_lines)
    print(f"   Run FCT summary: {summary_path}")
    return summary_path

def append_fct_summaries(run_dir=RUN_DIR):
    """Append exact per-flow and arithmetic-mean summaries to consumer logs."""
    print("📌 Appending exact per-consumer FCT summaries to receiver logs...")
    samples = collect_receiver_fct_samples(run_dir)
    for i in range(NUM_CONSUMERS):
        c_name = f'con{i}'
        log_path = os.path.join(run_dir, f'{c_name}.log')
        if not os.path.exists(log_path):
            print(f"   ⚠️  {c_name}: no log found")
            continue

        sessions = [sample for sample in samples if sample["consumer"] == c_name]
        stats = consumer_fct_statistics(sessions)

        if len(stats) != 1:
            summary = (
                f"[summary] {c_name} mean_fct95_s=NA mean_fct99_s=NA "
                f"mean_fct100_s=NA max_flow_fct100_s=NA "
                f"flows={len(sessions)} expected_flows={PRODUCERS_PER_CONSUMER} "
                "aggregation=arithmetic_mean status=incomplete\n"
            )
            print(f"   ⚠️  {c_name}: found {len(sessions)} exact FCT samples")
        else:
            item = stats[0]
            summary_lines = [
                f"[summary] {c_name} exact_per_flow_fct_begin "
                f"flows={len(sessions)}\n"
            ]
            for session in sorted(sessions, key=lambda value: value["session"]):
                summary_lines.append(
                    f"[summary] {c_name} session={session['session']} "
                    f"remote={session['remote']} "
                    f"fct95_s={session['fct95']:.6f} "
                    f"fct99_s={session['fct99']:.6f} "
                    f"fct100_s={session['fct100']:.6f} status=complete\n"
                )
            summary_lines.append(
                f"[summary] {c_name} mean_fct95_s={item['mean_fct95']:.6f} "
                f"mean_fct99_s={item['mean_fct99']:.6f} "
                f"mean_fct100_s={item['mean_fct100']:.6f} "
                f"max_flow_fct100_s={item['max_flow_fct100']:.6f} "
                f"flows={item['flows']} aggregation=arithmetic_mean "
                "status=complete\n"
            )
            summary = "".join(summary_lines)
            print(
                f"   {c_name}: mean exact FCT95/FCT99/FCT100 = "
                f"{item['mean_fct95']:.4f}/"
                f"{item['mean_fct99']:.4f}/"
                f"{item['mean_fct100']:.4f}s"
            )

        with open(log_path, "a", encoding="utf-8") as log_file:
            log_file.write(summary)

def wait_for_sender_completion(net, sender_done_paths, timeout=900, poll_interval=1):
    """Wait until every sender writes its done marker."""
    print(f"⏳ Waiting for all {len(sender_done_paths)} sender flows to finish...")
    pending = dict(sender_done_paths)
    failed = {}
    start_time = time.time()
    last_report = start_time

    while pending:
        finished = []
        for s_name, done_path in pending.items():
            s_node = net.get(s_name)
            if s_node.cmd(f'test -f {shlex.quote(done_path)}; echo $?').strip() == "0":
                exit_code = s_node.cmd(f'cat {shlex.quote(done_path)}').strip()
                print(f"   ✅ {s_name} finished with exit code {exit_code}")
                if exit_code != "0":
                    failed[s_name] = exit_code
                finished.append(s_name)

        for s_name in finished:
            pending.pop(s_name, None)

        if not pending:
            if failed:
                print("⚠️  Sender failures: " + ", ".join(f"{name}={code}" for name, code in sorted(failed.items())))
                return False
            return True

        now = time.time()
        if now - start_time > timeout:
            print(f"⚠️  Timeout waiting for senders: {', '.join(sorted(pending))}")
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
    print("\n" + "="*60)
    print(f"🤖 MANY-TO-FEW EXPERIMENT ({NUM_PRODUCERS} Senders -> {NUM_CONSUMERS} Receivers)")
    print("="*60)

    # 1. Start All Receivers
    for i in range(NUM_CONSUMERS):
        c_name = f'con{i}'
        c_node = net.get(c_name)
        c_ip = con_ips[c_name]
        c_log = os.path.join(RUN_DIR, f'{c_name}.log')
        c_csv = os.path.join(RUN_DIR, f'{c_name}_throughput.csv')
        print(f"🎧 Starting Receiver on {c_name} ({c_ip})...")
        c_node.cmd(
            f'{mpquic_env_prefix()}{shlex.quote(RECEIVER_BIN)} -port=6121 -multipath=true '
            f'-csv={shlex.quote(c_csv)} -csv-intervals=50,100 '
            f'-expected-msgs={APP_EXPECTED_MESSAGES} '
            f'-expected-bytes={APP_TRANSFER_TOTAL_BYTES} > '
            f'{shlex.quote(c_log)} 2>&1 &'
        )

    time.sleep(2)

    print("📨 Starting Senders with staggered delays...")

    # Base time reference for delays
    start_time_ref = time.time()

    # 2. Start All Senders
    sender_done_paths = {}
    sender_schedule = []
    for i in range(NUM_PRODUCERS):
        target_idx = i // PRODUCERS_PER_CONSUMER
        target_con_name = f'con{target_idx}'
        delay = CONSUMER_START_DELAYS.get(target_con_name, 0)
        sender_schedule.append((delay, target_idx, i))

    for delay, target_idx, i in sorted(sender_schedule):
        s_name = f'pro{i}'
        s_node = net.get(s_name)
        if ROUTE_INSTALL_MODE == "consumer_cores":
            s_bind_ip = producer_endpoints[s_name]['ip']
        else:
            s_bind_ip = pro_ips[s_name]

        # Determine target receiver (0-4 -> con0, 5-9 -> con1, etc.)
        target_con_name = f'con{target_idx}'
        target_ip = con_ips[target_con_name]

        # Calculate wait time relative to start reference
        elapsed = time.time() - start_time_ref
        time_to_wait = delay - elapsed

        if time_to_wait > 0:
            print(f"   ⏳ Waiting {time_to_wait:.2f}s before starting group for {target_con_name}...")
            time.sleep(time_to_wait)

        print(f"   🚀 {s_name} ({s_bind_ip}) -> {target_con_name} ({target_ip})")
        s_log = os.path.join(RUN_DIR, f'{s_name}.log')
        done_path = os.path.join(RUN_DIR, f'{s_name}.done')
        s_node.cmd(f'rm -f {shlex.quote(done_path)}')

        # 18,240,512 bytes = 3037 full 6006-byte chunks + one 290-byte final chunk.
        sender_cmd = (
            f'{mpquic_env_prefix()}{shlex.quote(SENDER_BIN)} -addr={target_ip}:6121 -bind={s_bind_ip} '
            f'{mpquic_sender_extra_args()}'
            f'-name="{s_name}" -size={APP_CHUNK_SIZE_BYTES} '
            f'-total={APP_TRANSFER_TOTAL_BYTES} '
            f'-pipeline={sender_pipeline_depth()} > {shlex.quote(s_log)} 2>&1'
        )
        wrapped_cmd = f"bash -lc {shlex.quote(sender_cmd + f'; echo $? > {shlex.quote(done_path)}')} &"

        s_node.cmd(wrapped_cmd)
        sender_done_paths[s_name] = done_path

    all_finished = wait_for_sender_completion(net, sender_done_paths)
    wait_for_receiver_fct_samples(timeout=30 if all_finished else 2)
    append_fct_summaries()
    write_run_fct_summary()

    # 3. Show Summary for all nodes
    show_experiment_summary(net)

    if all_finished:
        print("✅ Experiment complete. All sender flows finished; exiting Mininet.")
    else:
        print("⚠️  Experiment ended before every sender flow finished. Logs saved to .log files")

    # Cleanup is handled in the finally block of main

if __name__ == '__main__':
    setLogLevel('info')
    if not ensure_binaries_exist():
        raise SystemExit(1)
    prepare_run_dir()
    # Ensure clean start
    os.system('mn -c > /dev/null 2>&1')
    create_wan_topology()

#!/usr/bin/env python3
"""Run the pinned RTO=1 MARS face-failure/recovery experiment."""

import argparse
import hashlib
import importlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
NDND_ROOT = os.path.abspath(os.path.join(EXAMPLE_ROOT, "..", "..", ".."))

PROFILE_NAME = "failure-recovery-rto1"
FAILURE_MODE = "face-disable"
FAILURE_START_SEC = 1.0
FAILURE_END_SEC = 3.0

PINNED_ENVIRONMENT = {
    "MARS_CONSUMER_VARIANT": "shadow-fast-350-8-no-backoff-rto1",
    "MARS_FORWARDER_VARIANT": "persistent-2ms-netem-bwcap",
    "MARS_DEPLOYMENT_MODE": "normal",
    "MARS_CON_TO_CORE_LOSS": "0",
    "MARS_QDISC_DIAGNOSTICS": "0",
    "MARS_QDISC_HIERARCHY_AUDIT": "0",
}

RUNNER_PATH = os.path.join(SCRIPT_DIR, "ndnd_run.py")
CONSUMER_PATH = os.path.join(
    EXAMPLE_ROOT,
    "apps",
    "consumer",
    "consumer",
)
FORWARDER_PATH = os.path.join(NDND_ROOT, "ndnd")
PRODUCER_PATH = os.path.join(EXAMPLE_ROOT, "apps", "producer", "producer")
EXPECTED_CONSUMER_RTO_CONFIG = (
    "rtoOuterMultiplier=1 "
    "rtoFormula=M*(meanRTT+4*stdDev) "
    "minRtoMs=240 "
    "maxRtoMs=3500 "
    "fastRepairAdvancesRtoBackoff=false"
)

# Artifact hashes are recorded per run for provenance, not used as build constraints.
ARTIFACT_FILES = {
    "runner": (RUNNER_PATH, False),
    "consumer": (CONSUMER_PATH, True),
    "forwarder": (FORWARDER_PATH, True),
    "producer": (PRODUCER_PATH, True),
}

LOG_PARENT = os.path.join(
    EXAMPLE_ROOT,
    "logs",
    "failure",
    "face_disable",
    "rto_1",
    "loss_0pct",
)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as input_file:
        for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pin_environment():
    for name, value in PINNED_ENVIRONMENT.items():
        os.environ[name] = value


def inspect_artifacts():
    artifact_hashes = {}
    errors = []
    for label, (path, must_be_executable) in ARTIFACT_FILES.items():
        if not os.path.isfile(path):
            errors.append(f"{label}: missing file {path}")
            continue
        if must_be_executable and not os.access(path, os.X_OK):
            errors.append(f"{label}: file is not executable: {path}")
        artifact_hashes[label] = sha256_file(path)
    if errors:
        raise RuntimeError(
            "Experiment artifact validation failed:\n- "
            + "\n- ".join(errors)
        )
    return artifact_hashes


def verify_consumer_binary_configuration():
    result = subprocess.run(
        [CONSUMER_PATH, "--print-rto-config"],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "no output"
        raise RuntimeError(
            "Consumer configuration query failed "
            f"with exit code {result.returncode}: {detail}"
        )
    actual_config = result.stdout.strip()
    if actual_config != EXPECTED_CONSUMER_RTO_CONFIG:
        raise RuntimeError(
            "Consumer configuration mismatch:\n"
            f"  expected: {EXPECTED_CONSUMER_RTO_CONFIG}\n"
            f"  actual:   {actual_config}"
        )


def load_and_configure_runner():
    if SCRIPT_DIR not in sys.path:
        sys.path.insert(0, SCRIPT_DIR)
    runner = importlib.import_module("ndnd_run")
    runner.FAILURE_MODE = FAILURE_MODE
    runner.FAILURE_START_SEC = FAILURE_START_SEC
    runner.FAILURE_END_SEC = FAILURE_END_SEC
    return runner


def verify_runner_configuration(runner):
    expected_values = {
        "CONSUMER_VARIANT": "shadow-fast-350-8-no-backoff-rto1",
        "CONSUMER_RTO_OUTER_MULTIPLIER": 1,
        "CONSUMER_SHADOW_LOSS_ENABLED": True,
        "CONSUMER_SHADOW_FAST_REPAIR_ENABLED": True,
        "CONSUMER_FAST_REPAIR_ADVANCES_RTO_BACKOFF": False,
        "FORWARDER_VARIANT": "persistent-2ms-netem-bwcap",
        "FORWARDER_QDISC_AGGREGATION": "prefer-non-root-netem",
        "DEPLOYMENT_MODE": "normal",
        "CON_TO_CORE_LOSS": 0.0,
        "CORE_MESH_MODE": "heterogeneous",
        "HETEROGENEOUS_CORE_LINK_BWS_MBPS": [25, 40, 50, 60, 80],
        "NUM_ACTIVE_CONSUMERS": 5,
        "NUM_PRODUCERS_PER_CONSUMER": 5,
        "CONSUMER_START_DELAYS": {
            "con0": 0,
            "con1": 1,
            "con2": 2,
            "con3": 1,
            "con4": 2,
        },
        "POST_RUN_MODE": "auto_exit_on_flow_summary",
        "QDISC_DIAGNOSTICS_ENABLED": False,
        "QDISC_HIERARCHY_AUDIT_ENABLED": False,
        "QDISC_DIAGNOSTICS_REPORT_INTERVAL_SEC": 5,
        "FAILURE_MODE": FAILURE_MODE,
        "FAILURE_START_SEC": FAILURE_START_SEC,
        "FAILURE_END_SEC": FAILURE_END_SEC,
    }
    errors = []
    for name, expected in expected_values.items():
        actual = getattr(runner, name)
        if actual != expected:
            errors.append(f"{name}: expected {expected!r}, found {actual!r}")

    expected_paths = {
        "CONSUMER_BIN": CONSUMER_PATH,
        "NDND_BIN": FORWARDER_PATH,
        "PRODUCER_BIN": PRODUCER_PATH,
    }
    for name, expected in expected_paths.items():
        actual = os.path.abspath(getattr(runner, name))
        if actual != expected:
            errors.append(f"{name}: expected {expected}, found {actual}")

    if errors:
        raise RuntimeError(
            "Pinned runner configuration verification failed:\n- "
            + "\n- ".join(errors)
        )


def new_run_id(now=None, pid=None):
    if now is None:
        now = datetime.now(timezone.utc)
    if pid is None:
        pid = os.getpid()
    return f"{now.strftime('%Y%m%d-%H%M%S-%f')}-pid{pid}"


def create_exclusive_run_directory(log_parent=LOG_PARENT, now=None, pid=None):
    os.makedirs(log_parent, mode=0o775, exist_ok=True)
    run_dir = os.path.join(log_parent, new_run_id(now=now, pid=pid))
    try:
        os.mkdir(run_dir, mode=0o775)
    except FileExistsError as error:
        raise RuntimeError(
            f"Refusing to reuse existing run directory: {run_dir}"
        ) from error
    return run_dir


def augment_run_manifest(runner, logs_dir, artifact_hashes):
    manifest_path = os.path.join(logs_dir, "run_manifest.json")
    with open(manifest_path, "r", encoding="ascii") as manifest_file:
        manifest = json.load(manifest_file)

    manifest["experimentProfile"] = {
        "name": PROFILE_NAME,
        "pinnedEnvironment": dict(sorted(PINNED_ENVIRONMENT.items())),
    }
    manifest["failureRecovery"] = {
        "mode": FAILURE_MODE,
        "startOffsetMs": int(FAILURE_START_SEC * 1000),
        "endOffsetMs": int(FAILURE_END_SEC * 1000),
        "windowReference": "after all active prefixes finish path discovery",
        "scope": "one DT-valid face on each active consumer forwarder",
    }
    manifest["topologyProfile"] = {
        "coreMeshMode": runner.CORE_MESH_MODE,
        "heterogeneousCoreLinkBandwidthsMbps": (
            runner.HETEROGENEOUS_CORE_LINK_BWS_MBPS
        ),
        "consumerCoreRandomLossPercent": runner.CON_TO_CORE_LOSS,
    }
    manifest["launcher"] = {
        "path": os.path.abspath(__file__),
        "sha256": sha256_file(os.path.abspath(__file__)),
        "sourceRunnerPath": RUNNER_PATH,
        "sourceRunnerSha256": artifact_hashes["runner"],
    }
    manifest["logIsolation"] = {
        "policy": "new UTC-microsecond-and-PID directory created with exclusive mkdir",
        "runDirectory": os.path.abspath(logs_dir),
        "existingRunDirectoryReuseAllowed": False,
    }

    temporary_path = manifest_path + ".tmp"
    with open(temporary_path, "x", encoding="ascii", newline="\n") as output_file:
        json.dump(manifest, output_file, indent=2, sort_keys=True)
        output_file.write("\n")
    os.replace(temporary_path, manifest_path)


def verify_and_record_failure_events(runner, logs_dir):
    event_patterns = {
        "armed": re.compile(r"DT face failure armed.*\bfailedFace=(\d+)"),
        "started": re.compile(r"DT face failure started.*\bface=(\d+)"),
        "recovered": re.compile(r"DT face failure recovered.*\bface=(\d+)"),
    }
    consumer_results = {}
    errors = []

    for consumer_index in range(runner.NUM_ACTIVE_CONSUMERS):
        consumer_name = f"con{consumer_index}"
        log_path = os.path.join(logs_dir, f"{consumer_name}.log")
        event_faces = {event_name: [] for event_name in event_patterns}
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as log_file:
                for line in log_file:
                    for event_name, pattern in event_patterns.items():
                        match = pattern.search(line)
                        if match:
                            event_faces[event_name].append(match.group(1))
        except OSError as error:
            consumer_results[consumer_name] = {"error": str(error)}
            errors.append(f"{consumer_name}: unable to read {log_path}: {error}")
            continue

        consumer_results[consumer_name] = {
            "armedCount": len(event_faces["armed"]),
            "startedCount": len(event_faces["started"]),
            "recoveredCount": len(event_faces["recovered"]),
            "faces": event_faces,
        }

        for event_name, faces in event_faces.items():
            if len(faces) != 1:
                errors.append(
                    f"{consumer_name}: expected exactly one {event_name} event, "
                    f"found {len(faces)}"
                )
        observed_faces = {
            face for faces in event_faces.values() for face in faces
        }
        if all(event_faces[event_name] for event_name in event_patterns):
            if len(observed_faces) != 1:
                errors.append(
                    f"{consumer_name}: failure events disagree on face IDs "
                    f"{sorted(observed_faces)}"
                )

    validation = {
        "passed": not errors,
        "requiredEventsPerConsumer": ["armed", "started", "recovered"],
        "consumers": consumer_results,
        "errors": errors,
    }
    manifest_path = os.path.join(logs_dir, "run_manifest.json")
    with open(manifest_path, "r", encoding="ascii") as manifest_file:
        manifest = json.load(manifest_file)
    manifest["failureEventValidation"] = validation
    temporary_path = manifest_path + ".validation.tmp"
    with open(temporary_path, "x", encoding="ascii", newline="\n") as output_file:
        json.dump(manifest, output_file, indent=2, sort_keys=True)
        output_file.write("\n")
    os.replace(temporary_path, manifest_path)

    if errors:
        raise RuntimeError(
            "Failure/recovery event validation failed; logs preserved at "
            f"{logs_dir}:\n- " + "\n- ".join(errors)
        )
    print(
        "Failure/recovery event validation passed for "
        f"{runner.NUM_ACTIVE_CONSUMERS} consumers."
    )


def print_profile(runner, artifact_hashes, preflight):
    mode = "Preflight passed" if preflight else "Pinned profile verified"
    print(f"{mode}: {PROFILE_NAME}")
    print(f"  Consumer: {runner.CONSUMER_VARIANT} ({artifact_hashes['consumer']})")
    print(f"  Forwarder: {runner.FORWARDER_VARIANT} ({artifact_hashes['forwarder']})")
    print(f"  Producer: {artifact_hashes['producer']}")
    print(f"  Runner: {artifact_hashes['runner']}")
    print(
        "  Topology: normal deployment, heterogeneous core mesh, "
        f"consumer-core loss={runner.CON_TO_CORE_LOSS:g}%"
    )
    print(
        f"  Failure: {runner.FAILURE_MODE}, "
        f"window={runner.FAILURE_START_SEC:.1f}s-{runner.FAILURE_END_SEC:.1f}s"
    )
    print(f"  Dedicated log parent: {LOG_PARENT}")
    if preflight:
        print("  No log directory was created during preflight.")


def clean_mininet_state():
    return os.system("mn -c > /dev/null 2>&1")


def run_experiment(runner, artifact_hashes):
    if os.geteuid() != 0:
        raise RuntimeError(
            "The experiment requires root privileges. Run this launcher with sudo."
        )

    original_dir = os.getcwd()
    net = None
    clean_mininet_state()
    logs_dir = create_exclusive_run_directory()
    print(f"Using exclusive run logs directory: {logs_dir}")
    runner.write_run_manifest(logs_dir)
    augment_run_manifest(runner, logs_dir, artifact_hashes)

    try:
        net, allocator = runner.create_topology_and_net()
        runner.log_ip_configuration(net)
        runner.warmup_network(net, allocator)
        runner.start_ndnd_daemons(net, allocator, logs_dir)
        runner.run_applications(net, allocator, logs_dir)
        completed = runner.wait_for_all_flow_summaries(logs_dir)
        if not completed:
            raise RuntimeError(
                f"Flow completion validation failed; logs preserved at {logs_dir}"
            )
        verify_and_record_failure_events(runner, logs_dir)
    finally:
        try:
            if net is not None:
                try:
                    runner.stop_ndnd_daemons(net)
                finally:
                    net.stop()
        finally:
            os.chdir(original_dir)
            clean_mininet_state()


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run the pinned MARS RTO=1 face-disable failure/recovery experiment."
        )
    )
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="verify the pinned profile without starting Mininet or creating logs",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    sys.dont_write_bytecode = True
    pin_environment()
    artifact_hashes = inspect_artifacts()
    verify_consumer_binary_configuration()
    runner = load_and_configure_runner()
    runner.ensure_system_sbin_on_path()
    runner.check_local_binaries()
    verify_runner_configuration(runner)
    runner.check_scaling_limits()
    print_profile(runner, artifact_hashes, args.preflight)
    if args.preflight:
        return

    runner.setLogLevel("info")
    run_experiment(runner, artifact_hashes)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)

#!/usr/bin/env python3

import argparse
import datetime
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_ROOT = SCRIPT_DIR / "results"
MINIMAL_ROOT = RESULTS_ROOT / "heterogeneous" / "mars_3032" / "minimal"
RUNNER = SCRIPT_DIR / "mininet-mptcp-minimal-run.py"
LOSS_RATES = ("0", "0.01", "0.1", "1")
LOSS_LABELS = ("0", "0p01", "0p1", "1")
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class BatchError(RuntimeError):
    pass


def default_batch_id():
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.strftime("%Y%m%dT%H%M%SZ")


def normalize_batch_id(value):
    normalized = str(value).strip()
    if not BATCH_ID_RE.fullmatch(normalized):
        raise argparse.ArgumentTypeError(
            "batch ID must be 1-64 characters using only letters, digits, '.', '_', or '-'"
        )
    return normalized


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Run an isolated four-loss-rate MPTCP minimal batch and validate "
            "the standardized per-flow FCT95/FCT99/FCT100 results."
        )
    )
    parser.add_argument(
        "--batch-id",
        type=normalize_batch_id,
        default=default_batch_id(),
        help="Unique batch directory name (default: current UTC timestamp)",
    )
    return parser.parse_args(argv)


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as input_file:
        for block in iter(lambda: input_file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def protected_results_digest(excluded_batch):
    """Hash paths, metadata, and contents of every pre-existing result file."""
    digest = hashlib.sha256()
    if not RESULTS_ROOT.is_dir():
        return digest.hexdigest()

    excluded_batch = excluded_batch.resolve()
    files = []
    for path in RESULTS_ROOT.rglob("*"):
        if not path.is_file():
            continue
        try:
            path.resolve().relative_to(excluded_batch)
        except ValueError:
            files.append(path)

    for path in sorted(files, key=lambda item: str(item.relative_to(RESULTS_ROOT))):
        relative = str(path.relative_to(RESULTS_ROOT))
        stat_result = path.stat()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(
            f"{stat_result.st_size}:{stat_result.st_mtime_ns}:{stat_result.st_mode}".encode(
                "ascii"
            )
        )
        digest.update(b"\0")
        with path.open("rb") as input_file:
            for block in iter(lambda: input_file.read(1024 * 1024), b""):
                digest.update(block)
    return digest.hexdigest()


def read_tcp_congestion_control():
    path = Path("/proc/sys/net/ipv4/tcp_congestion_control")
    try:
        return path.read_text(encoding="ascii").strip()
    except OSError as error:
        raise BatchError(f"cannot read {path}: {error}") from error


def find_active_mininet_processes():
    matches = []
    for process_name in ("mn", "mnexec"):
        result = subprocess.run(
            ["pgrep", "-x", process_name],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            matches.append(f"{process_name}:{','.join(result.stdout.split())}")
    return matches


def preflight(batch_dir):
    if os.geteuid() != 0:
        raise BatchError("run this batch with sudo/root privileges")
    if batch_dir.exists():
        raise BatchError(f"refusing to reuse existing batch directory: {batch_dir}")
    if read_tcp_congestion_control() != "cubic":
        raise BatchError("net.ipv4.tcp_congestion_control must be cubic")
    for path in (RUNNER, SCRIPT_DIR / "sender.py", SCRIPT_DIR / "receiver.py"):
        if not path.is_file():
            raise BatchError(f"required file is missing: {path}")
    if shutil.which("mn") is None:
        raise BatchError("Mininet command 'mn' is unavailable")
    active = find_active_mininet_processes()
    if active:
        raise BatchError(
            "another Mininet process appears active; stop it before this batch: "
            + " ".join(active)
        )


def stream_command(command, console_log):
    command_text = " ".join(command)
    banner = f"\n[batch] command={command_text}\n"
    print(banner, end="", flush=True)
    console_log.write(banner)
    console_log.flush()

    process = subprocess.Popen(
        command,
        cwd=SCRIPT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)
        console_log.write(line)
        console_log.flush()
    return_code = process.wait()
    if return_code != 0:
        raise BatchError(f"command failed with exit code {return_code}: {command_text}")


def require_text(text, expected, source):
    if expected not in text:
        raise BatchError(f"{source}: missing expected record: {expected}")


def validate_loss_run(run_dir, batch_id, loss_rate):
    config_path = run_dir / "run_configuration.log"
    summary_path = run_dir / "run_fct_summary.log"
    if not config_path.is_file() or not summary_path.is_file():
        raise BatchError(f"{run_dir}: missing configuration or FCT summary")

    config = config_path.read_text(encoding="utf-8")
    for expected in (
        f"batch_id={batch_id}",
        f"con_to_core_loss_percent={loss_rate}",
        "core_link_mode=heterogeneous",
        "mptcp_subflow_mode=single",
        "mptcp_experiment_mode=baseline_ecmp",
        "mptcp_max_total_subflows=1",
        "producer_subflow_endpoints=0",
        "consumer_signal_endpoints=0",
        "sender_bind=loopback",
        "fct_standard=mars_3032x6016_v1",
        "fct_start_boundary=sender_before_start_preamble",
        "expected_messages=3032",
        "message_size_bytes=6016",
        "expected_bytes=18240512",
        "tcp_congestion_control=cubic",
    ):
        require_text(config, expected, config_path)

    sender_logs = sorted(run_dir.glob("pro*.log"))
    done_files = sorted(run_dir.glob("pro*.done"))
    receiver_logs = sorted(run_dir.glob("con[0-4].log"))
    if len(sender_logs) != 25 or len(done_files) != 25 or len(receiver_logs) != 5:
        raise BatchError(
            f"{run_dir}: expected 25 sender logs, 25 done files, and 5 receiver logs; "
            f"found {len(sender_logs)}, {len(done_files)}, and {len(receiver_logs)}"
        )

    for path in done_files:
        if path.read_text(encoding="ascii").strip() != "0":
            raise BatchError(f"{path}: sender did not exit with code 0")
    for path in sender_logs:
        text = path.read_text(encoding="utf-8", errors="replace")
        require_text(text, "Socket protocol: 262 (mptcp=True)", path)
        require_text(text, "Completed transfer for pro", path)
        require_text(text, "Transferred: 18240512 bytes", path)

    raw_fct_records = []
    for path in receiver_logs:
        records = [line for line in path.read_text(
            encoding="utf-8", errors="replace"
        ).splitlines() if "[fct_summary]" in line]
        if len(records) != 5:
            raise BatchError(f"{path}: expected 5 raw FCT records, found {len(records)}")
        raw_fct_records.extend(records)
    for record in raw_fct_records:
        for expected in (
            "expected_msgs=3032",
            "message_size_bytes=6016",
            "expected_bytes=18240512",
            "received_bytes=18240512",
            "fct95_target_msgs=2881",
            "fct99_target_msgs=3002",
            "fct100_target_msgs=3032",
            "method=nearest_rank",
            "status=complete",
            "standard=mars_3032x6016_v1",
            "start_boundary=sender_before_start_preamble",
        ):
            require_text(record, expected, run_dir)

    summary_lines = summary_path.read_text(
        encoding="utf-8", errors="replace"
    ).splitlines()
    flow_rows = [line for line in summary_lines if line.startswith("[run_summary] consumer=")]
    if len(flow_rows) != 25 or any("status=complete" not in row for row in flow_rows):
        raise BatchError(f"{summary_path}: incomplete 25-flow exact FCT summary")
    if not any(
        line.startswith("[cross_flow]") and "status=complete" in line
        for line in summary_lines
    ):
        raise BatchError(f"{summary_path}: missing complete cross-flow summary")


def chown_batch(batch_dir):
    sudo_uid = os.environ.get("SUDO_UID")
    sudo_gid = os.environ.get("SUDO_GID")
    if sudo_uid is None or sudo_gid is None:
        return
    uid = int(sudo_uid)
    gid = int(sudo_gid)
    for current_dir, dir_names, file_names in os.walk(batch_dir):
        os.chown(current_dir, uid, gid)
        os.chmod(current_dir, 0o775)
        for name in dir_names:
            path = Path(current_dir) / name
            os.chown(path, uid, gid)
            os.chmod(path, 0o775)
        for name in file_names:
            path = Path(current_dir) / name
            os.chown(path, uid, gid)
            os.chmod(path, 0o664)


def write_batch_audit(batch_dir, batch_id, before_digest, after_digest):
    audit_path = batch_dir / "batch_audit.log"
    sources = (
        Path(__file__).resolve(),
        RUNNER,
        SCRIPT_DIR / "sender.py",
        SCRIPT_DIR / "receiver.py",
        SCRIPT_DIR / "fct_protocol.py",
    )
    lines = [
        "# MPTCP minimal four-loss-rate batch audit.\n",
        f"completed_utc={datetime.datetime.now(datetime.timezone.utc).isoformat()}\n",
        f"batch_id={batch_id}\n",
        f"batch_dir={batch_dir}\n",
        "loss_rates_percent=0,0.01,0.1,1\n",
        "mode=single/baseline_ecmp\n",
        "fct_standard=mars_3032x6016_v1\n",
        "validated_runs=4\n",
        "validated_flows=100\n",
        f"protected_results_sha256_before={before_digest}\n",
        f"protected_results_sha256_after={after_digest}\n",
        f"protected_results_unchanged={str(before_digest == after_digest).lower()}\n",
    ]
    for path in sources:
        lines.append(f"sha256.{path.name}={file_sha256(path)}\n")
    with audit_path.open("x", encoding="utf-8") as output_file:
        output_file.writelines(lines)


def main(argv=None):
    args = parse_args(argv)
    batch_dir = MINIMAL_ROOT / args.batch_id
    try:
        preflight(batch_dir)
        protected_before = protected_results_digest(batch_dir)
        batch_dir.mkdir(parents=True, exist_ok=False)
    except (BatchError, OSError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    console_path = batch_dir / "batch_console.log"
    try:
        with console_path.open("x", encoding="utf-8") as console_log:
            for loss_rate, loss_label in zip(LOSS_RATES, LOSS_LABELS):
                command = [
                    sys.executable,
                    str(RUNNER),
                    "--core-link-mode",
                    "heterogeneous",
                    "--con-to-core-loss",
                    loss_rate,
                    "--batch-id",
                    args.batch_id,
                ]
                stream_command(command, console_log)
                run_dir = batch_dir / f"con_core_loss_{loss_label}pct"
                validate_loss_run(run_dir, args.batch_id, loss_rate)
                current_digest = protected_results_digest(batch_dir)
                if current_digest != protected_before:
                    raise BatchError(
                        "a pre-existing result file changed during the batch; stopping"
                    )

        protected_after = protected_results_digest(batch_dir)
        if protected_after != protected_before:
            raise BatchError("pre-existing result tree digest changed")
        write_batch_audit(
            batch_dir,
            args.batch_id,
            protected_before,
            protected_after,
        )
        print(f"\nBatch completed and validated: {batch_dir}")
        return 0
    except (BatchError, OSError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        print(f"Partial batch retained without overwrite: {batch_dir}", file=sys.stderr)
        return 2
    finally:
        chown_batch(batch_dir)


if __name__ == "__main__":
    raise SystemExit(main())

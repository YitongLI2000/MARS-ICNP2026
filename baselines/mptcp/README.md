# MPTCP Mininet Emulation Baseline

## Requirements

- a Linux kernel with MPTCP enabled
- Python 3
- Mininet with Open vSwitch
- `iproute2` with `ip mptcp`
- `net.ipv4.tcp_congestion_control=cubic`
- root privileges

Check the host before running:

```bash
sysctl net.mptcp.enabled
sysctl net.ipv4.tcp_congestion_control
ip mptcp help
```

## Modes

- **Minimal:** uses one MPTCP subflow.
- **Oracle:** exposes the consumer-core paths as MPTCP subflows.

All MPTCP subflows use CUBIC. Both run entry points verify the system setting
and stop before the experiment if it is not `cubic`.

## Run

From `baselines/mptcp`:

```bash
# Minimal
sudo python3 run_mptcp_minimal_batch.py \
  --batch-id "minimal-$(date -u +%Y%m%dT%H%M%SZ)"

# Oracle
sudo env MPTCP_RESULTS_RUN_ID="oracle-$(date -u +%Y%m%dT%H%M%SZ)" \
  python3 mininet-mptcp-run.py --core-link-mode heterogeneous
```

The batch and run IDs only name new isolated result directories; they do not
change the experiment configuration. `batch_audit.log` records the source
hashes used for the Minimal batch.

Run only one Mininet experiment on the host at a time.

## License

See [`LICENSE`](LICENSE) and [`../../THIRD_PARTY.md`](../../THIRD_PARTY.md).

# MPQUIC Mininet Emulation Baseline

## Requirements

- Go 1.17 or a compatible newer release
- Python 3
- Mininet with Open vSwitch
- root privileges

## Build

From `baselines/mpquic`:

```bash
go mod download
cd example/mars-many-to-few
go build -o receiver receiver.go
go build -o sender sender.go
cd ../..
```

## Modes

- **Minimal:** uses the initial MPQUIC path with CUBIC.
- **Oracle:** uses CUBIC on the initial path and coupled OLIA on the additional
  paths.

## Run

From `baselines/mpquic`:

```bash
# Minimal
sudo python3 example/mars-many-to-few/scripts/mininet-mpquic-minimal-four-loss.py

# Oracle
sudo env MPQUIC_RESULTS_RUN_ID="oracle-$(date -u +%Y%m%dT%H%M%SZ)" \
  MPQUIC_ROUTE_INSTALL_MODE=consumer_cores \
  python3 example/mars-many-to-few/scripts/mininet-mpquic-run.py
```

The Oracle run ID only names a new isolated result directory; it does not
change the experiment configuration.

Run only one Mininet experiment on the host at a time.

## License

See [`LICENSE`](LICENSE) and [`../../THIRD_PARTY.md`](../../THIRD_PARTY.md).

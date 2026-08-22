# MARS Mininet Emulation

## Requirements

- Go 1.24.3 or newer
- Python 3
- Mininet with Open vSwitch
- a C compiler, `make`, `iproute2`, `iputils-ping`, `procps`, and `psmisc`
- root privileges

## Build

From the repository root:

```bash
cd mars
go mod download
(cd std/examples/mars && go mod download)
./rebuild.sh
```

## Run

From `mars/std/examples/mars`:

```bash
# Normal
sudo env MARS_DEPLOYMENT_MODE=normal python3 scripts/ndnd_run.py

# Minimal
sudo env MARS_DEPLOYMENT_MODE=minimal python3 scripts/ndnd_run.py

# Failure
sudo python3 scripts/ndnd_run_failure_rto1.py
```

Each run writes logs, manifests, and CSV files under `logs/` in a separate run
directory. Generated node configurations are written under `configs/`.

The runners perform host-wide Mininet cleanup and stop experiment processes by
name. Run only one Mininet experiment on the host at a time.

## License

See [`LICENSE.md`](LICENSE.md) and [`../THIRD_PARTY.md`](../THIRD_PARTY.md).
Retain all upstream license, copyright, and source-file notices.

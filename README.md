# MARS: Multipath Adaptive Reliable Service

This repository contains the Mininet emulation code for MARS and the MPTCP
and MPQUIC baselines.

## Components

| Component | Documentation |
| --- | --- |
| MARS | [`mars/`](mars/README.md) |
| MPTCP | [`baselines/mptcp/`](baselines/mptcp/README.md) |
| MPQUIC | [`baselines/mpquic/`](baselines/mpquic/README.md) |

Each component README lists its requirements and run commands. Mininet runs
require root privileges.

## Quick Start

From the repository root:

```bash
cd mars
go mod download
(cd std/examples/mars && go mod download)
./rebuild.sh
cd std/examples/mars
sudo env MARS_DEPLOYMENT_MODE=normal python3 scripts/ndnd_run.py
```

Run only one Mininet experiment on the host at a time.

## License

This repository contains components under different licenses. See the license
file in each component and [`THIRD_PARTY.md`](THIRD_PARTY.md).

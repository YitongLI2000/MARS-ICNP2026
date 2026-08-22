# Third-Party Notices

This repository contains components under different licenses. There is no
single repository-wide license. The license file nearest a source tree and
the notices in individual source files are authoritative.

## Bundled Source

| Component | Third-party software | License files and notices |
|---|---|---|
| `mars/` | [NDNd](https://github.com/named-data/ndnd) | `mars/LICENSE.md`, `mars/fw/CITATION.cff`, `mars/dv/CITATION.cff`, and retained source-file notices |
| `baselines/mpquic/` | [mp-quic](https://github.com/qdeconinck/mp-quic) and [quic-go](https://github.com/lucas-clemente/quic-go) | `baselines/mpquic/LICENSE` and retained source-file notices |

The repository-specific and experiment-specific files in each component use
that component's stated license. Preserve all license files, copyright
statements, and source-file notices when redistributing the repository or an
individual component.

## External Runtime Dependencies

`baselines/mptcp/` requires Linux MPTCP, Mininet, Open vSwitch, Python, and
standard Linux networking tools. These dependencies are not redistributed.
The component's own files are licensed under `baselines/mptcp/LICENSE`.

The Go modules declared in `mars/go.mod` and `baselines/mpquic/go.mod` are
downloaded during setup and are not vendored in this repository. Their own
license terms apply.

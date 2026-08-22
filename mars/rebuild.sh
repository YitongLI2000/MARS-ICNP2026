#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_ROOT="${SCRIPT_DIR}/std/examples/mars"
CONSUMER_BIN="${EXAMPLE_ROOT}/apps/consumer/consumer"
PRODUCER_BIN="${EXAMPLE_ROOT}/apps/producer/producer"

if [[ -x /usr/local/go/bin/go ]]; then
  MARS_GO_ROOT="/usr/local/go"
  MARS_GO_BIN="${MARS_GO_ROOT}/bin/go"
else
  MARS_GO_BIN="$(command -v go)"
  MARS_GO_ROOT="$(${MARS_GO_BIN} env GOROOT)"
fi

export GOROOT="${MARS_GO_ROOT}"
export PATH="${MARS_GO_ROOT}/bin:${PATH}"
export GOCACHE="${GOCACHE:-${SCRIPT_DIR}/build/go-cache}"

echo "[INFO] Testing release consumer"
"${MARS_GO_BIN}" -C "${EXAMPLE_ROOT}" test -race ./apps/consumer

echo "[INFO] Testing release forwarder"
"${MARS_GO_BIN}" -C "${SCRIPT_DIR}" test ./fw/fw ./fw/face

echo "[INFO] Building release forwarder"
make -C "${SCRIPT_DIR}" ndnd

echo "[INFO] Building producer"
"${MARS_GO_BIN}" -C "${EXAMPLE_ROOT}" build -trimpath \
  -o "${PRODUCER_BIN}" ./apps/producer

echo "[INFO] Building RTO1 consumer"
"${MARS_GO_BIN}" -C "${EXAMPLE_ROOT}" build -trimpath \
  -o "${CONSUMER_BIN}" ./apps/consumer

expected_config="rtoOuterMultiplier=1 rtoFormula=M*(meanRTT+4*stdDev) minRtoMs=240 maxRtoMs=3500 fastRepairAdvancesRtoBackoff=false"
actual_config="$(${CONSUMER_BIN} --print-rto-config)"
if [[ "${actual_config}" != "${expected_config}" ]]; then
  echo "[ERROR] Release consumer configuration mismatch" >&2
  echo "[ERROR] expected: ${expected_config}" >&2
  echo "[ERROR] actual:   ${actual_config}" >&2
  exit 1
fi

echo "[INFO] Build succeeded"
echo "  - ${SCRIPT_DIR}/ndnd"
echo "  - ${PRODUCER_BIN}"
echo "  - ${CONSUMER_BIN}"

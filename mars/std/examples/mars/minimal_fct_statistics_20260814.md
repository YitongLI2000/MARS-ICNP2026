# Minimal Deployment FCT Statistics

This report is a pinned record of the minimal-deployment experiment runs
listed below. It does not select the newest directory under each loss-rate
directory. Older minimal runs and runs created after these pinned runs are not
included.

## Standardized MARS FCT Calculation

Use this definition unchanged when producing the same table for another
solution:

- **Eligible population:** one loss-rate run contains 25 application flows
  (5 consumers x 5 flows). Include only flows whose final summary has
  `complete=true`, `receivedPackets=expectedPackets`, and `missingPackets=0`.
- **Per-flow milestones:** let `N=expectedPackets` and
  `k_p=ceil(p*N)`. `FCT95` is the elapsed time from the MARS DT flow
  `startTime` until the `k_0.95`-th unique, valid Data packet arrives;
  `FCT99` and `FCT100` use `k_0.99` and `N`, respectively. Duplicate or
  invalid Data does not advance the count. Here `N=3032`, so the milestones
  are packets 2881, 3002, and 3032.
- **Median Flow FCTp:** collect the 25 per-flow `FCTp` values for one loss
  rate, sort them in ascending order, and select the 13th value. No
  per-consumer averaging is performed first.
- **Max Flow FCTp:** select the largest of the same 25 per-flow `FCTp` values.
- **Precision:** aggregate the full-precision values from `Flow Summary`, then
  round only the displayed result to three decimal places. All values are in
  seconds.
- **Cross-solution mapping:** use the same application-flow start boundary and
  the same 3032 application-level logical chunks per flow. Count each logical
  chunk once when it is completely delivered. Do not use TCP segments, QUIC
  packets, NDNLP fragments, or retransmission attempts as milestone units.

`FCT95` is a 95% flow-completion milestone, not the 95th percentile of packet
latency or of the 25 flows.

## Source Runs

All paths are relative to `std/examples/mars/logs/`. For each run, the FCT data
come only from the five `Flow Summary` records in each listed consumer log.

| Loss Rate | Dataset Directory | Consumer Logs | Start Time (UTC) |
|---:|:---|:---|:---|
| 0% | `minimal/loss_0pct/20260814-090543/` | `con0_app.log`, `con1_app.log`, `con2_app.log`, `con3_app.log`, `con4_app.log` | `2026-08-14T09:05:43Z` |
| 0.01% | `minimal/loss_0p01pct/20260814-090733/` | `con0_app.log`, `con1_app.log`, `con2_app.log`, `con3_app.log`, `con4_app.log` | `2026-08-14T09:07:33Z` |
| 0.1% | `minimal/loss_0p1pct/20260814-090923/` | `con0_app.log`, `con1_app.log`, `con2_app.log`, `con3_app.log`, `con4_app.log` | `2026-08-14T09:09:23Z` |
| 1% | `minimal/loss_1pct/20260814-091114/` | `con0_app.log`, `con1_app.log`, `con2_app.log`, `con3_app.log`, `con4_app.log` | `2026-08-14T09:11:14Z` |

The corresponding metadata file for every row is `run_manifest.json` in the
dataset directory. All four manifests record `deploymentMode=minimal`,
`failureMode=normal`, and `rtoEstimator.outerMultiplier=1`, and identify the
same consumer build:

- Variant: `shadow-fast-350-8-no-backoff-rto1`
- SHA256: `d80a6f9e32d7373772e5bab68e60d69b23b609e1c2eb0352b8b8f4c4c9272456`

## Aggregation Definitions

- Each consumer has five completed flows.
- `Mean FCT95`, `Mean FCT99`, and `Mean FCT100` are arithmetic means across
  that consumer's five flow summaries.
- The Cross-Consumer table follows the standardized 25-flow median/max
  definition above.
- In the Per-Consumer table, `Max Flow FCT100` is the maximum among that
  consumer's five flows.
- All FCT values are in seconds and are rounded to three decimal places.

## Cross-Consumer FCT Statistics

| Loss Rate | Median Flow FCT95 | Max Flow FCT95 | Median Flow FCT99 | Max Flow FCT99 | Median Flow FCT100 | Max Flow FCT100 |
|---:|---:|---:|---:|---:|---:|---:|
| 0% | 18.785 | 19.828 | 19.411 | 20.451 | 19.591 | 20.723 |
| 0.01% | 19.098 | 20.560 | 19.605 | 20.727 | 19.702 | 20.953 |
| 0.1% | 19.251 | 20.850 | 19.804 | 21.010 | 19.881 | 21.049 |
| 1% | 19.710 | 21.015 | 20.205 | 21.212 | 20.589 | 21.706 |

## Per-Consumer FCT Statistics

| Loss Rate | Consumer | Mean FCT95 | Mean FCT99 | Mean FCT100 | Max Flow FCT100 |
|---:|:---:|---:|---:|---:|---:|
| 0% | con0 | 18.506 | 18.996 | 19.101 | 20.187 |
| 0% | con1 | 18.637 | 19.147 | 19.259 | 19.846 |
| 0% | con2 | 19.123 | 19.776 | 19.950 | 20.723 |
| 0% | con3 | 18.664 | 19.182 | 19.308 | 20.199 |
| 0% | con4 | 18.389 | 18.955 | 19.082 | 19.644 |
| 0.01% | con0 | 19.541 | 20.058 | 20.166 | 20.683 |
| 0.01% | con1 | 18.296 | 18.756 | 18.856 | 19.643 |
| 0.01% | con2 | 19.554 | 20.108 | 20.256 | 20.953 |
| 0.01% | con3 | 18.986 | 19.486 | 19.598 | 20.205 |
| 0.01% | con4 | 18.481 | 18.946 | 19.067 | 19.751 |
| 0.1% | con0 | 18.586 | 19.089 | 19.212 | 20.017 |
| 0.1% | con1 | 19.722 | 20.264 | 20.434 | 21.049 |
| 0.1% | con2 | 18.682 | 19.271 | 19.517 | 20.522 |
| 0.1% | con3 | 18.609 | 19.074 | 19.210 | 20.210 |
| 0.1% | con4 | 18.453 | 18.923 | 19.239 | 20.454 |
| 1% | con0 | 18.774 | 19.210 | 19.681 | 21.009 |
| 1% | con1 | 18.705 | 19.156 | 19.376 | 20.932 |
| 1% | con2 | 19.054 | 19.541 | 19.991 | 21.290 |
| 1% | con3 | 19.320 | 19.811 | 20.261 | 21.619 |
| 1% | con4 | 19.378 | 19.950 | 20.445 | 21.706 |

## Verification

- Parsed records: 4 loss rates x 5 consumers x 5 flows = 100 flow summaries.
- Every parsed flow is unique and has `complete=true`,
  `receivedPackets=expectedPackets=3032`, `missingPackets=0`, and
  `rtoOuterMultiplier=1`.
- Each Cross-Consumer row was computed directly from its 25 eligible flows;
  consumer means were not used.
- Total RTO expirations are 2928, 3542, 4331, and 8125 at 0%, 0.01%, 0.1%,
  and 1% loss, respectively.
- The unrounded 1% worst-flow value was verified directly in
  `minimal/loss_1pct/20260814-091114/con4_app.log` for flow `/pro20app` as
  `21.705975540` seconds.

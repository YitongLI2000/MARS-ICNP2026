# RTO = 1 FCT Statistics

This report is a pinned record of the RTO = 1 experiment runs listed below. It
does not select the newest directory under each loss-rate directory. Runs
created after these pinned runs belong to other experiments and are not
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
| 0% | `loss_0pct/20260814-083632/` | `con0_app.log`, `con1_app.log`, `con2_app.log`, `con3_app.log`, `con4_app.log` | `2026-08-14T08:36:32Z` |
| 0.01% | `loss_0p01pct/20260814-084049/` | `con0_app.log`, `con1_app.log`, `con2_app.log`, `con3_app.log`, `con4_app.log` | `2026-08-14T08:40:49Z` |
| 0.1% | `loss_0p1pct/20260814-083838/` | `con0_app.log`, `con1_app.log`, `con2_app.log`, `con3_app.log`, `con4_app.log` | `2026-08-14T08:38:38Z` |
| 1% | `loss_1pct/20260814-083309/` | `con0_app.log`, `con1_app.log`, `con2_app.log`, `con3_app.log`, `con4_app.log` | `2026-08-14T08:33:10Z` |

The corresponding metadata file for every row is `run_manifest.json` in the
dataset directory. All four manifests record `rtoEstimator.outerMultiplier=1`
and identify the same consumer build:

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
| 0% | 4.161 | 4.887 | 4.339 | 5.108 | 4.544 | 5.291 |
| 0.01% | 4.178 | 4.960 | 4.354 | 5.183 | 4.550 | 5.622 |
| 0.1% | 4.205 | 4.917 | 4.375 | 5.135 | 4.590 | 5.231 |
| 1% | 4.384 | 5.303 | 4.606 | 5.642 | 5.210 | 6.458 |

## Per-Consumer FCT Statistics

| Loss Rate | Consumer | Mean FCT95 | Mean FCT99 | Mean FCT100 | Max Flow FCT100 |
|---:|:---:|---:|---:|---:|---:|
| 0% | con0 | 4.170 | 4.341 | 4.541 | 4.553 |
| 0% | con1 | 4.866 | 5.085 | 5.269 | 5.291 |
| 0% | con2 | 3.898 | 4.055 | 4.127 | 4.134 |
| 0% | con3 | 4.237 | 4.412 | 4.634 | 4.647 |
| 0% | con4 | 3.903 | 4.060 | 4.185 | 4.191 |
| 0.01% | con0 | 4.181 | 4.356 | 4.551 | 4.580 |
| 0.01% | con1 | 4.903 | 5.133 | 5.473 | 5.622 |
| 0.01% | con2 | 3.932 | 4.093 | 4.238 | 4.244 |
| 0.01% | con3 | 4.405 | 4.591 | 4.818 | 4.888 |
| 0.01% | con4 | 3.898 | 4.057 | 4.135 | 4.152 |
| 0.1% | con0 | 4.202 | 4.373 | 4.606 | 4.692 |
| 0.1% | con1 | 4.862 | 5.071 | 5.213 | 5.231 |
| 0.1% | con2 | 3.946 | 4.101 | 4.243 | 4.279 |
| 0.1% | con3 | 4.262 | 4.450 | 4.636 | 4.670 |
| 0.1% | con4 | 3.927 | 4.085 | 4.179 | 4.196 |
| 1% | con0 | 4.385 | 4.601 | 5.284 | 5.498 |
| 1% | con1 | 5.232 | 5.574 | 6.103 | 6.458 |
| 1% | con2 | 4.158 | 4.324 | 5.154 | 6.129 |
| 1% | con3 | 4.506 | 4.708 | 5.269 | 5.961 |
| 1% | con4 | 4.126 | 4.300 | 4.737 | 5.107 |

## Verification

- Parsed records: 4 loss rates x 5 consumers x 5 flows = 100 flow summaries.
- Every parsed flow is unique and has `complete=true`,
  `receivedPackets=expectedPackets=3032`, and `missingPackets=0`.
- Each Cross-Consumer row was computed directly from its 25 eligible flows;
  consumer means were not used.
- Total RTO expirations are 1036, 1266, 1275, and 4507 at 0%, 0.01%, 0.1%,
  and 1% loss, respectively.
- The unrounded 1% worst-flow value was verified directly in
  `loss_1pct/20260814-083309/con1_app.log` as `6.458004406` seconds.

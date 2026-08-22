# MPQUIC FCT Statistics

This report summarizes the explicitly listed MPQUIC loss-rate runs. It uses only exact receiver-side `[fct_summary]` records and does not use the legacy checkpoint estimates.

## Standardized MPQUIC FCT Calculation

Use this definition unchanged when reproducing these MPQUIC tables:

- **Eligible population:** one loss-rate run contains 25 application flows (5 consumers x 5 flows). Include only flows whose receiver summary has `status=complete`, `received_msgs=expected_msgs=3038`, and `received_bytes=expected_bytes=18240512`.
- **Per-flow milestones:** let `N=expected_msgs` and `k_p=ceil(p*N)`. `FCT95` is the elapsed time from the MPQUIC receiver flow `startTime` until the `k_0.95`-th application chunk is completely read. `startTime` is captured once when the receiver starts handling the accepted QUIC session, before accepting its application streams. `FCT99` and `FCT100` use `k_0.99` and `N`, respectively. Here `N=3038`, so the milestones are chunks 2887, 3008, and 3038.
- **Logical completion unit:** one sender application chunk uses one bidirectional QUIC stream and advances the count once only after the receiver has completely read that stream. QUIC packets, frames, and transport retransmission attempts do not advance the count.
- **Median Flow FCTp:** collect the 25 per-flow `FCTp` values for one loss rate, sort them in ascending order, and select the 13th value. No per-consumer averaging is performed first.
- **Max Flow FCTp:** select the largest of the same 25 per-flow `FCTp` values.
- **Precision:** aggregate the receiver's six-decimal per-flow values without intermediate rounding, then round only the displayed result to three decimal places. All values are in seconds.

`FCT95` is a 95% flow-completion milestone, not the 95th percentile of packet latency or of the 25 flows.

## Reference Runs

For each run, FCT data were taken only from the five listed consumer logs. Generated run logs are not included in the repository.

| Loss Rate | Dataset Directory | Consumer Logs |
| --- | --- | --- |
| 0% | `con_core_loss_0pct/` | `con0.log`, `con1.log`, `con2.log`, `con3.log`, `con4.log` |
| 0.01% | `con_core_loss_0p01pct/` | `con0.log`, `con1.log`, `con2.log`, `con3.log`, `con4.log` |
| 0.1% | `con_core_loss_0p1pct/` | `con0.log`, `con1.log`, `con2.log`, `con3.log`, `con4.log` |
| 1% | `con_core_loss_1pct/` | `con0.log`, `con1.log`, `con2.log`, `con3.log`, `con4.log` |

## Aggregation Definitions

- Each consumer has five completed flows.
- `Mean FCT95`, `Mean FCT99`, and `Mean FCT100` are arithmetic means across that consumer's five per-flow summaries.
- The Cross-Consumer table follows the standardized direct 25-flow median/max definition above.
- In the Per-Consumer table, `Max Flow FCT100` is the maximum among that consumer's five flows.
- All displayed FCT values are seconds rounded to three decimal places.

## Cross-Consumer FCT Statistics

| Loss Rate | Median Flow FCT95 | Max Flow FCT95 | Median Flow FCT99 | Max Flow FCT99 | Median Flow FCT100 | Max Flow FCT100 |
| --- | --- | --- | --- | --- | --- | --- |
| 0% | 6.113 | 6.182 | 6.370 | 6.433 | 6.431 | 6.482 |
| 0.01% | 6.123 | 6.196 | 6.376 | 6.451 | 6.427 | 6.494 |
| 0.1% | 5.992 | 6.205 | 6.193 | 6.453 | 6.242 | 6.502 |
| 1% | 12.278 | 14.701 | 13.138 | 15.452 | 13.259 | 15.699 |

## Per-Consumer FCT Statistics

| Loss Rate | Consumer | Mean FCT95 | Mean FCT99 | Mean FCT100 | Max Flow FCT100 |
| --- | --- | --- | --- | --- | --- |
| 0% | con0 | 6.170 | 6.421 | 6.476 | 6.482 |
| 0% | con1 | 6.167 | 6.419 | 6.475 | 6.478 |
| 0% | con2 | 4.174 | 4.343 | 4.384 | 4.389 |
| 0% | con3 | 6.121 | 6.379 | 6.437 | 6.465 |
| 0% | con4 | 4.168 | 4.337 | 4.378 | 4.389 |
| 0.01% | con0 | 6.132 | 6.380 | 6.429 | 6.452 |
| 0.01% | con1 | 6.169 | 6.421 | 6.475 | 6.491 |
| 0.01% | con2 | 4.172 | 4.341 | 4.381 | 4.389 |
| 0.01% | con3 | 6.175 | 6.429 | 6.487 | 6.494 |
| 0.01% | con4 | 4.173 | 4.344 | 4.384 | 4.393 |
| 0.1% | con0 | 6.114 | 6.336 | 6.385 | 6.441 |
| 0.1% | con1 | 6.175 | 6.426 | 6.480 | 6.502 |
| 0.1% | con2 | 4.214 | 4.386 | 4.427 | 4.437 |
| 0.1% | con3 | 6.091 | 6.278 | 6.330 | 6.424 |
| 0.1% | con4 | 4.215 | 4.386 | 4.439 | 4.456 |
| 1% | con0 | 12.740 | 13.589 | 13.702 | 14.523 |
| 1% | con1 | 12.721 | 13.535 | 13.678 | 15.699 |
| 1% | con2 | 11.647 | 12.322 | 12.455 | 13.610 |
| 1% | con3 | 12.482 | 13.292 | 13.416 | 14.068 |
| 1% | con4 | 12.185 | 12.907 | 13.030 | 15.209 |

## Verification

- Parsed records: 4 loss rates x 5 consumers x 5 flows = 100 per-flow summaries.
- Every parsed flow has a unique session ID within its consumer, `status=complete`, `received_msgs=expected_msgs=3038`, and `received_bytes=expected_bytes=18240512`.
- Each Cross-Consumer row was computed directly from its 25 eligible flows; consumer means were not used.

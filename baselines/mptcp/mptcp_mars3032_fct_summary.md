# MPTCP MARS-Standardized FCT Statistics

Standard: `mars_3032x6016_v1`. Each run contains 25 application flows using 3,032 complete 6,016-byte logical chunks. All values are in seconds.

## Cross-Consumer FCT Statistics

Median Flow and Max Flow are calculated directly across all 25 per-flow values; consumer means are not applied first.

| Loss Rate | Median Flow FCT95 | Max Flow FCT95 | Median Flow FCT99 | Max Flow FCT99 | Median Flow FCT100 | Max Flow FCT100 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0% | 6.803 | 12.633 | 6.999 | 12.744 | 7.206 | 12.745 |
| 0.01% | 7.106 | 12.150 | 7.418 | 12.173 | 7.625 | 12.202 |
| 0.1% | 6.938 | 12.750 | 7.186 | 12.950 | 7.195 | 13.004 |
| 1% | 11.104 | 15.928 | 11.283 | 16.387 | 11.384 | 16.628 |

## Per-Consumer FCT Statistics

Mean FCT* is the arithmetic mean across five flows. Max Flow FCT100 is the slowest of those five flows. All values are in seconds.

| Loss Rate | Consumer | Mean FCT95 | Mean FCT99 | Mean FCT100 | Max Flow FCT100 |
| --- | --- | ---: | ---: | ---: | ---: |
| 0% | con0 | 7.109 | 7.433 | 7.467 | 8.758 |
| 0% | con1 | 11.877 | 12.270 | 12.433 | 12.745 |
| 0% | con2 | 6.331 | 6.485 | 6.648 | 6.960 |
| 0% | con3 | 7.274 | 7.445 | 7.625 | 7.877 |
| 0% | con4 | 6.270 | 6.486 | 6.589 | 7.025 |
| 0.01% | con0 | 9.775 | 10.015 | 10.270 | 12.202 |
| 0.01% | con1 | 9.201 | 9.497 | 9.599 | 11.004 |
| 0.01% | con2 | 6.175 | 6.370 | 6.517 | 7.241 |
| 0.01% | con3 | 7.293 | 7.517 | 7.663 | 8.195 |
| 0.01% | con4 | 6.226 | 6.409 | 6.502 | 7.104 |
| 0.1% | con0 | 10.688 | 10.949 | 10.999 | 13.004 |
| 0.1% | con1 | 7.120 | 7.331 | 7.395 | 10.302 |
| 0.1% | con2 | 6.418 | 6.622 | 6.678 | 7.288 |
| 0.1% | con3 | 7.102 | 7.298 | 7.352 | 8.791 |
| 0.1% | con4 | 6.389 | 6.566 | 6.608 | 7.614 |
| 1% | con0 | 12.403 | 12.681 | 12.783 | 14.888 |
| 1% | con1 | 13.346 | 13.804 | 13.931 | 16.628 |
| 1% | con2 | 9.965 | 10.384 | 10.533 | 12.009 |
| 1% | con3 | 10.227 | 10.639 | 10.742 | 15.808 |
| 1% | con4 | 9.896 | 10.320 | 10.550 | 13.213 |

## Compatibility: Cross-Consumer Mean Statistics

This retained view calculates median and max across the five consumer flow means. It is not the standardized 25-flow Cross-Consumer table.

| Loss Rate | Median FCT95 | Max FCT95 | Median FCT99 | Max FCT99 | Median FCT100 | Max FCT100 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0% | 7.109 | 11.877 | 7.433 | 12.270 | 7.467 | 12.433 |
| 0.01% | 7.293 | 9.775 | 7.517 | 10.015 | 7.663 | 10.270 |
| 0.1% | 7.102 | 10.688 | 7.298 | 10.949 | 7.352 | 10.999 |
| 1% | 10.227 | 13.346 | 10.639 | 13.804 | 10.742 | 13.931 |

Exact per-flow FCT95/FCT99/FCT100 records are available in `mptcp_mars3032_fct_per_flow.csv`.

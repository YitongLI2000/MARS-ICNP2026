#!/usr/bin/env python3
"""Generate exact MPQUIC cross-consumer and per-consumer FCT tables."""

from __future__ import annotations

import argparse
import csv
import os
import re
import statistics
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


EXPECTED_CONSUMERS = 5
EXPECTED_FLOWS_PER_CONSUMER = 5
EXPECTED_MESSAGES = 3038
EXPECTED_BYTES = 18240512

LOSS_DIR_RE = re.compile(r"^con_core_loss_(?P<loss>[0-9]+(?:p[0-9]+)?)pct$")
FCT_SUMMARY_RE = re.compile(
    r"\[fct_summary\]\s+"
    r"session=(?P<session>\d+)\s+"
    r"remote=(?P<remote>\S+)\s+"
    r"expected_msgs=(?P<expected_msgs>\d+)\s+"
    r"received_msgs=(?P<received_msgs>\d+)\s+"
    r"expected_bytes=(?P<expected_bytes>\d+)\s+"
    r"received_bytes=(?P<received_bytes>\d+)\s+"
    r"fct95_s=(?P<fct95>[0-9.]+|NA)\s+"
    r"fct99_s=(?P<fct99>[0-9.]+|NA)\s+"
    r"fct100_s=(?P<fct100>[0-9.]+|NA)\s+"
    r"method=(?P<method>\S+)\s+"
    r"metric=(?P<metric>\S+)\s+"
    r"clock=(?P<clock>\S+)\s+"
    r"status=(?P<status>\S+)"
)


class AnalysisError(RuntimeError):
    pass


@dataclass(frozen=True)
class FlowResult:
    loss_pct: Decimal
    consumer: str
    session: int
    fct95_s: Decimal
    fct99_s: Decimal
    fct100_s: Decimal


@dataclass(frozen=True)
class ConsumerResult:
    loss_pct: Decimal
    consumer: str
    flows: int
    mean_fct95_s: Decimal
    mean_fct99_s: Decimal
    mean_fct100_s: Decimal
    max_flow_fct100_s: Decimal


@dataclass(frozen=True)
class CrossConsumerResult:
    loss_pct: Decimal
    consumers: int
    flows: int
    median_fct95_s: Decimal
    max_fct95_s: Decimal
    median_fct99_s: Decimal
    max_fct99_s: Decimal
    median_fct100_s: Decimal
    max_fct100_s: Decimal


def loss_from_directory(path: Path) -> Decimal:
    match = LOSS_DIR_RE.fullmatch(path.name)
    if match is None:
        raise AnalysisError(f"unexpected loss directory: {path}")
    return Decimal(match.group("loss").replace("p", "."))


def parse_consumer_log(path: Path, loss_pct: Decimal) -> list[FlowResult]:
    results: list[FlowResult] = []
    summary_lines = 0

    with path.open("r", encoding="utf-8", errors="replace") as log_file:
        for line_number, line in enumerate(log_file, start=1):
            if "[fct_summary]" not in line:
                continue
            summary_lines += 1
            match = FCT_SUMMARY_RE.search(line)
            if match is None:
                raise AnalysisError(
                    f"{path}:{line_number}: malformed [fct_summary] record"
                )

            label = f"{path}:{line_number}: session {match.group('session')}"
            if match.group("status") != "complete":
                raise AnalysisError(f"{label}: status is not complete")
            if (
                int(match.group("expected_msgs")) != EXPECTED_MESSAGES
                or int(match.group("received_msgs")) != EXPECTED_MESSAGES
                or int(match.group("expected_bytes")) != EXPECTED_BYTES
                or int(match.group("received_bytes")) != EXPECTED_BYTES
            ):
                raise AnalysisError(f"{label}: message or byte count is incomplete")
            if (
                match.group("method") != "nearest_rank"
                or match.group("metric") != "message_completion"
                or match.group("clock") != "monotonic"
            ):
                raise AnalysisError(f"{label}: unexpected measurement semantics")
            if "NA" in (
                match.group("fct95"),
                match.group("fct99"),
                match.group("fct100"),
            ):
                raise AnalysisError(f"{label}: missing FCT milestone")

            fct95 = Decimal(match.group("fct95"))
            fct99 = Decimal(match.group("fct99"))
            fct100 = Decimal(match.group("fct100"))
            if not Decimal(0) <= fct95 <= fct99 <= fct100:
                raise AnalysisError(f"{label}: FCT milestones are not monotonic")

            results.append(
                FlowResult(
                    loss_pct=loss_pct,
                    consumer=path.stem,
                    session=int(match.group("session")),
                    fct95_s=fct95,
                    fct99_s=fct99,
                    fct100_s=fct100,
                )
            )

    if summary_lines != EXPECTED_FLOWS_PER_CONSUMER:
        raise AnalysisError(
            f"{path}: found {summary_lines} FCT summaries, expected "
            f"{EXPECTED_FLOWS_PER_CONSUMER}"
        )
    session_ids = {result.session for result in results}
    if len(session_ids) != EXPECTED_FLOWS_PER_CONSUMER:
        raise AnalysisError(f"{path}: FCT summary session IDs are not unique")
    return results


def collect_flows(results_dir: Path) -> list[FlowResult]:
    loss_dirs = sorted(
        (
            path
            for path in results_dir.iterdir()
            if path.is_dir() and LOSS_DIR_RE.fullmatch(path.name)
        ),
        key=loss_from_directory,
    )
    if not loss_dirs:
        raise AnalysisError(f"{results_dir}: no con_core_loss_*pct directories")

    flows: list[FlowResult] = []
    expected_names = {f"con{i}.log" for i in range(EXPECTED_CONSUMERS)}
    for loss_dir in loss_dirs:
        consumer_logs = set(loss_dir.glob("con*.log"))
        actual_names = {path.name for path in consumer_logs}
        if actual_names != expected_names:
            raise AnalysisError(
                f"{loss_dir}: consumer logs are {sorted(actual_names)}, expected "
                f"{sorted(expected_names)}"
            )
        loss_pct = loss_from_directory(loss_dir)
        for consumer_index in range(EXPECTED_CONSUMERS):
            flows.extend(
                parse_consumer_log(loss_dir / f"con{consumer_index}.log", loss_pct)
            )
    return flows


def decimal_mean(values: list[Decimal]) -> Decimal:
    return sum(values, start=Decimal(0)) / Decimal(len(values))


def aggregate_consumers(flows: list[FlowResult]) -> list[ConsumerResult]:
    groups: dict[tuple[Decimal, str], list[FlowResult]] = {}
    for flow in flows:
        groups.setdefault((flow.loss_pct, flow.consumer), []).append(flow)

    results: list[ConsumerResult] = []
    for loss_pct, consumer in sorted(groups, key=lambda item: (item[0], item[1])):
        group = groups[(loss_pct, consumer)]
        if len(group) != EXPECTED_FLOWS_PER_CONSUMER:
            raise AnalysisError(
                f"loss={loss_pct}% {consumer}: found {len(group)} flows, expected "
                f"{EXPECTED_FLOWS_PER_CONSUMER}"
            )
        results.append(
            ConsumerResult(
                loss_pct=loss_pct,
                consumer=consumer,
                flows=len(group),
                mean_fct95_s=decimal_mean([item.fct95_s for item in group]),
                mean_fct99_s=decimal_mean([item.fct99_s for item in group]),
                mean_fct100_s=decimal_mean([item.fct100_s for item in group]),
                max_flow_fct100_s=max(item.fct100_s for item in group),
            )
        )
    return results


def aggregate_cross_consumers(flows: list[FlowResult]) -> list[CrossConsumerResult]:
    """Calculate median and max directly across all 25 flows at each loss."""
    groups: dict[Decimal, list[FlowResult]] = {}
    for flow in flows:
        groups.setdefault(flow.loss_pct, []).append(flow)

    results: list[CrossConsumerResult] = []
    for loss_pct, group in sorted(groups.items()):
        expected_flows = EXPECTED_CONSUMERS * EXPECTED_FLOWS_PER_CONSUMER
        if len(group) != expected_flows:
            raise AnalysisError(
                f"loss={loss_pct}%: found {len(group)} flows, expected "
                f"{expected_flows}"
            )
        fct95 = [item.fct95_s for item in group]
        fct99 = [item.fct99_s for item in group]
        fct100 = [item.fct100_s for item in group]
        results.append(
            CrossConsumerResult(
                loss_pct=loss_pct,
                consumers=EXPECTED_CONSUMERS,
                flows=len(group),
                median_fct95_s=statistics.median(fct95),
                max_fct95_s=max(fct95),
                median_fct99_s=statistics.median(fct99),
                max_fct99_s=max(fct99),
                median_fct100_s=statistics.median(fct100),
                max_fct100_s=max(fct100),
            )
        )
    return results


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def display_seconds(value: Decimal) -> str:
    return f"{value:.3f}"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def write_cross_csv(path: Path, results: list[CrossConsumerResult]) -> None:
    fields = [
        "loss_pct",
        "median_fct95_s",
        "max_fct95_s",
        "median_fct99_s",
        "max_fct99_s",
        "median_fct100_s",
        "max_fct100_s",
        "consumers",
        "flows",
        "aggregation",
        "status",
    ]
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "loss_pct": decimal_text(result.loss_pct),
                    "median_fct95_s": decimal_text(result.median_fct95_s),
                    "max_fct95_s": decimal_text(result.max_fct95_s),
                    "median_fct99_s": decimal_text(result.median_fct99_s),
                    "max_fct99_s": decimal_text(result.max_fct99_s),
                    "median_fct100_s": decimal_text(result.median_fct100_s),
                    "max_fct100_s": decimal_text(result.max_fct100_s),
                    "consumers": result.consumers,
                    "flows": result.flows,
                    "aggregation": "direct_25_flow_median_max",
                    "status": "complete",
                }
            )


def write_consumer_csv(path: Path, results: list[ConsumerResult]) -> None:
    fields = [
        "loss_pct",
        "consumer",
        "mean_fct95_s",
        "mean_fct99_s",
        "mean_fct100_s",
        "max_flow_fct100_s",
        "flows",
        "aggregation",
        "status",
    ]
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "loss_pct": decimal_text(result.loss_pct),
                    "consumer": result.consumer,
                    "mean_fct95_s": decimal_text(result.mean_fct95_s),
                    "mean_fct99_s": decimal_text(result.mean_fct99_s),
                    "mean_fct100_s": decimal_text(result.mean_fct100_s),
                    "max_flow_fct100_s": decimal_text(
                        result.max_flow_fct100_s
                    ),
                    "flows": result.flows,
                    "aggregation": "arithmetic_mean",
                    "status": "complete",
                }
            )


def write_markdown(
    path: Path,
    flows: list[FlowResult],
    consumers: list[ConsumerResult],
    cross_consumers: list[CrossConsumerResult],
    cross_csv_path: Path,
    consumer_csv_path: Path,
) -> None:
    report_dir = path.resolve().parent
    cross_csv_reference = Path(
        os.path.relpath(cross_csv_path.resolve(), start=report_dir)
    ).as_posix()
    consumer_csv_reference = Path(
        os.path.relpath(consumer_csv_path.resolve(), start=report_dir)
    ).as_posix()
    cross_rows = [
        [
            f"{decimal_text(result.loss_pct)}%",
            display_seconds(result.median_fct95_s),
            display_seconds(result.max_fct95_s),
            display_seconds(result.median_fct99_s),
            display_seconds(result.max_fct99_s),
            display_seconds(result.median_fct100_s),
            display_seconds(result.max_fct100_s),
        ]
        for result in cross_consumers
    ]
    consumer_rows = [
        [
            f"{decimal_text(result.loss_pct)}%",
            result.consumer,
            display_seconds(result.mean_fct95_s),
            display_seconds(result.mean_fct99_s),
            display_seconds(result.mean_fct100_s),
            display_seconds(result.max_flow_fct100_s),
        ]
        for result in consumers
    ]

    source_rows = [
        [
            f"{decimal_text(result.loss_pct)}%",
            f"`con_core_loss_{decimal_text(result.loss_pct).replace('.', 'p')}pct/`",
            "`con0.log`, `con1.log`, `con2.log`, `con3.log`, `con4.log`",
        ]
        for result in cross_consumers
    ]

    text = "\n".join(
        [
            "# MPQUIC FCT Statistics",
            "",
            "This report summarizes the explicitly listed MPQUIC loss-rate runs. "
            "It uses only exact receiver-side `[fct_summary]` records and does "
            "not use the legacy checkpoint estimates.",
            "",
            "## Standardized MPQUIC FCT Calculation",
            "",
            "Use this definition unchanged when reproducing these MPQUIC tables:",
            "",
            f"- **Eligible population:** one loss-rate run contains "
            f"{EXPECTED_CONSUMERS * EXPECTED_FLOWS_PER_CONSUMER} application "
            f"flows ({EXPECTED_CONSUMERS} consumers x "
            f"{EXPECTED_FLOWS_PER_CONSUMER} flows). Include only flows whose "
            f"receiver summary has `status=complete`, "
            f"`received_msgs=expected_msgs={EXPECTED_MESSAGES}`, and "
            f"`received_bytes=expected_bytes={EXPECTED_BYTES}`.",
            "- **Per-flow milestones:** let `N=expected_msgs` and "
            "`k_p=ceil(p*N)`. `FCT95` is the elapsed time from the MPQUIC "
            "receiver flow `startTime` until the `k_0.95`-th application chunk "
            "is completely read. `startTime` is captured once when the receiver "
            "starts handling the accepted QUIC session, before accepting its "
            "application streams. `FCT99` and `FCT100` use `k_0.99` and `N`, "
            f"respectively. Here `N={EXPECTED_MESSAGES}`, so the milestones are "
            "chunks 2887, 3008, and 3038.",
            "- **Logical completion unit:** one sender application chunk uses one "
            "bidirectional QUIC stream and advances the count once only after "
            "the receiver has completely read that stream. QUIC packets, frames, "
            "and transport retransmission attempts do not advance the count.",
            "- **Median Flow FCTp:** collect the 25 per-flow `FCTp` values for one "
            "loss rate, sort them in ascending order, and select the 13th value. "
            "No per-consumer averaging is performed first.",
            "- **Max Flow FCTp:** select the largest of the same 25 per-flow "
            "`FCTp` values.",
            "- **Precision:** aggregate the receiver's six-decimal per-flow "
            "values without intermediate rounding, then round only the displayed "
            "result to three decimal places. All values are in seconds.",
            "",
            "`FCT95` is a 95% flow-completion milestone, not the 95th percentile "
            "of packet latency or of the 25 flows.",
            "",
            "## Source Runs",
            "",
            "All paths are relative to `example/mars-many-to-few/results/`. For "
            "each run, FCT data come only from the five listed consumer logs.",
            "",
            markdown_table(
                ["Loss Rate", "Dataset Directory", "Consumer Logs"],
                source_rows,
            ),
            "",
            "## Aggregation Definitions",
            "",
            "- Each consumer has five completed flows.",
            "- `Mean FCT95`, `Mean FCT99`, and `Mean FCT100` are arithmetic "
            "means across that consumer's five per-flow summaries.",
            "- The Cross-Consumer table follows the standardized direct 25-flow "
            "median/max definition above.",
            "- In the Per-Consumer table, `Max Flow FCT100` is the maximum among "
            "that consumer's five flows.",
            "- All displayed FCT values are seconds rounded to three decimal places.",
            "",
            "## Cross-Consumer FCT Statistics",
            "",
            markdown_table(
                [
                    "Loss Rate",
                    "Median Flow FCT95",
                    "Max Flow FCT95",
                    "Median Flow FCT99",
                    "Max Flow FCT99",
                    "Median Flow FCT100",
                    "Max Flow FCT100",
                ],
                cross_rows,
            ),
            "",
            "## Per-Consumer FCT Statistics",
            "",
            markdown_table(
                [
                    "Loss Rate",
                    "Consumer",
                    "Mean FCT95",
                    "Mean FCT99",
                    "Mean FCT100",
                    "Max Flow FCT100",
                ],
                consumer_rows,
            ),
            "",
            "## Verification",
            "",
            f"- Parsed records: {len(cross_consumers)} loss rates x "
            f"{EXPECTED_CONSUMERS} consumers x "
            f"{EXPECTED_FLOWS_PER_CONSUMER} flows = {len(flows)} per-flow "
            "summaries.",
            f"- Every parsed flow has a unique session ID within its consumer, "
            f"`status=complete`, `received_msgs=expected_msgs={EXPECTED_MESSAGES}`, "
            f"and `received_bytes=expected_bytes={EXPECTED_BYTES}`.",
            "- Each Cross-Consumer row was computed directly from its 25 eligible "
            "flows; consumer means were not used.",
            f"- Machine-readable tables: `{cross_csv_reference}` and "
            f"`{consumer_csv_reference}`.",
            "",
        ]
    )
    path.write_text(text, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    default_results_dir = script_dir.parent / "results"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=default_results_dir)
    parser.add_argument("--markdown", type=Path)
    parser.add_argument("--cross-csv", type=Path)
    parser.add_argument("--consumer-csv", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results_dir = args.results_dir.resolve()
    markdown_path = args.markdown or results_dir / "exact_fct_statistics.md"
    cross_csv_path = args.cross_csv or results_dir / "exact_cross_consumer_fct.csv"
    consumer_csv_path = (
        args.consumer_csv or results_dir / "exact_per_consumer_fct.csv"
    )

    try:
        flows = collect_flows(results_dir)
        consumers = aggregate_consumers(flows)
        cross_consumers = aggregate_cross_consumers(flows)
        write_cross_csv(cross_csv_path, cross_consumers)
        write_consumer_csv(consumer_csv_path, consumers)
        write_markdown(
            markdown_path,
            flows,
            consumers,
            cross_consumers,
            cross_csv_path,
            consumer_csv_path,
        )
    except (AnalysisError, OSError, ValueError) as error:
        raise SystemExit(f"error: {error}") from error

    print(
        f"Validated {len(flows)} flows, {len(consumers)} consumer summaries, "
        f"and {len(cross_consumers)} loss rates"
    )
    print(f"Markdown: {markdown_path}")
    print(f"Cross-consumer CSV: {cross_csv_path}")
    print(f"Per-consumer CSV: {consumer_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

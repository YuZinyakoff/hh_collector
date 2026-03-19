from __future__ import annotations

import argparse
import sys
from uuid import UUID

from hhru_platform.application.commands.report_run_coverage import (
    CrawlRunNotFoundError,
    ReportRunCoverageCommand,
    RunCoverageReport,
    report_run_coverage,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyCrawlPartitionRepository,
    SqlAlchemyCrawlRunRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.observability.metrics import get_metrics_registry

DEFAULT_MAX_TREE_ROWS = 200


def register_reporting_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    coverage_parser = subparsers.add_parser(
        "show-run-coverage",
        help="Print planner-v2 style coverage summary for a crawl_run.",
    )
    coverage_parser.add_argument(
        "--run-id",
        type=UUID,
        required=True,
        help="Existing crawl_run identifier.",
    )
    coverage_parser.set_defaults(handler=handle_show_run_coverage)

    tree_parser = subparsers.add_parser(
        "show-run-tree",
        help="Print a compact tree view of crawl_partitions for a crawl_run.",
    )
    tree_parser.add_argument(
        "--run-id",
        type=UUID,
        required=True,
        help="Existing crawl_run identifier.",
    )
    tree_parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help="Optional maximum depth to show.",
    )
    tree_parser.add_argument(
        "--max-rows",
        type=int,
        default=DEFAULT_MAX_TREE_ROWS,
        help="Maximum number of rows to print.",
    )
    tree_parser.set_defaults(handler=handle_show_run_tree)


def handle_show_run_coverage(args: argparse.Namespace) -> int:
    try:
        report = _load_run_coverage_report(args.run_id)
    except CrawlRunNotFoundError as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_run_coverage_summary(report)
    return 0


def handle_show_run_tree(args: argparse.Namespace) -> int:
    max_depth = args.max_depth
    max_rows = args.max_rows
    if max_depth is not None and max_depth < 0:
        print("max_depth must be greater than or equal to zero", file=sys.stderr)
        return 1
    if max_rows is not None and max_rows < 1:
        print("max_rows must be greater than or equal to one", file=sys.stderr)
        return 1

    try:
        report = _load_run_coverage_report(args.run_id)
    except CrawlRunNotFoundError as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_run_tree(report, max_depth=max_depth, max_rows=max_rows)
    return 0


def _load_run_coverage_report(crawl_run_id: UUID) -> RunCoverageReport:
    with session_scope() as session:
        return report_run_coverage(
            ReportRunCoverageCommand(crawl_run_id=crawl_run_id),
            crawl_run_repository=SqlAlchemyCrawlRunRepository(session),
            crawl_partition_repository=SqlAlchemyCrawlPartitionRepository(session),
            metrics_recorder=get_metrics_registry(),
        )


def _print_run_coverage_summary(report: RunCoverageReport) -> None:
    summary = report.summary
    print("run coverage summary")
    print(f"run_id={summary.crawl_run_id}")
    print(f"run_type={summary.run_type}")
    print(f"run_status={summary.run_status}")
    print(f"total_partitions={summary.total_partitions}")
    print(f"root_partitions={summary.root_partitions}")
    print(f"terminal_partitions={summary.terminal_partitions}")
    print(f"covered_terminal_partitions={summary.covered_terminal_partitions}")
    print(f"pending_partitions={summary.pending_partitions}")
    print(f"pending_terminal_partitions={summary.pending_terminal_partitions}")
    print(f"running_partitions={summary.running_partitions}")
    print(f"split_partitions={summary.split_partitions}")
    print(f"unresolved_partitions={summary.unresolved_partitions}")
    print(f"failed_partitions={summary.failed_partitions}")
    print(f"coverage_ratio={summary.coverage_ratio:.4f}")
    print(f"fully_covered={'yes' if summary.is_fully_covered else 'no'}")


def _print_run_tree(
    report: RunCoverageReport,
    *,
    max_depth: int | None,
    max_rows: int | None,
) -> None:
    filtered_rows = [
        row
        for row in report.tree_rows
        if max_depth is None or row.depth <= max_depth
    ]
    shown_rows = filtered_rows if max_rows is None else filtered_rows[:max_rows]
    is_truncated = len(shown_rows) < len(filtered_rows)

    print("run partition tree")
    print(f"run_id={report.crawl_run.id}")
    print(f"run_type={report.crawl_run.run_type}")
    print(f"run_status={report.crawl_run.status}")
    print(f"total_rows={len(report.tree_rows)}")
    print(f"shown_rows={len(shown_rows)}")
    print(f"max_depth={max_depth if max_depth is not None else '-'}")
    print(f"max_rows={max_rows if max_rows is not None else '-'}")
    print(f"truncated={'yes' if is_truncated else 'no'}")

    for row in shown_rows:
        indent = "  " * row.depth
        print(
            indent
            + "partition="
            + (
                f"{row.partition_id} "
                f"parent={row.parent_partition_id or '-'} "
                f"depth={row.depth} "
                f"scope_key={row.scope_key} "
                f"status={row.status} "
                f"coverage_status={row.coverage_status} "
                f"is_terminal={'yes' if row.is_terminal else 'no'} "
                f"is_saturated={'yes' if row.is_saturated else 'no'}"
            )
        )

    if is_truncated:
        print("more_rows=hidden")

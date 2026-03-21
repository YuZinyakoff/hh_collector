from __future__ import annotations

import argparse
import sys

from hhru_platform.application.commands.run_housekeeping import (
    HOUSEKEEPING_MODE_DRY_RUN,
    HousekeepingRetentionPolicy,
    RunHousekeepingCommand,
    RunHousekeepingResult,
    run_housekeeping,
)
from hhru_platform.config.settings import get_settings
from hhru_platform.infrastructure.db.repositories import SqlAlchemyHousekeepingRepository
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.housekeeping import LocalReportArtifactStore
from hhru_platform.infrastructure.observability.metrics import get_metrics_registry


def register_housekeeping_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "run-housekeeping",
        help=(
            "Preview or execute conservative retention cleanup for old raw, snapshot, "
            "finished run state, detail-attempt history, and local report artifacts."
        ),
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete eligible rows/files. Default is dry-run preview only.",
    )
    parser.add_argument(
        "--triggered-by",
        default="run-housekeeping",
        help="Actor or subsystem that initiated housekeeping. Defaults to run-housekeeping.",
    )
    parser.set_defaults(handler=handle_run_housekeeping)


def handle_run_housekeeping(args: argparse.Namespace) -> int:
    settings = get_settings()
    command = RunHousekeepingCommand(
        retention_policy=HousekeepingRetentionPolicy(
            raw_api_payload_retention_days=settings.housekeeping_raw_api_payload_retention_days,
            vacancy_snapshot_retention_days=settings.housekeeping_vacancy_snapshot_retention_days,
            finished_crawl_run_retention_days=(
                settings.housekeeping_finished_crawl_run_retention_days
            ),
            detail_fetch_attempt_retention_days=(
                settings.housekeeping_detail_fetch_attempt_retention_days
            ),
            report_artifact_retention_days=settings.housekeeping_report_artifact_retention_days,
            report_artifact_dir=settings.housekeeping_report_artifact_dir,
            delete_limit_per_target=settings.housekeeping_delete_limit_per_target,
        ),
        execute=bool(args.execute),
        triggered_by=str(args.triggered_by),
    )

    try:
        with session_scope() as session:
            result = run_housekeeping(
                command,
                housekeeping_repository=SqlAlchemyHousekeepingRepository(session),
                report_artifact_store=LocalReportArtifactStore(),
                metrics_recorder=get_metrics_registry(),
            )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_run_housekeeping_summary(result)
    return 0


def _print_run_housekeeping_summary(result: RunHousekeepingResult) -> None:
    if result.mode == HOUSEKEEPING_MODE_DRY_RUN:
        print("completed housekeeping dry-run")
    else:
        print("completed housekeeping execution")
    print(f"status={result.status}")
    print(f"mode={result.mode}")
    print(f"triggered_by={result.triggered_by}")
    print(f"evaluated_at={result.evaluated_at.isoformat()}")
    print(f"total_candidates={result.total_candidates}")
    print(f"total_action_count={result.total_action_count}")
    print(f"total_deleted={result.total_deleted}")
    for summary in result.summaries:
        print(
            "target="
            f"{summary.target} "
            f"item_type={summary.item_type} "
            f"enabled={'yes' if summary.enabled else 'no'} "
            f"retention_days={summary.retention_days} "
            f"cutoff={summary.cutoff.isoformat() if summary.cutoff is not None else '-'} "
            f"candidate_count={summary.candidate_count} "
            f"action_count={summary.action_count} "
            f"deleted_count={summary.deleted_count} "
            f"limited={'yes' if summary.limited else 'no'}"
        )

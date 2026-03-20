from __future__ import annotations

import argparse
import sys
import time

from hhru_platform.application.commands.run_collection_once_v2 import (
    RunCollectionOnceV2Command,
)
from hhru_platform.application.commands.scheduler_loop import (
    SchedulerLoopCommand,
    SchedulerLoopResult,
    scheduler_loop,
)
from hhru_platform.application.commands.trigger_run_now import (
    TRIGGER_RUN_NOW_STATUS_SKIPPED_ACTIVE_RUN,
    TRIGGER_RUN_NOW_STATUS_SKIPPED_OVERLAP,
    TriggerRunNowCommand,
    TriggerRunNowResult,
    trigger_run_now,
)
from hhru_platform.application.policies.list_engine import PartitionSaturationPolicyV1
from hhru_platform.application.policies.reconciliation import (
    MissingRunsReconciliationPolicyV1,
)
from hhru_platform.infrastructure.db.admission import (
    PostgresCollectionRunAdmissionController,
)
from hhru_platform.infrastructure.hh_api.client import HHApiClient
from hhru_platform.infrastructure.observability.metrics import get_metrics_registry
from hhru_platform.interfaces.cli.commands.run_once import (
    _execute_run_collection_once_v2_step,
    _parse_yes_no,
    _print_run_once_v2_summary,
)


def register_scheduler_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    trigger_parser = subparsers.add_parser(
        "trigger-run-now",
        help=(
            "Attempt one guarded immediate run-once-v2 execution "
            "with scheduler admission control."
        ),
    )
    _add_run_once_v2_arguments(
        trigger_parser,
        default_triggered_by="trigger-run-now",
    )
    trigger_parser.set_defaults(handler=handle_trigger_run_now)

    scheduler_parser = subparsers.add_parser(
        "scheduler-loop",
        help=(
            "Run the scheduler-lite loop that periodically attempts "
            "guarded run-once-v2 executions."
        ),
    )
    _add_run_once_v2_arguments(
        scheduler_parser,
        default_triggered_by="scheduler-loop",
    )
    scheduler_parser.add_argument(
        "--interval-seconds",
        type=float,
        default=3600.0,
        help="Interval between scheduler ticks. Defaults to 3600 seconds.",
    )
    scheduler_parser.add_argument(
        "--max-ticks",
        type=int,
        default=None,
        help="Optional number of ticks to execute before exiting.",
    )
    scheduler_parser.set_defaults(handler=handle_scheduler_loop)


def handle_trigger_run_now(args: argparse.Namespace) -> int:
    api_client = HHApiClient.from_settings()
    saturation_policy = PartitionSaturationPolicyV1()
    reconciliation_policy = MissingRunsReconciliationPolicyV1()
    admission_controller = PostgresCollectionRunAdmissionController()
    metrics_recorder = get_metrics_registry()

    result = trigger_run_now(
        TriggerRunNowCommand(run_command=_build_run_once_v2_command(args)),
        admission_controller=admission_controller,
        run_collection_once_v2_step=lambda step_command: _execute_run_collection_once_v2_step(
            step_command,
            api_client=api_client,
            saturation_policy=saturation_policy,
            reconciliation_policy=reconciliation_policy,
        ),
        metrics_recorder=metrics_recorder,
    )

    _print_trigger_run_now_summary(result)
    return _trigger_exit_code(result)


def handle_scheduler_loop(args: argparse.Namespace) -> int:
    api_client = HHApiClient.from_settings()
    saturation_policy = PartitionSaturationPolicyV1()
    reconciliation_policy = MissingRunsReconciliationPolicyV1()
    admission_controller = PostgresCollectionRunAdmissionController()
    metrics_recorder = get_metrics_registry()

    try:
        result = scheduler_loop(
            SchedulerLoopCommand(
                interval_seconds=float(args.interval_seconds),
                max_ticks=args.max_ticks,
                run_command=_build_run_once_v2_command(args),
            ),
            trigger_run_now_step=lambda step_command: trigger_run_now(
                step_command,
                admission_controller=admission_controller,
                run_collection_once_v2_step=lambda run_command: (
                    _execute_run_collection_once_v2_step(
                        run_command,
                        api_client=api_client,
                        saturation_policy=saturation_policy,
                        reconciliation_policy=reconciliation_policy,
                    )
                ),
                metrics_recorder=metrics_recorder,
            ),
            sleep_step=time.sleep,
        )
    except KeyboardInterrupt:
        print("scheduler loop interrupted", file=sys.stderr)
        return 130

    _print_scheduler_loop_summary(result)
    return 0


def _add_run_once_v2_arguments(
    parser: argparse.ArgumentParser,
    *,
    default_triggered_by: str,
) -> None:
    parser.add_argument(
        "--sync-dictionaries",
        choices=("yes", "no"),
        default="no",
        help="Sync all supported dictionaries before the run. Defaults to no.",
    )
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=100,
        help="Maximum number of selective detail fetches after list coverage completes.",
    )
    parser.add_argument(
        "--detail-refresh-ttl-days",
        type=int,
        default=30,
        help="TTL in days for selective detail refreshes. Defaults to 30.",
    )
    parser.add_argument(
        "--run-type",
        default="weekly_sweep",
        help="Logical run_type for the created crawl_run. Defaults to weekly_sweep.",
    )
    parser.add_argument(
        "--triggered-by",
        default=default_triggered_by,
        help=f"Actor or subsystem that initiated the flow. Defaults to {default_triggered_by}.",
    )


def _build_run_once_v2_command(args: argparse.Namespace) -> RunCollectionOnceV2Command:
    return RunCollectionOnceV2Command(
        sync_dictionaries=_parse_yes_no(str(args.sync_dictionaries)),
        detail_limit=int(args.detail_limit),
        detail_refresh_ttl_days=int(args.detail_refresh_ttl_days),
        run_type=str(args.run_type),
        triggered_by=str(args.triggered_by),
    )


def _print_trigger_run_now_summary(result: TriggerRunNowResult) -> None:
    if result.status == TRIGGER_RUN_NOW_STATUS_SKIPPED_OVERLAP:
        print("skipped trigger-run-now due to admission overlap")
    elif result.status == TRIGGER_RUN_NOW_STATUS_SKIPPED_ACTIVE_RUN:
        print("skipped trigger-run-now because an active crawl run already exists")
    elif result.run_result is None:
        print("failed trigger-run-now execution")
    else:
        print("completed trigger-run-now execution")

    print(f"status={result.status}")
    print(f"run_id={result.run_id or '-'}")
    print(f"active_run_id={result.active_run_id or '-'}")
    print(f"active_run_status={result.active_run_status or '-'}")
    print(f"error={result.error_message or '-'}")
    if result.run_result is not None:
        _print_run_once_v2_summary(result.run_result)


def _print_scheduler_loop_summary(result: SchedulerLoopResult) -> None:
    print("completed scheduler loop")
    print(f"ticks_executed={result.ticks_executed}")
    print(f"runs_started={result.runs_started}")
    print(f"skipped_overlap_ticks={result.skipped_overlap_ticks}")
    print(f"skipped_active_run_ticks={result.skipped_active_run_ticks}")
    print(f"succeeded_runs={result.succeeded_runs}")
    print(
        "completed_with_detail_errors_runs="
        f"{result.completed_with_detail_errors_runs}"
    )
    print(f"completed_with_unresolved_runs={result.completed_with_unresolved_runs}")
    print(f"failed_runs={result.failed_runs}")
    print(f"last_tick_status={result.last_tick_status or '-'}")
    print(f"last_run_id={result.last_run_id or '-'}")


def _trigger_exit_code(result: TriggerRunNowResult) -> int:
    if result.status in {
        "succeeded",
        "completed_with_detail_errors",
        TRIGGER_RUN_NOW_STATUS_SKIPPED_OVERLAP,
        TRIGGER_RUN_NOW_STATUS_SKIPPED_ACTIVE_RUN,
    }:
        return 0
    return 1

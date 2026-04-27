from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import cast

from hhru_platform.application.commands.export_retention_archive import (
    ExportRetentionArchiveCommand,
    ExportRetentionArchiveResult,
    export_retention_archive,
)
from hhru_platform.application.commands.run_housekeeping import (
    HOUSEKEEPING_MODE_DRY_RUN,
    HousekeepingRetentionPolicy,
    RetentionArchiveStore,
    RunHousekeepingCommand,
    RunHousekeepingResult,
    run_housekeeping,
)
from hhru_platform.application.commands.sync_retention_archive_offsite import (
    SyncRetentionArchiveOffsiteCommand,
    SyncRetentionArchiveOffsiteResult,
    sync_retention_archive_offsite,
)
from hhru_platform.config.settings import Settings, get_settings
from hhru_platform.infrastructure.db.repositories import SqlAlchemyHousekeepingRepository
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.housekeeping import (
    LocalReportArtifactStore,
    LocalRetentionArchiveStore,
    LocalRetentionArchiveUploadReceiptStore,
    WebDavArchiveUploader,
)
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
    parser.add_argument(
        "--archive-before-delete",
        action="store_true",
        help=(
            "For raw_api_payload and vacancy_snapshot, export the selected retention rows "
            "into the local archive directory before deleting them."
        ),
    )
    parser.set_defaults(handler=handle_run_housekeeping)

    archive_parser = subparsers.add_parser(
        "export-retention-archive",
        help=(
            "Export current raw_api_payload and vacancy_snapshot retention candidates into "
            "compressed local archive chunks with sidecar manifests."
        ),
    )
    archive_parser.add_argument(
        "--triggered-by",
        default="export-retention-archive",
        help=(
            "Actor or subsystem that initiated archive export. "
            "Defaults to export-retention-archive."
        ),
    )
    archive_parser.set_defaults(handler=handle_export_retention_archive)

    offsite_parser = subparsers.add_parser(
        "sync-retention-archive-offsite",
        help=(
            "Upload local retention archive chunks plus manifests to off-host WebDAV "
            "storage and keep local upload receipts."
        ),
    )
    offsite_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of manifest bundles to inspect in one run.",
    )
    offsite_parser.add_argument(
        "--triggered-by",
        default="sync-retention-archive-offsite",
        help=(
            "Actor or subsystem that initiated off-host archive sync. "
            "Defaults to sync-retention-archive-offsite."
        ),
    )
    offsite_parser.set_defaults(handler=handle_sync_retention_archive_offsite)


def handle_run_housekeeping(args: argparse.Namespace) -> int:
    settings = get_settings()
    command = RunHousekeepingCommand(
        retention_policy=_build_housekeeping_retention_policy(settings),
        execute=bool(args.execute),
        archive_before_delete=bool(args.archive_before_delete),
        archive_dir=Path(settings.housekeeping_archive_dir),
        triggered_by=str(args.triggered_by),
    )

    try:
        with session_scope() as session:
            result = run_housekeeping(
                command,
                housekeeping_repository=SqlAlchemyHousekeepingRepository(session),
                report_artifact_store=LocalReportArtifactStore(),
                retention_archive_store=cast(
                    RetentionArchiveStore,
                    LocalRetentionArchiveStore(),
                ),
                metrics_recorder=get_metrics_registry(),
            )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_run_housekeeping_summary(result)
    return 0


def handle_export_retention_archive(args: argparse.Namespace) -> int:
    settings = get_settings()
    command = ExportRetentionArchiveCommand(
        retention_policy=_build_housekeeping_retention_policy(settings),
        archive_dir=Path(settings.housekeeping_archive_dir),
        triggered_by=str(args.triggered_by),
    )

    try:
        with session_scope() as session:
            result = export_retention_archive(
                command,
                retention_archive_repository=SqlAlchemyHousekeepingRepository(session),
                retention_archive_store=LocalRetentionArchiveStore(),
            )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_export_retention_archive_summary(result)
    return 0


def handle_sync_retention_archive_offsite(args: argparse.Namespace) -> int:
    settings = get_settings()
    command = SyncRetentionArchiveOffsiteCommand(
        archive_dir=Path(settings.housekeeping_archive_dir),
        offsite_url=settings.housekeeping_archive_offsite_url,
        offsite_root=settings.housekeeping_archive_offsite_root,
        username=settings.housekeeping_archive_offsite_username,
        password=settings.housekeeping_archive_offsite_password,
        bearer_token=settings.housekeeping_archive_offsite_bearer_token,
        timeout_seconds=settings.housekeeping_archive_offsite_timeout_seconds,
        limit=args.limit,
        triggered_by=str(args.triggered_by),
    )
    if command.auth_mode == "bearer":
        uploader = WebDavArchiveUploader.with_bearer_token(
            base_url=command.offsite_url,
            remote_root=command.offsite_root,
            bearer_token=str(command.bearer_token),
            timeout_seconds=command.timeout_seconds,
        )
    else:
        uploader = WebDavArchiveUploader.with_basic_auth(
            base_url=command.offsite_url,
            remote_root=command.offsite_root,
            username=str(command.username),
            password=str(command.password),
            timeout_seconds=command.timeout_seconds,
        )

    try:
        result = sync_retention_archive_offsite(
            command,
            offsite_uploader=uploader,
            receipt_store=LocalRetentionArchiveUploadReceiptStore(),
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_sync_retention_archive_offsite_summary(result)
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
    print(f"total_archived={result.total_archived}")
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
            f"archived_count={summary.archived_count} "
            f"archive_file={summary.archive_file or '-'} "
            f"manifest_file={summary.manifest_file or '-'} "
            f"archive_sha256={summary.archive_sha256 or '-'} "
            f"archive_size_bytes={summary.archive_size_bytes} "
            f"limited={'yes' if summary.limited else 'no'}"
        )


def _print_export_retention_archive_summary(result: ExportRetentionArchiveResult) -> None:
    print("completed retention archive export")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"evaluated_at={result.evaluated_at.isoformat()}")
    print(f"archive_dir={result.archive_dir}")
    print(f"total_candidates={result.total_candidates}")
    print(f"total_exported={result.total_exported}")
    for summary in result.summaries:
        print(
            "target="
            f"{summary.target} "
            f"enabled={'yes' if summary.enabled else 'no'} "
            f"retention_days={summary.retention_days} "
            f"cutoff={summary.cutoff.isoformat() if summary.cutoff is not None else '-'} "
            f"candidate_count={summary.candidate_count} "
            f"exported_count={summary.exported_count} "
            f"archive_size_bytes={summary.archive_size_bytes} "
            f"archive_sha256={summary.archive_sha256 or '-'} "
            f"archive_file={summary.archive_file or '-'} "
            f"manifest_file={summary.manifest_file or '-'} "
            f"limited={'yes' if summary.limited else 'no'}"
        )


def _print_sync_retention_archive_offsite_summary(
    result: SyncRetentionArchiveOffsiteResult,
) -> None:
    print("completed retention archive offsite sync")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"synced_at={result.synced_at.isoformat()}")
    print(f"archive_dir={result.archive_dir}")
    print(f"offsite_url={result.offsite_url}")
    print(f"offsite_root={result.offsite_root}")
    print(f"auth_mode={result.auth_mode}")
    print(f"scanned_manifest_count={result.scanned_manifest_count}")
    print(f"candidate_bundle_count={result.candidate_bundle_count}")
    print(f"uploaded_bundle_count={result.uploaded_bundle_count}")
    print(f"skipped_bundle_count={result.skipped_bundle_count}")
    for summary in result.summaries:
        print(
            "manifest_file="
            f"{summary.manifest_file} "
            f"archive_file={summary.archive_file} "
            f"uploaded={'yes' if summary.uploaded else 'no'} "
            f"skipped={'yes' if summary.skipped else 'no'} "
            f"remote_archive_path={summary.remote_archive_path} "
            f"remote_manifest_path={summary.remote_manifest_path} "
            f"archive_sha256={summary.archive_sha256} "
            f"manifest_sha256={summary.manifest_sha256} "
            f"receipt_file={summary.receipt_file or '-'}"
        )


def _build_housekeeping_retention_policy(settings: Settings) -> HousekeepingRetentionPolicy:
    return HousekeepingRetentionPolicy(
        raw_api_payload_retention_days=settings.housekeeping_raw_api_payload_retention_days,
        vacancy_snapshot_retention_days=settings.housekeeping_vacancy_snapshot_retention_days,
        finished_crawl_run_retention_days=(
            settings.housekeeping_finished_crawl_run_retention_days
        ),
        detail_fetch_attempt_retention_days=(
            settings.housekeeping_detail_fetch_attempt_retention_days
        ),
        report_artifact_retention_days=settings.housekeeping_report_artifact_retention_days,
        report_artifact_dir=Path(settings.housekeeping_report_artifact_dir),
        delete_limit_per_target=settings.housekeeping_delete_limit_per_target,
    )

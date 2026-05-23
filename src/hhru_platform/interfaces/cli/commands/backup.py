from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hhru_platform.application.commands.run_backup import RunBackupCommand, run_backup
from hhru_platform.application.commands.run_restore_drill import (
    RunRestoreDrillCommand,
    run_restore_drill,
)
from hhru_platform.application.commands.sync_backup_offsite import (
    BackupOffsiteUploader,
    SyncBackupOffsiteCommand,
    SyncBackupOffsiteResult,
    sync_backup_offsite,
)
from hhru_platform.config.settings import Settings, get_settings
from hhru_platform.infrastructure.backup import (
    BackupService,
    LocalBackupOffsiteUploadReceiptStore,
    S3BackupOffsiteUploader,
)
from hhru_platform.infrastructure.housekeeping import WebDavArchiveUploader
from hhru_platform.infrastructure.observability.metrics import get_metrics_registry


def register_backup_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    backup_parser = subparsers.add_parser(
        "run-backup",
        help="Create a PostgreSQL backup, verify the archive, and publish lifecycle metrics.",
    )
    backup_parser.add_argument(
        "--triggered-by",
        default="run-backup",
        help="Actor or subsystem that initiated the backup. Defaults to run-backup.",
    )
    backup_parser.set_defaults(handler=handle_run_backup)

    verify_parser = subparsers.add_parser(
        "verify-backup-file",
        help="Inspect an existing PostgreSQL backup archive and print verification details.",
    )
    verify_parser.add_argument(
        "--backup-file",
        type=Path,
        required=True,
        help="Path to the backup archive file to inspect.",
    )
    verify_parser.set_defaults(handler=handle_verify_backup_file)

    restore_parser = subparsers.add_parser(
        "run-restore-drill",
        help=(
            "Restore a backup into a separate target database, recreate it if requested, "
            "and verify core tables exist after restore."
        ),
    )
    restore_parser.add_argument(
        "--backup-file",
        type=Path,
        required=True,
        help="Path to the backup archive file to restore.",
    )
    restore_parser.add_argument(
        "--target-db",
        default=None,
        help=(
            "Target database name for the drill restore. Defaults to "
            "HHRU_BACKUP_RESTORE_DRILL_TARGET_DB."
        ),
    )
    restore_parser.add_argument(
        "--drop-target-db",
        choices=("yes", "no"),
        default=None,
        help=(
            "Recreate the target database before restore. Defaults to "
            "HHRU_BACKUP_RESTORE_DRILL_DROP_EXISTING."
        ),
    )
    restore_parser.add_argument(
        "--triggered-by",
        default="run-restore-drill",
        help="Actor or subsystem that initiated the restore drill. Defaults to run-restore-drill.",
    )
    restore_parser.set_defaults(handler=handle_run_restore_drill)

    offsite_parser = subparsers.add_parser(
        "sync-backup-offsite",
        help=(
            "Upload recent PostgreSQL backup dumps plus manifests to off-host "
            "storage and keep local upload receipts."
        ),
    )
    offsite_parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Maximum latest backup dumps to inspect in one run. Defaults to 1.",
    )
    offsite_parser.add_argument(
        "--triggered-by",
        default="sync-backup-offsite",
        help=(
            "Actor or subsystem that initiated off-host backup sync. "
            "Defaults to sync-backup-offsite."
        ),
    )
    offsite_parser.add_argument(
        "--chunk-size-bytes",
        type=int,
        default=None,
        help=(
            "Split dump uploads into fixed-size parts. Defaults to "
            "HHRU_BACKUP_OFFSITE_CHUNK_SIZE_BYTES."
        ),
    )
    offsite_parser.set_defaults(handler=handle_sync_backup_offsite)


def handle_run_backup(args: argparse.Namespace) -> int:
    try:
        result = run_backup(
            RunBackupCommand(triggered_by=str(args.triggered_by)),
            backup_service=BackupService(),
            metrics_recorder=get_metrics_registry(),
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    print("completed backup")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"recorded_at={result.recorded_at.isoformat()}")
    print(f"backup_file={result.backup_file}")
    print(f"backup_size_bytes={result.backup_size_bytes}")
    print(f"backup_sha256={result.backup_sha256}")
    print(f"archive_entry_count={result.archive_entry_count}")
    return 0


def handle_verify_backup_file(args: argparse.Namespace) -> int:
    try:
        summary = BackupService().inspect_backup_file(args.backup_file)
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    print("verified backup file")
    print(f"backup_file={summary.backup_file}")
    print(f"backup_size_bytes={summary.size_bytes}")
    print(f"backup_sha256={summary.sha256}")
    print(f"archive_entry_count={summary.archive_entry_count}")
    return 0


def handle_run_restore_drill(args: argparse.Namespace) -> int:
    settings = get_settings()
    target_db = str(args.target_db or settings.backup_restore_drill_target_db)
    drop_target_db = (
        settings.backup_restore_drill_drop_existing
        if args.drop_target_db is None
        else args.drop_target_db == "yes"
    )
    try:
        result = run_restore_drill(
            RunRestoreDrillCommand(
                backup_file=Path(args.backup_file),
                target_db=target_db,
                drop_target_db=drop_target_db,
                triggered_by=str(args.triggered_by),
            ),
            backup_service=BackupService(settings),
            metrics_recorder=get_metrics_registry(),
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    print("completed restore drill")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"recorded_at={result.recorded_at.isoformat()}")
    print(f"backup_file={result.backup_file}")
    print(f"target_db={result.target_db}")
    print(f"archive_entry_count={result.archive_entry_count}")
    print(f"schema_verified={'yes' if result.schema_verified else 'no'}")
    print(
        "verified_tables="
        f"{result.verified_tables_count}/{len(result.checked_tables)}"
    )
    print(f"checked_tables={','.join(result.checked_tables)}")
    return 0


def handle_sync_backup_offsite(args: argparse.Namespace) -> int:
    settings = get_settings()
    try:
        command, uploader = _build_backup_offsite_command_and_uploader(
            args=args,
            settings=settings,
        )
        result = sync_backup_offsite(
            command,
            offsite_uploader=uploader,
            receipt_store=LocalBackupOffsiteUploadReceiptStore(),
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_sync_backup_offsite_summary(result)
    return 0


def _build_backup_offsite_command_and_uploader(
    *,
    args: argparse.Namespace,
    settings: Settings,
) -> tuple[SyncBackupOffsiteCommand, BackupOffsiteUploader]:
    backend = settings.backup_offsite_backend.strip().lower()
    chunk_size_bytes = (
        args.chunk_size_bytes
        if args.chunk_size_bytes is not None
        else settings.backup_offsite_chunk_size_bytes
    )
    if backend == "s3":
        endpoint_url = settings.backup_offsite_s3_endpoint_url.strip()
        bucket = settings.backup_offsite_s3_bucket.strip()
        access_key_id = settings.backup_offsite_s3_access_key_id or ""
        secret_access_key = settings.backup_offsite_s3_secret_access_key or ""
        if not endpoint_url:
            raise ValueError("HHRU_BACKUP_OFFSITE_S3_ENDPOINT_URL must not be empty")
        if not bucket:
            raise ValueError("HHRU_BACKUP_OFFSITE_S3_BUCKET must not be empty")
        if not access_key_id or not secret_access_key:
            raise ValueError(
                "HHRU_BACKUP_OFFSITE_S3_ACCESS_KEY_ID and "
                "HHRU_BACKUP_OFFSITE_S3_SECRET_ACCESS_KEY must be configured"
            )
        command = SyncBackupOffsiteCommand(
            backup_dir=Path(settings.backup_dir),
            offsite_url=_s3_offsite_url(endpoint_url=endpoint_url, bucket=bucket),
            offsite_root=settings.backup_offsite_root,
            username=access_key_id,
            password=secret_access_key,
            auth_mode_label="s3",
            timeout_seconds=settings.backup_offsite_timeout_seconds,
            chunk_size_bytes=chunk_size_bytes,
            limit=args.limit,
            triggered_by=str(args.triggered_by),
        )
        s3_uploader = S3BackupOffsiteUploader.with_credentials(
            endpoint_url=endpoint_url,
            bucket=bucket,
            key_prefix=command.offsite_root,
            region_name=settings.backup_offsite_s3_region,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        )
        return command, s3_uploader

    if backend != "webdav":
        raise ValueError("HHRU_BACKUP_OFFSITE_BACKEND must be either webdav or s3")

    command = SyncBackupOffsiteCommand(
        backup_dir=Path(settings.backup_dir),
        offsite_url=settings.backup_offsite_url
        or settings.housekeeping_archive_offsite_url,
        offsite_root=settings.backup_offsite_root,
        username=settings.backup_offsite_username
        or settings.housekeeping_archive_offsite_username,
        password=settings.backup_offsite_password
        or settings.housekeeping_archive_offsite_password,
        bearer_token=settings.backup_offsite_bearer_token
        or settings.housekeeping_archive_offsite_bearer_token,
        timeout_seconds=settings.backup_offsite_timeout_seconds,
        chunk_size_bytes=chunk_size_bytes,
        limit=args.limit,
        triggered_by=str(args.triggered_by),
    )
    if command.auth_mode == "bearer":
        webdav_uploader = WebDavArchiveUploader.with_bearer_token(
            base_url=command.offsite_url,
            remote_root=command.offsite_root,
            bearer_token=str(command.bearer_token),
            timeout_seconds=command.timeout_seconds,
        )
    else:
        webdav_uploader = WebDavArchiveUploader.with_basic_auth(
            base_url=command.offsite_url,
            remote_root=command.offsite_root,
            username=str(command.username),
            password=str(command.password),
            timeout_seconds=command.timeout_seconds,
        )
    return command, webdav_uploader


def _s3_offsite_url(*, endpoint_url: str, bucket: str) -> str:
    return f"{endpoint_url.strip().rstrip('/')}/{bucket.strip()}"


def _print_sync_backup_offsite_summary(result: SyncBackupOffsiteResult) -> None:
    print("completed backup offsite sync")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"synced_at={result.synced_at.isoformat()}")
    print(f"backup_dir={result.backup_dir}")
    print(f"offsite_url={result.offsite_url}")
    print(f"offsite_root={result.offsite_root}")
    print(f"auth_mode={result.auth_mode}")
    print(f"limit={result.limit or 0}")
    print(f"scanned_backup_count={result.scanned_backup_count}")
    print(f"candidate_backup_count={result.candidate_backup_count}")
    print(f"uploaded_backup_count={result.uploaded_backup_count}")
    print(f"skipped_backup_count={result.skipped_backup_count}")
    for summary in result.summaries:
        print(
            "backup="
            f"{summary.backup_file} "
            f"backup_size_bytes={summary.backup_size_bytes} "
            f"backup_sha256={summary.backup_sha256} "
            f"manifest_file={summary.manifest_file} "
            f"manifest_sha256={summary.manifest_sha256} "
            f"chunk_size_bytes={summary.chunk_size_bytes} "
            f"part_count={summary.part_count} "
            f"remote_backup_path={summary.remote_backup_path} "
            f"remote_manifest_path={summary.remote_manifest_path} "
            f"uploaded={'yes' if summary.uploaded else 'no'} "
            f"skipped={'yes' if summary.skipped else 'no'} "
            f"receipt_file={summary.receipt_file or '-'}"
        )

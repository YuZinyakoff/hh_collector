from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hhru_platform.application.commands.run_backup import RunBackupCommand, run_backup
from hhru_platform.application.commands.run_restore_drill import (
    RunRestoreDrillCommand,
    run_restore_drill,
)
from hhru_platform.config.settings import get_settings
from hhru_platform.infrastructure.backup import BackupService
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

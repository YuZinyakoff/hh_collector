from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from hhru_platform.application.commands.audit_research_archive_coverage import (
    AuditResearchArchiveCoverageCommand,
    AuditResearchArchiveCoverageResult,
    audit_research_archive_coverage,
)
from hhru_platform.application.commands.export_research_archive import (
    DEFAULT_RESEARCH_ARCHIVE_DATASETS,
    INCREMENTAL_RESEARCH_ARCHIVE_DATASETS,
    SUPPORTED_RESEARCH_ARCHIVE_DATASETS,
    ExportResearchArchiveCommand,
    ExportResearchArchiveResult,
    export_research_archive,
)
from hhru_platform.application.commands.preview_research_archive_housekeeping import (
    PreviewResearchArchiveHousekeepingCommand,
    PreviewResearchArchiveHousekeepingResult,
    preview_research_archive_housekeeping,
)
from hhru_platform.application.commands.sync_research_archive_offsite import (
    SyncResearchArchiveOffsiteCommand,
    SyncResearchArchiveOffsiteResult,
    sync_research_archive_offsite,
)
from hhru_platform.application.commands.verify_research_archive import (
    VerifyResearchArchiveCommand,
    VerifyResearchArchiveResult,
    verify_research_archive,
)
from hhru_platform.application.commands.verify_research_archive_offsite import (
    VerifyResearchArchiveOffsiteCommand,
    VerifyResearchArchiveOffsiteResult,
    verify_research_archive_offsite,
)
from hhru_platform.config.settings import Settings, get_settings
from hhru_platform.infrastructure.backup.s3_backup_offsite_uploader import (
    S3BackupOffsiteUploader,
)
from hhru_platform.infrastructure.db.repositories.housekeeping_repo import (
    SqlAlchemyHousekeepingRepository,
)
from hhru_platform.infrastructure.db.repositories.research_archive_repo import (
    SqlAlchemyResearchArchiveRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.research_archive import (
    LocalResearchArchiveCheckpointStore,
    LocalResearchArchiveCheckpointVerificationReceiptStore,
    LocalResearchArchiveCursorStore,
    LocalResearchArchiveOffsiteUploadReceiptStore,
    LocalResearchArchiveOffsiteVerificationReceiptStore,
    LocalResearchArchiveStore,
    ResearchArchiveManifestVerifier,
)


def register_research_archive_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    export_parser = subparsers.add_parser(
        "export-research-archive",
        help="Export Archive v1 datasets into local jsonl.gz chunks with manifests.",
    )
    export_parser.add_argument(
        "--archive-dir",
        type=Path,
        help="Archive root directory. Defaults to HHRU_RESEARCH_ARCHIVE_DIR.",
    )
    export_parser.add_argument(
        "--dataset",
        action="append",
        choices=SUPPORTED_RESEARCH_ARCHIVE_DATASETS,
        help=(
            "Dataset key to export. Can be repeated. Defaults to all Archive v1 "
            "foundation datasets."
        ),
    )
    export_parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Maximum rows per archive chunk. Defaults to 100000.",
    )
    export_parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="PostgreSQL streaming batch size. Defaults to 10000.",
    )
    export_parser.add_argument(
        "--limit-per-dataset",
        type=int,
        help="Optional row limit per dataset for tool validation runs.",
    )
    export_parser.add_argument(
        "--archive-kind",
        default="tool_validation",
        help="Archive label, for example tool_validation, pilot_evidence or production.",
    )
    export_parser.add_argument(
        "--incremental",
        action="store_true",
        help=(
            "Export only the settled append-only source-id suffix recorded after local "
            "production manifests."
        ),
    )
    export_parser.add_argument(
        "--settled-delay-hours",
        type=_non_negative_float,
        default=24.0,
        help="Safety delay for incremental exports. Defaults to 24 hours.",
    )
    export_parser.add_argument(
        "--triggered-by",
        default="export-research-archive",
        help="Actor or subsystem that initiated export.",
    )
    export_parser.set_defaults(handler=handle_export_research_archive)

    verify_parser = subparsers.add_parser(
        "verify-research-archive",
        help="Verify local Archive v1 manifests, gzip chunks, checksums and inventory.",
    )
    verify_parser.add_argument(
        "--archive-dir",
        type=Path,
        help="Archive root directory. Defaults to HHRU_RESEARCH_ARCHIVE_DIR.",
    )
    verify_parser.add_argument(
        "--manifest-file",
        type=Path,
        action="append",
        help="Specific manifest to verify. Can be repeated. Defaults to all manifests.",
    )
    verify_parser.add_argument(
        "--limit",
        type=int,
        help="Optional manifest count limit for a quick smoke check.",
    )
    verify_parser.add_argument(
        "--triggered-by",
        default="verify-research-archive",
        help="Actor or subsystem that initiated verification.",
    )
    verify_parser.set_defaults(handler=handle_verify_research_archive)

    sync_offsite_parser = subparsers.add_parser(
        "sync-research-archive-offsite",
        help=(
            "Upload local Archive v1 chunks, manifests, inventory and checkpoints "
            "to S3 offsite storage."
        ),
    )
    sync_offsite_parser.add_argument(
        "--archive-dir",
        type=Path,
        help="Archive root directory. Defaults to HHRU_RESEARCH_ARCHIVE_DIR.",
    )
    sync_offsite_parser.add_argument(
        "--manifest-file",
        type=Path,
        action="append",
        help="Specific manifest to upload. Can be repeated. Defaults to all manifests.",
    )
    sync_offsite_parser.add_argument(
        "--limit",
        type=int,
        help="Optional manifest count limit for a quick smoke upload.",
    )
    sync_offsite_parser.add_argument(
        "--triggered-by",
        default="sync-research-archive-offsite",
        help="Actor or subsystem that initiated offsite sync.",
    )
    sync_offsite_parser.set_defaults(handler=handle_sync_research_archive_offsite)

    verify_offsite_parser = subparsers.add_parser(
        "verify-research-archive-offsite",
        help="Verify Archive v1 objects in S3 and read back a small sample.",
    )
    verify_offsite_parser.add_argument(
        "--archive-dir",
        type=Path,
        help="Archive root directory. Defaults to HHRU_RESEARCH_ARCHIVE_DIR.",
    )
    verify_offsite_parser.add_argument(
        "--manifest-file",
        type=Path,
        action="append",
        help="Specific manifest to verify. Can be repeated. Defaults to all manifests.",
    )
    verify_offsite_parser.add_argument(
        "--limit",
        type=int,
        help="Optional manifest count limit for a quick remote smoke check.",
    )
    verify_offsite_parser.add_argument(
        "--readback-limit",
        type=int,
        default=1,
        help="Number of data chunks to download and checksum/parse. Defaults to 1.",
    )
    verify_offsite_parser.add_argument(
        "--triggered-by",
        default="verify-research-archive-offsite",
        help="Actor or subsystem that initiated offsite verification.",
    )
    verify_offsite_parser.set_defaults(handler=handle_verify_research_archive_offsite)

    audit_coverage_parser = subparsers.add_parser(
        "audit-research-archive-coverage",
        help="Audit verified checkpoint coverage for append-only Archive v1 datasets.",
    )
    audit_coverage_parser.add_argument(
        "--archive-dir",
        type=Path,
        help="Archive root directory. Defaults to HHRU_RESEARCH_ARCHIVE_DIR.",
    )
    audit_coverage_parser.add_argument(
        "--archive-kind",
        default="production",
        help="Archive label to audit. Defaults to production.",
    )
    audit_coverage_parser.add_argument(
        "--dataset",
        action="append",
        choices=INCREMENTAL_RESEARCH_ARCHIVE_DATASETS,
        help="Append-only dataset to audit. Can be repeated. Defaults to all.",
    )
    audit_coverage_parser.add_argument(
        "--triggered-by",
        default="audit-research-archive-coverage",
        help="Actor or subsystem that initiated the audit.",
    )
    audit_coverage_parser.set_defaults(handler=handle_audit_research_archive_coverage)

    housekeeping_preview_parser = subparsers.add_parser(
        "preview-research-archive-housekeeping",
        help=(
            "Preview raw payload and vacancy snapshot retention candidates bounded "
            "by complete verified Archive v1 coverage."
        ),
    )
    housekeeping_preview_parser.add_argument(
        "--archive-dir",
        type=Path,
        help="Archive root directory. Defaults to HHRU_RESEARCH_ARCHIVE_DIR.",
    )
    housekeeping_preview_parser.add_argument(
        "--archive-kind",
        default="production",
        help="Archive label to audit before planning. Defaults to production.",
    )
    housekeeping_preview_parser.add_argument(
        "--raw-api-payload-retention-days",
        type=_non_negative_int,
        help="Override HHRU_HOUSEKEEPING_RAW_API_PAYLOAD_RETENTION_DAYS.",
    )
    housekeeping_preview_parser.add_argument(
        "--vacancy-snapshot-retention-days",
        type=_non_negative_int,
        help="Override HHRU_HOUSEKEEPING_VACANCY_SNAPSHOT_RETENTION_DAYS.",
    )
    housekeeping_preview_parser.add_argument(
        "--finished-crawl-run-retention-days",
        type=_non_negative_int,
        help="Override HHRU_HOUSEKEEPING_FINISHED_CRAWL_RUN_RETENTION_DAYS.",
    )
    housekeeping_preview_parser.add_argument(
        "--delete-limit-per-target",
        type=_positive_int,
        help="Override HHRU_HOUSEKEEPING_DELETE_LIMIT_PER_TARGET.",
    )
    housekeeping_preview_parser.add_argument(
        "--triggered-by",
        default="preview-research-archive-housekeeping",
        help="Actor or subsystem that initiated the preview.",
    )
    housekeeping_preview_parser.set_defaults(
        handler=handle_preview_research_archive_housekeeping
    )


def handle_export_research_archive(args: argparse.Namespace) -> int:
    settings = get_settings()
    incremental = bool(args.incremental)
    settled_before = (
        datetime.now(UTC) - timedelta(hours=float(args.settled_delay_hours))
        if incremental
        else None
    )
    command = ExportResearchArchiveCommand(
        archive_dir=Path(args.archive_dir or settings.research_archive_dir),
        datasets=tuple(
            args.dataset
            or (
                INCREMENTAL_RESEARCH_ARCHIVE_DATASETS
                if incremental
                else DEFAULT_RESEARCH_ARCHIVE_DATASETS
            )
        ),
        chunk_size=int(args.chunk_size),
        batch_size=int(args.batch_size),
        limit_per_dataset=args.limit_per_dataset,
        archive_kind=str(args.archive_kind),
        triggered_by=str(args.triggered_by),
        source_database=settings.db_name,
        source_git_revision=_git_revision(),
        source_command=_source_command(),
        incremental=incremental,
        settled_before=settled_before,
    )

    try:
        with session_scope() as session:
            result = export_research_archive(
                command,
                research_archive_repository=SqlAlchemyResearchArchiveRepository(session),
                research_archive_store=LocalResearchArchiveStore(),
                research_archive_cursor_store=LocalResearchArchiveCursorStore(),
                research_archive_checkpoint_store=LocalResearchArchiveCheckpointStore(),
            )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_export_result(result)
    return 0


def handle_verify_research_archive(args: argparse.Namespace) -> int:
    settings = get_settings()
    command = VerifyResearchArchiveCommand(
        archive_dir=Path(args.archive_dir or settings.research_archive_dir),
        manifest_files=tuple(args.manifest_file or ()),
        limit=args.limit,
        triggered_by=str(args.triggered_by),
    )

    try:
        result = verify_research_archive(
            command,
            manifest_verifier=ResearchArchiveManifestVerifier(),
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_verify_result(result)
    return 0


def handle_sync_research_archive_offsite(args: argparse.Namespace) -> int:
    settings = get_settings()
    try:
        command, uploader = _build_research_archive_offsite_command_and_store(
            args=args,
            settings=settings,
        )
        result = sync_research_archive_offsite(
            command,
            offsite_uploader=uploader,
            receipt_store=LocalResearchArchiveOffsiteUploadReceiptStore(),
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_sync_offsite_result(result)
    return 0


def handle_verify_research_archive_offsite(args: argparse.Namespace) -> int:
    settings = get_settings()
    try:
        command, remote_store = _build_verify_research_archive_offsite_command_and_store(
            args=args,
            settings=settings,
        )
        result = verify_research_archive_offsite(
            command,
            remote_store=remote_store,
            receipt_store=LocalResearchArchiveOffsiteVerificationReceiptStore(),
            checkpoint_receipt_store=LocalResearchArchiveCheckpointVerificationReceiptStore(),
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_verify_offsite_result(result)
    return 0


def handle_audit_research_archive_coverage(args: argparse.Namespace) -> int:
    settings = get_settings()
    try:
        _ensure_research_archive_s3_backend(settings)
        command = AuditResearchArchiveCoverageCommand(
            archive_dir=Path(args.archive_dir or settings.research_archive_dir),
            archive_kind=str(args.archive_kind),
            datasets=tuple(args.dataset or INCREMENTAL_RESEARCH_ARCHIVE_DATASETS),
            offsite_url=_s3_offsite_url(
                endpoint_url=_research_archive_s3_endpoint_url(settings),
                bucket=_research_archive_s3_bucket(settings),
            ),
            offsite_root=settings.research_archive_offsite_root,
            triggered_by=str(args.triggered_by),
        )
        result = audit_research_archive_coverage(
            command,
            checkpoint_store=LocalResearchArchiveCheckpointStore(),
            receipt_store=LocalResearchArchiveOffsiteVerificationReceiptStore(),
            checkpoint_receipt_store=LocalResearchArchiveCheckpointVerificationReceiptStore(),
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_audit_coverage_result(result)
    return 0 if result.complete else 1


def handle_preview_research_archive_housekeeping(args: argparse.Namespace) -> int:
    settings = get_settings()
    try:
        _ensure_research_archive_s3_backend(settings)
        command = PreviewResearchArchiveHousekeepingCommand(
            archive_dir=Path(args.archive_dir or settings.research_archive_dir),
            archive_kind=str(args.archive_kind),
            offsite_url=_s3_offsite_url(
                endpoint_url=_research_archive_s3_endpoint_url(settings),
                bucket=_research_archive_s3_bucket(settings),
            ),
            offsite_root=settings.research_archive_offsite_root,
            raw_api_payload_retention_days=(
                args.raw_api_payload_retention_days
                if args.raw_api_payload_retention_days is not None
                else settings.housekeeping_raw_api_payload_retention_days
            ),
            vacancy_snapshot_retention_days=(
                args.vacancy_snapshot_retention_days
                if args.vacancy_snapshot_retention_days is not None
                else settings.housekeeping_vacancy_snapshot_retention_days
            ),
            finished_crawl_run_retention_days=(
                args.finished_crawl_run_retention_days
                if args.finished_crawl_run_retention_days is not None
                else settings.housekeeping_finished_crawl_run_retention_days
            ),
            delete_limit_per_target=(
                args.delete_limit_per_target
                if args.delete_limit_per_target is not None
                else settings.housekeeping_delete_limit_per_target
            ),
            triggered_by=str(args.triggered_by),
        )
        with session_scope() as session:
            result = preview_research_archive_housekeeping(
                command,
                housekeeping_repository=SqlAlchemyHousekeepingRepository(session),
                checkpoint_store=LocalResearchArchiveCheckpointStore(),
                receipt_store=LocalResearchArchiveOffsiteVerificationReceiptStore(),
                checkpoint_receipt_store=(
                    LocalResearchArchiveCheckpointVerificationReceiptStore()
                ),
            )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_housekeeping_preview_result(result)
    return 0 if result.ready else 1


def _print_export_result(result: ExportResearchArchiveResult) -> None:
    print("completed research archive export")
    print(f"status={result.status}")
    print(f"schema_version={result.schema_version}")
    print(f"archive_kind={result.archive_kind}")
    print(f"incremental={'yes' if result.incremental else 'no'}")
    print(f"settled_before={result.settled_before.isoformat() if result.settled_before else '-'}")
    print(f"triggered_by={result.triggered_by}")
    print(f"archive_dir={result.archive_dir}")
    print(f"created_at={result.created_at.isoformat()}")
    print(f"checkpoint_file={result.checkpoint_file or '-'}")
    print(f"total_chunk_count={result.total_chunk_count}")
    print(f"total_row_count={result.total_row_count}")
    print(f"total_data_size_bytes={result.total_data_size_bytes}")
    for summary in result.summaries:
        source_id_before = (
            summary.source_id_before if summary.source_id_before is not None else "-"
        )
        source_id_after = summary.source_id_after if summary.source_id_after is not None else "-"
        print(
            "dataset_summary "
            f"dataset={summary.dataset} "
            f"chunk_count={summary.chunk_count} "
            f"row_count={summary.row_count} "
            f"data_size_bytes={summary.data_size_bytes} "
            f"source_id_before={source_id_before} "
            f"source_id_after={source_id_after}"
        )


def _print_verify_result(result: VerifyResearchArchiveResult) -> None:
    print("completed research archive verification")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"archive_dir={result.archive_dir}")
    print(f"scanned_manifest_count={result.scanned_manifest_count}")
    print(f"verified_manifest_count={result.verified_manifest_count}")
    print(f"total_row_count={result.total_row_count}")
    print(f"total_data_size_bytes={result.total_data_size_bytes}")
    for summary in result.summaries:
        print(
            "manifest_summary "
            f"dataset={summary.dataset} "
            f"layer={summary.layer} "
            f"row_count={summary.row_count} "
            f"data_size_bytes={summary.data_size_bytes} "
            f"verified={'yes' if summary.verified else 'no'} "
            f"manifest_file={summary.manifest_file}"
        )


def _build_research_archive_offsite_command_and_store(
    *,
    args: argparse.Namespace,
    settings: Settings,
) -> tuple[SyncResearchArchiveOffsiteCommand, S3BackupOffsiteUploader]:
    _ensure_research_archive_s3_backend(settings)
    endpoint_url = _research_archive_s3_endpoint_url(settings)
    bucket = _research_archive_s3_bucket(settings)
    region_name = _research_archive_s3_region(settings)
    access_key_id = _research_archive_s3_access_key_id(settings)
    secret_access_key = _research_archive_s3_secret_access_key(settings)
    offsite_url = _s3_offsite_url(endpoint_url=endpoint_url, bucket=bucket)
    command = SyncResearchArchiveOffsiteCommand(
        archive_dir=Path(args.archive_dir or settings.research_archive_dir),
        manifest_files=tuple(args.manifest_file or ()),
        limit=args.limit,
        offsite_url=offsite_url,
        offsite_root=settings.research_archive_offsite_root,
        triggered_by=str(args.triggered_by),
    )
    uploader = S3BackupOffsiteUploader.with_credentials(
        endpoint_url=endpoint_url,
        bucket=bucket,
        key_prefix=command.offsite_root,
        region_name=region_name,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )
    return command, uploader


def _build_verify_research_archive_offsite_command_and_store(
    *,
    args: argparse.Namespace,
    settings: Settings,
) -> tuple[VerifyResearchArchiveOffsiteCommand, S3BackupOffsiteUploader]:
    _ensure_research_archive_s3_backend(settings)
    endpoint_url = _research_archive_s3_endpoint_url(settings)
    bucket = _research_archive_s3_bucket(settings)
    region_name = _research_archive_s3_region(settings)
    access_key_id = _research_archive_s3_access_key_id(settings)
    secret_access_key = _research_archive_s3_secret_access_key(settings)
    offsite_url = _s3_offsite_url(endpoint_url=endpoint_url, bucket=bucket)
    command = VerifyResearchArchiveOffsiteCommand(
        archive_dir=Path(args.archive_dir or settings.research_archive_dir),
        manifest_files=tuple(args.manifest_file or ()),
        limit=args.limit,
        readback_limit=int(args.readback_limit),
        offsite_url=offsite_url,
        offsite_root=settings.research_archive_offsite_root,
        triggered_by=str(args.triggered_by),
    )
    remote_store = S3BackupOffsiteUploader.with_credentials(
        endpoint_url=endpoint_url,
        bucket=bucket,
        key_prefix=command.offsite_root,
        region_name=region_name,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )
    return command, remote_store


def _ensure_research_archive_s3_backend(settings: Settings) -> None:
    backend = settings.research_archive_offsite_backend.strip().lower()
    if backend != "s3":
        raise ValueError("research archive offsite currently supports only S3 backend")
    if not _research_archive_s3_endpoint_url(settings):
        raise ValueError(
            "HHRU_RESEARCH_ARCHIVE_OFFSITE_S3_ENDPOINT_URL or "
            "HHRU_BACKUP_OFFSITE_S3_ENDPOINT_URL must be configured"
        )
    if not _research_archive_s3_bucket(settings):
        raise ValueError(
            "HHRU_RESEARCH_ARCHIVE_OFFSITE_S3_BUCKET or "
            "HHRU_BACKUP_OFFSITE_S3_BUCKET must be configured"
        )
    if not (
        _research_archive_s3_access_key_id(settings)
        and _research_archive_s3_secret_access_key(settings)
    ):
        raise ValueError(
            "research archive S3 access key and secret key must be configured "
            "directly or through HHRU_BACKUP_OFFSITE_S3_*"
        )


def _research_archive_s3_endpoint_url(settings: Settings) -> str:
    return (
        settings.research_archive_offsite_s3_endpoint_url.strip()
        or settings.backup_offsite_s3_endpoint_url.strip()
    )


def _research_archive_s3_bucket(settings: Settings) -> str:
    return (
        settings.research_archive_offsite_s3_bucket.strip()
        or settings.backup_offsite_s3_bucket.strip()
    )


def _research_archive_s3_region(settings: Settings) -> str:
    return (
        settings.research_archive_offsite_s3_region.strip()
        or settings.backup_offsite_s3_region.strip()
        or "ru-1"
    )


def _research_archive_s3_access_key_id(settings: Settings) -> str:
    return (settings.research_archive_offsite_s3_access_key_id or "").strip() or (
        settings.backup_offsite_s3_access_key_id or ""
    ).strip()


def _research_archive_s3_secret_access_key(settings: Settings) -> str:
    return (settings.research_archive_offsite_s3_secret_access_key or "").strip() or (
        settings.backup_offsite_s3_secret_access_key or ""
    ).strip()


def _s3_offsite_url(*, endpoint_url: str, bucket: str) -> str:
    return f"{endpoint_url.strip().rstrip('/')}/{bucket.strip()}"


def _print_sync_offsite_result(result: SyncResearchArchiveOffsiteResult) -> None:
    print("completed research archive offsite sync")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"synced_at={result.synced_at.isoformat()}")
    print(f"archive_dir={result.archive_dir}")
    print(f"offsite_url={result.offsite_url}")
    print(f"offsite_root={result.offsite_root}")
    print(f"limit={result.limit or 0}")
    print(f"scanned_manifest_count={result.scanned_manifest_count}")
    print(f"candidate_manifest_count={result.candidate_manifest_count}")
    print(f"uploaded_manifest_count={result.uploaded_manifest_count}")
    print(f"skipped_manifest_count={result.skipped_manifest_count}")
    print(f"inventory_file={result.inventory_file or '-'}")
    print(f"remote_inventory_path={result.remote_inventory_path or '-'}")
    print(f"inventory_uploaded={'yes' if result.inventory_uploaded else 'no'}")
    print(f"checkpoint_uploaded_count={result.checkpoint_uploaded_count}")
    for checkpoint_file, remote_checkpoint_path in zip(
        result.checkpoint_files,
        result.remote_checkpoint_paths,
        strict=True,
    ):
        print(
            "checkpoint_summary "
            f"checkpoint_file={checkpoint_file} "
            f"remote_checkpoint_path={remote_checkpoint_path}"
        )
    for summary in result.summaries:
        print(
            "manifest="
            f"{summary.manifest_file} "
            f"dataset={summary.dataset} "
            f"layer={summary.layer} "
            f"row_count={summary.row_count} "
            f"data_file={summary.data_file} "
            f"data_size_bytes={summary.data_size_bytes} "
            f"data_sha256={summary.data_sha256} "
            f"manifest_sha256={summary.manifest_sha256} "
            f"remote_data_path={summary.remote_data_path} "
            f"remote_manifest_path={summary.remote_manifest_path} "
            f"uploaded={'yes' if summary.uploaded else 'no'} "
            f"skipped={'yes' if summary.skipped else 'no'} "
            f"receipt_file={summary.receipt_file or '-'}"
        )


def _print_verify_offsite_result(result: VerifyResearchArchiveOffsiteResult) -> None:
    print("verified research archive offsite")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"archive_dir={result.archive_dir}")
    print(f"offsite_url={result.offsite_url}")
    print(f"offsite_root={result.offsite_root}")
    print(f"scanned_manifest_count={result.scanned_manifest_count}")
    print(f"verified_manifest_count={result.verified_manifest_count}")
    print(f"verified_object_count={result.verified_object_count}")
    print(f"verification_receipt_count={result.verification_receipt_count}")
    print(f"verified_checkpoint_count={result.verified_checkpoint_count}")
    print(f"readback_count={result.readback_count}")
    for readback in result.readbacks:
        print(
            "readback_summary "
            f"remote_data_path={readback.remote_data_path} "
            f"row_count={readback.row_count} "
            f"data_size_bytes={readback.data_size_bytes} "
            f"data_sha256={readback.data_sha256}"
        )


def _print_audit_coverage_result(result: AuditResearchArchiveCoverageResult) -> None:
    print("audited research archive coverage")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"archive_dir={result.archive_dir}")
    print(f"archive_kind={result.archive_kind}")
    print(f"issue_count={result.issue_count}")
    for summary in result.summaries:
        print(
            "dataset_summary "
            f"dataset={summary.dataset} "
            f"status={summary.status} "
            f"scanned_checkpoint_count={summary.scanned_checkpoint_count} "
            f"verified_checkpoint_count={summary.verified_checkpoint_count} "
            f"verified_manifest_count={summary.verified_manifest_count} "
            f"verified_row_count={summary.verified_row_count} "
            f"source_id_covered={summary.source_id_covered} "
            f"issue_count={len(summary.issues)}"
        )
        for issue in summary.issues:
            print(
                "coverage_issue "
                f"dataset={issue.dataset} "
                f"checkpoint_file={issue.checkpoint_file or '-'} "
                f"message={issue.message}"
            )


def _print_housekeeping_preview_result(
    result: PreviewResearchArchiveHousekeepingResult,
) -> None:
    print("previewed research archive housekeeping")
    print(f"status={result.status}")
    print(f"triggered_by={result.triggered_by}")
    print(f"evaluated_at={result.evaluated_at.isoformat()}")
    print(f"archive_dir={result.archive_dir}")
    print(f"archive_kind={result.archive_kind}")
    print(f"coverage_status={result.coverage.status}")
    print(f"coverage_issue_count={result.coverage.issue_count}")
    print(f"total_candidates={result.total_candidates}")
    print(f"total_action_count={result.total_action_count}")
    for summary in result.summaries:
        print(
            "target_summary "
            f"target={summary.target} "
            f"dataset={summary.dataset} "
            f"enabled={'yes' if summary.enabled else 'no'} "
            f"retention_days={summary.retention_days} "
            f"cutoff={summary.cutoff.isoformat() if summary.cutoff else '-'} "
            f"source_id_covered={summary.source_id_covered} "
            f"candidate_count={summary.candidate_count} "
            f"action_count={summary.action_count} "
            f"selected_min_id={summary.selected_min_id or '-'} "
            f"selected_max_id={summary.selected_max_id or '-'} "
            f"limited={'yes' if summary.limited else 'no'}"
        )
    run_tree = result.run_tree_summary
    print(
        "run_tree_summary "
        f"enabled={'yes' if run_tree.enabled else 'no'} "
        f"retention_days={run_tree.retention_days} "
        f"cutoff={run_tree.cutoff.isoformat() if run_tree.cutoff else '-'} "
        f"seen_event_source_id_covered={run_tree.seen_event_source_id_covered} "
        f"candidate_count={run_tree.candidate_count} "
        f"coverage_safe_candidate_count={run_tree.coverage_safe_candidate_count} "
        f"coverage_blocked_candidate_count={run_tree.coverage_blocked_candidate_count} "
        f"action_count={run_tree.action_count} "
        f"selected_partition_count={run_tree.selected_partition_count} "
        f"selected_vacancy_seen_event_count={run_tree.selected_vacancy_seen_event_count} "
        f"limited={'yes' if run_tree.limited else 'no'}"
    )


def _git_revision() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def _source_command() -> str:
    return " ".join(shlex.quote(argument) for argument in sys.argv[1:])


def _non_negative_float(value: str) -> float:
    normalized_value = float(value)
    if normalized_value < 0:
        raise argparse.ArgumentTypeError("value must be greater than or equal to zero")
    return normalized_value


def _non_negative_int(value: str) -> int:
    normalized_value = int(value)
    if normalized_value < 0:
        raise argparse.ArgumentTypeError("value must be greater than or equal to zero")
    return normalized_value


def _positive_int(value: str) -> int:
    normalized_value = int(value)
    if normalized_value < 1:
        raise argparse.ArgumentTypeError("value must be greater than or equal to one")
    return normalized_value

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

from hhru_platform.application.commands.export_research_archive import (
    DEFAULT_RESEARCH_ARCHIVE_DATASETS,
    SUPPORTED_RESEARCH_ARCHIVE_DATASETS,
    ExportResearchArchiveCommand,
    ExportResearchArchiveResult,
    export_research_archive,
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
from hhru_platform.infrastructure.db.repositories.research_archive_repo import (
    SqlAlchemyResearchArchiveRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.research_archive import (
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
        help="Upload local Archive v1 chunks, manifests and inventory to S3 offsite storage.",
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


def handle_export_research_archive(args: argparse.Namespace) -> int:
    settings = get_settings()
    command = ExportResearchArchiveCommand(
        archive_dir=Path(args.archive_dir or settings.research_archive_dir),
        datasets=tuple(args.dataset or DEFAULT_RESEARCH_ARCHIVE_DATASETS),
        chunk_size=int(args.chunk_size),
        batch_size=int(args.batch_size),
        limit_per_dataset=args.limit_per_dataset,
        archive_kind=str(args.archive_kind),
        triggered_by=str(args.triggered_by),
        source_database=settings.db_name,
        source_git_revision=_git_revision(),
        source_command=_source_command(),
    )

    try:
        with session_scope() as session:
            result = export_research_archive(
                command,
                research_archive_repository=SqlAlchemyResearchArchiveRepository(session),
                research_archive_store=LocalResearchArchiveStore(),
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
        )
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1

    _print_verify_offsite_result(result)
    return 0


def _print_export_result(result: ExportResearchArchiveResult) -> None:
    print("completed research archive export")
    print(f"status={result.status}")
    print(f"schema_version={result.schema_version}")
    print(f"archive_kind={result.archive_kind}")
    print(f"triggered_by={result.triggered_by}")
    print(f"archive_dir={result.archive_dir}")
    print(f"created_at={result.created_at.isoformat()}")
    print(f"total_chunk_count={result.total_chunk_count}")
    print(f"total_row_count={result.total_row_count}")
    print(f"total_data_size_bytes={result.total_data_size_bytes}")
    for summary in result.summaries:
        print(
            "dataset_summary "
            f"dataset={summary.dataset} "
            f"chunk_count={summary.chunk_count} "
            f"row_count={summary.row_count} "
            f"data_size_bytes={summary.data_size_bytes}"
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
    print(f"readback_count={result.readback_count}")
    for readback in result.readbacks:
        print(
            "readback_summary "
            f"remote_data_path={readback.remote_data_path} "
            f"row_count={readback.row_count} "
            f"data_size_bytes={readback.data_size_bytes} "
            f"data_sha256={readback.data_sha256}"
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

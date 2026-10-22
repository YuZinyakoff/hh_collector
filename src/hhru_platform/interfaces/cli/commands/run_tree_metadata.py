from __future__ import annotations

import argparse
import sys
from pathlib import Path
from uuid import UUID

from sqlalchemy import text

from hhru_platform.application.commands.export_run_tree_metadata import export_run_tree_metadata
from hhru_platform.config.settings import get_settings
from hhru_platform.infrastructure.backup.s3_backup_offsite_uploader import S3BackupOffsiteUploader
from hhru_platform.infrastructure.db.repositories.run_tree_metadata_repo import (
    SqlAlchemyRunTreeMetadataRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.research_archive.run_tree_metadata import (
    sync_and_verify_bundle,
    verify_bundle,
)
from hhru_platform.interfaces.cli.commands.research_archive import (
    _ensure_research_archive_s3_backend,
    _research_archive_s3_access_key_id,
    _research_archive_s3_bucket,
    _research_archive_s3_endpoint_url,
    _research_archive_s3_region,
    _research_archive_s3_secret_access_key,
    _s3_offsite_url,
)


def register_run_tree_metadata_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    export = subparsers.add_parser(
        "export-run-tree-metadata", help="Export one finished run and all partitions; no DB writes."
    )
    export.add_argument("--run-id", type=UUID, required=True)
    export.add_argument("--output-dir", type=Path, default=Path(".state/archive/run-tree-metadata"))
    export.set_defaults(handler=handle_export)
    verify = subparsers.add_parser(
        "verify-run-tree-metadata", help="Verify local metadata checksums and parent graph."
    )
    verify.add_argument("--bundle-dir", type=Path, required=True)
    verify.set_defaults(handler=handle_verify)
    sync = subparsers.add_parser(
        "sync-run-tree-metadata", help="Upload metadata to S3 and fully read back both files."
    )
    sync.add_argument("--bundle-dir", type=Path, required=True)
    sync.set_defaults(handler=handle_sync)


def handle_export(args: argparse.Namespace) -> int:
    try:
        with session_scope() as session:
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            session.execute(text("SET LOCAL statement_timeout = '5min'"))
            bundle = export_run_tree_metadata(
                run_id=args.run_id,
                output_dir=args.output_dir,
                repository=SqlAlchemyRunTreeMetadataRepository(session),
            )
        print(f"status=succeeded\nrun_id={bundle.run_id}\nbundle_dir={bundle.directory}")
        print(f"partition_count={bundle.partition_count}")
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


def handle_verify(args: argparse.Namespace) -> int:
    try:
        bundle = verify_bundle(args.bundle_dir)
        print(f"status=succeeded\nrun_id={bundle.run_id}\npartition_count={bundle.partition_count}")
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


def handle_sync(args: argparse.Namespace) -> int:
    try:
        settings = get_settings()
        _ensure_research_archive_s3_backend(settings)
        endpoint = _research_archive_s3_endpoint_url(settings)
        bucket = _research_archive_s3_bucket(settings)
        store = S3BackupOffsiteUploader.with_credentials(
            endpoint_url=endpoint,
            bucket=bucket,
            key_prefix="",
            region_name=_research_archive_s3_region(settings),
            access_key_id=_research_archive_s3_access_key_id(settings),
            secret_access_key=_research_archive_s3_secret_access_key(settings),
        )
        bundle = sync_and_verify_bundle(
            args.bundle_dir,
            remote_store=store,
            offsite_url=_s3_offsite_url(endpoint_url=endpoint, bucket=bucket),
            offsite_root=settings.research_archive_offsite_root,
        )
        print(f"status=succeeded\nrun_id={bundle.run_id}\nfull_readback=yes")
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0

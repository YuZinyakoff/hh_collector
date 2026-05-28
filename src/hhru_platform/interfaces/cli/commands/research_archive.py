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
from hhru_platform.application.commands.verify_research_archive import (
    VerifyResearchArchiveCommand,
    VerifyResearchArchiveResult,
    verify_research_archive,
)
from hhru_platform.config.settings import get_settings
from hhru_platform.infrastructure.db.repositories.research_archive_repo import (
    SqlAlchemyResearchArchiveRepository,
)
from hhru_platform.infrastructure.db.session import session_scope
from hhru_platform.infrastructure.research_archive import (
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

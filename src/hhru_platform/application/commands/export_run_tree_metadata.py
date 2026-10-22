import logging
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)
from hhru_platform.infrastructure.research_archive.run_tree_metadata import (
    RunTreeMetadataBundle,
    write_bundle,
)

LOGGER = logging.getLogger(__name__)


class RunTreeMetadataRepository(Protocol):
    def iter_records(self, run_id: UUID) -> Iterable[Mapping[str, Any]]: ...


def export_run_tree_metadata(
    *, run_id: UUID, output_dir: Path, repository: RunTreeMetadataRepository
) -> RunTreeMetadataBundle:
    started_at = log_operation_started(LOGGER, operation="export_run_tree_metadata", run_id=run_id)
    try:
        bundle = write_bundle(
            output_dir=output_dir, run_id=run_id, records=repository.iter_records(run_id)
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="export_run_tree_metadata",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            run_id=run_id,
        )
        raise
    record_operation_succeeded(
        LOGGER,
        operation="export_run_tree_metadata",
        started_at=started_at,
        run_id=run_id,
        partition_count=bundle.partition_count,
    )
    return bundle

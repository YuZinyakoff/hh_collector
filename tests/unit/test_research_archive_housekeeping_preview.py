from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy.dialects import postgresql

from hhru_platform.application.commands.audit_research_archive_coverage import (
    RESEARCH_ARCHIVE_COVERAGE_STATUS_COMPLETE,
    RESEARCH_ARCHIVE_COVERAGE_STATUS_INCOMPLETE,
    AuditResearchArchiveCoverageResult,
    ResearchArchiveCoverageIssue,
    ResearchArchiveDatasetCoverageSummary,
)
from hhru_platform.application.commands.preview_research_archive_housekeeping import (
    RESEARCH_ARCHIVE_HOUSEKEEPING_PREVIEW_STATUS_BLOCKED,
    RESEARCH_ARCHIVE_HOUSEKEEPING_PREVIEW_STATUS_READY,
    PreviewResearchArchiveHousekeepingCommand,
    preview_research_archive_housekeeping,
)
from hhru_platform.infrastructure.db.repositories.housekeeping_repo import (
    SqlAlchemyHousekeepingRepository,
)


class FakeHousekeepingRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, datetime, int, int | None]] = []

    def count_raw_api_payload_candidates(
        self,
        *,
        cutoff: datetime,
        max_source_id: int | None = None,
    ) -> int:
        self.calls.append(("count_raw", cutoff, max_source_id or 0, None))
        return 5

    def list_raw_api_payload_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_source_id: int | None = None,
    ) -> list[int]:
        self.calls.append(("list_raw", cutoff, max_source_id or 0, limit))
        return [3, 5]

    def count_vacancy_snapshot_candidates(
        self,
        *,
        cutoff: datetime,
        max_source_id: int | None = None,
    ) -> int:
        self.calls.append(("count_snapshot", cutoff, max_source_id or 0, None))
        return 2

    def list_vacancy_snapshot_ids_for_retention(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_source_id: int | None = None,
    ) -> list[int]:
        self.calls.append(("list_snapshot", cutoff, max_source_id or 0, limit))
        return [11, 13]


def test_preview_research_archive_housekeeping_is_blocked_before_complete_coverage(
    monkeypatch,
) -> None:
    repository = FakeHousekeepingRepository()
    monkeypatch.setattr(
        "hhru_platform.application.commands.preview_research_archive_housekeeping."
        "audit_research_archive_coverage",
        lambda *args, **kwargs: _coverage_result(complete=False),
    )

    result = preview_research_archive_housekeeping(
        _command(),
        housekeeping_repository=repository,
        checkpoint_store=cast(Any, object()),
        receipt_store=cast(Any, object()),
        checkpoint_receipt_store=cast(Any, object()),
    )

    assert result.status == RESEARCH_ARCHIVE_HOUSEKEEPING_PREVIEW_STATUS_BLOCKED
    assert result.ready is False
    assert result.summaries == ()
    assert repository.calls == []


def test_preview_research_archive_housekeeping_bounds_candidates_by_verified_cursors(
    monkeypatch,
) -> None:
    repository = FakeHousekeepingRepository()
    monkeypatch.setattr(
        "hhru_platform.application.commands.preview_research_archive_housekeeping."
        "audit_research_archive_coverage",
        lambda *args, **kwargs: _coverage_result(complete=True),
    )

    result = preview_research_archive_housekeeping(
        _command(),
        housekeeping_repository=repository,
        checkpoint_store=cast(Any, object()),
        receipt_store=cast(Any, object()),
        checkpoint_receipt_store=cast(Any, object()),
    )

    assert result.status == RESEARCH_ARCHIVE_HOUSEKEEPING_PREVIEW_STATUS_READY
    assert result.ready is True
    assert result.total_candidates == 7
    assert result.total_action_count == 4
    assert repository.calls == [
        ("count_raw", datetime(2026, 5, 22, 12, 0, tzinfo=UTC), 81, None),
        ("list_raw", datetime(2026, 5, 22, 12, 0, tzinfo=UTC), 81, 10),
        ("count_snapshot", datetime(2026, 5, 12, 12, 0, tzinfo=UTC), 1240, None),
        ("list_snapshot", datetime(2026, 5, 12, 12, 0, tzinfo=UTC), 1240, 10),
    ]
    assert result.summaries[0].selected_min_id == 3
    assert result.summaries[0].selected_max_id == 5
    assert result.summaries[0].limited is True


def test_housekeeping_repository_applies_verified_source_id_bounds() -> None:
    session = RecordingSession()
    repository = SqlAlchemyHousekeepingRepository(cast(Any, session))
    cutoff = datetime(2026, 5, 22, 12, 0, tzinfo=UTC)

    repository.count_raw_api_payload_candidates(cutoff=cutoff, max_source_id=81)
    raw_sql = _sql(session.statement)
    repository.count_vacancy_snapshot_candidates(cutoff=cutoff, max_source_id=1240)
    snapshot_sql = _sql(session.statement)

    assert "raw_api_payload.id <= 81" in raw_sql
    assert "vacancy_snapshot.id <= 1240" in snapshot_sql


class RecordingSession:
    def __init__(self) -> None:
        self.statement = None

    def scalar(self, statement) -> int:
        self.statement = statement
        return 0


def _command() -> PreviewResearchArchiveHousekeepingCommand:
    return PreviewResearchArchiveHousekeepingCommand(
        archive_dir=".state/archive/research",
        archive_kind="incremental_validation",
        offsite_url="https://s3.example.test/bucket",
        offsite_root="/hhru-platform/research-archive-smoke/test",
        raw_api_payload_retention_days=10,
        vacancy_snapshot_retention_days=20,
        delete_limit_per_target=10,
        triggered_by="unit-test",
        evaluated_at=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
    )


def _coverage_result(*, complete: bool) -> AuditResearchArchiveCoverageResult:
    status = (
        RESEARCH_ARCHIVE_COVERAGE_STATUS_COMPLETE
        if complete
        else RESEARCH_ARCHIVE_COVERAGE_STATUS_INCOMPLETE
    )
    issue = () if complete else (
        ResearchArchiveCoverageIssue(
            dataset="bronze/raw_api_payload",
            checkpoint_file=None,
            message="checkpoint receipt missing",
        ),
    )
    return AuditResearchArchiveCoverageResult(
        status=status,
        archive_dir=".state/archive/research",
        archive_kind="incremental_validation",
        triggered_by="unit-test",
        summaries=(
            ResearchArchiveDatasetCoverageSummary(
                dataset="bronze/raw_api_payload",
                status=status,
                scanned_checkpoint_count=2,
                verified_checkpoint_count=2 if complete else 0,
                verified_manifest_count=3 if complete else 0,
                verified_row_count=20 if complete else 0,
                source_id_covered=81 if complete else 0,
                issues=issue,
            ),
            ResearchArchiveDatasetCoverageSummary(
                dataset="silver/api_request_log",
                status=status,
                scanned_checkpoint_count=2,
                verified_checkpoint_count=2 if complete else 0,
                verified_manifest_count=2 if complete else 0,
                verified_row_count=20 if complete else 0,
                source_id_covered=81 if complete else 0,
                issues=(),
            ),
            ResearchArchiveDatasetCoverageSummary(
                dataset="silver/vacancy_snapshot",
                status=status,
                scanned_checkpoint_count=2,
                verified_checkpoint_count=2 if complete else 0,
                verified_manifest_count=2 if complete else 0,
                verified_row_count=20 if complete else 0,
                source_id_covered=1240 if complete else 0,
                issues=(),
            ),
            ResearchArchiveDatasetCoverageSummary(
                dataset="silver/vacancy_seen_event",
                status=status,
                scanned_checkpoint_count=2,
                verified_checkpoint_count=2 if complete else 0,
                verified_manifest_count=2 if complete else 0,
                verified_row_count=20 if complete else 0,
                source_id_covered=1240 if complete else 0,
                issues=(),
            ),
        ),
    )


def _sql(statement) -> str:
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

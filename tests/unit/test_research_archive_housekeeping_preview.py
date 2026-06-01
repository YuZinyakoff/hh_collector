from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

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
        self.calls: list[tuple[object, ...]] = []
        self.run_ids = [UUID("00000000-0000-0000-0000-000000000001")]

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

    def count_finished_crawl_run_candidates(self, *, cutoff: datetime) -> int:
        self.calls.append(("count_run", cutoff))
        return 3

    def count_finished_crawl_run_candidates_blocked_by_seen_event_coverage(
        self,
        *,
        cutoff: datetime,
        max_seen_event_source_id: int,
    ) -> int:
        self.calls.append(("count_blocked_run", cutoff, max_seen_event_source_id))
        return 1

    def list_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
        self,
        *,
        cutoff: datetime,
        limit: int,
        max_seen_event_source_id: int,
    ) -> list[UUID]:
        self.calls.append(("list_safe_run", cutoff, max_seen_event_source_id, limit))
        return self.run_ids

    def count_crawl_partitions_for_run_ids(self, run_ids: list[UUID]) -> int:
        self.calls.append(("count_partition", tuple(run_ids)))
        return 7

    def count_vacancy_seen_events_for_run_ids(self, run_ids: list[UUID]) -> int:
        self.calls.append(("count_seen_event", tuple(run_ids)))
        return 11


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
    assert result.run_tree_summary.enabled is False
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
        ("count_run", datetime(2026, 5, 2, 12, 0, tzinfo=UTC)),
        ("count_blocked_run", datetime(2026, 5, 2, 12, 0, tzinfo=UTC), 1240),
        ("list_safe_run", datetime(2026, 5, 2, 12, 0, tzinfo=UTC), 1240, 10),
        ("count_partition", tuple(repository.run_ids)),
        ("count_seen_event", tuple(repository.run_ids)),
    ]
    assert result.summaries[0].selected_min_id == 3
    assert result.summaries[0].selected_max_id == 5
    assert result.summaries[0].limited is True
    assert result.run_tree_summary.candidate_count == 3
    assert result.run_tree_summary.coverage_safe_candidate_count == 2
    assert result.run_tree_summary.coverage_blocked_candidate_count == 1
    assert result.run_tree_summary.action_count == 1
    assert result.run_tree_summary.selected_partition_count == 7
    assert result.run_tree_summary.selected_vacancy_seen_event_count == 11


def test_housekeeping_repository_applies_verified_source_id_bounds() -> None:
    session = RecordingSession()
    repository = SqlAlchemyHousekeepingRepository(cast(Any, session))
    cutoff = datetime(2026, 5, 22, 12, 0, tzinfo=UTC)

    repository.count_raw_api_payload_candidates(cutoff=cutoff, max_source_id=81)
    raw_sql = _sql(session.statement)
    repository.count_vacancy_snapshot_candidates(cutoff=cutoff, max_source_id=1240)
    snapshot_sql = _sql(session.statement)

    assert "raw_api_payload.id <= 81" in raw_sql
    assert "vacancy_snapshot.short_payload_ref_id <= 81" in raw_sql
    assert "vacancy_snapshot.detail_payload_ref_id <= 81" in raw_sql
    assert "vacancy_snapshot.id <= 1240" in snapshot_sql
    assert "NOT (EXISTS (SELECT 1" in snapshot_sql
    assert "vacancy_snapshot_1.vacancy_id = vacancy_snapshot.vacancy_id" in snapshot_sql

    repository.count_finished_crawl_run_candidates_blocked_by_seen_event_coverage(
        cutoff=cutoff,
        max_seen_event_source_id=1240,
    )
    blocked_run_sql = _sql(session.statement)
    repository.list_finished_crawl_run_ids_for_retention_bounded_by_seen_event_coverage(
        cutoff=cutoff,
        limit=10,
        max_seen_event_source_id=1240,
    )
    safe_run_sql = _sql(session.statement)

    assert "vacancy_seen_event.crawl_run_id = crawl_run.id" in blocked_run_sql
    assert "vacancy_seen_event.id > 1240" in blocked_run_sql
    assert "NOT (EXISTS (SELECT 1" in safe_run_sql


class RecordingSession:
    def __init__(self) -> None:
        self.statement = None

    def scalar(self, statement) -> int:
        self.statement = statement
        return 0

    def scalars(self, statement) -> tuple[object, ...]:
        self.statement = statement
        return ()


def _command() -> PreviewResearchArchiveHousekeepingCommand:
    return PreviewResearchArchiveHousekeepingCommand(
        archive_dir=".state/archive/research",
        archive_kind="incremental_validation",
        offsite_url="https://s3.example.test/bucket",
        offsite_root="/hhru-platform/research-archive-smoke/test",
        raw_api_payload_retention_days=10,
        vacancy_snapshot_retention_days=20,
        finished_crawl_run_retention_days=30,
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

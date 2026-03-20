from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailResult,
)
from hhru_platform.application.commands.retry_failed_details import (
    RetryFailedDetailsCommand,
    RetryFailedDetailsNotAllowedError,
    retry_failed_details,
)
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.domain.entities.detail_fetch_attempt import DetailFetchAttempt


class InMemoryCrawlRunRepository:
    def __init__(self, crawl_run: CrawlRun) -> None:
        self.crawl_run = crawl_run
        self.complete_calls: list[dict[str, object]] = []

    def get(self, run_id: UUID) -> CrawlRun | None:
        if self.crawl_run.id != run_id:
            return None
        return self.crawl_run

    def complete(self, **kwargs: object) -> CrawlRun:
        self.complete_calls.append(dict(kwargs))
        self.crawl_run.status = str(kwargs["status"])
        self.crawl_run.finished_at = kwargs["finished_at"]
        self.crawl_run.notes = kwargs["notes"]
        return self.crawl_run


class SequenceDetailFetchAttemptRepository:
    def __init__(self, snapshots: tuple[tuple[DetailFetchAttempt, ...], ...]) -> None:
        self._snapshots = list(snapshots)

    def list_repair_backlog_by_run_id(self, crawl_run_id: UUID) -> list[DetailFetchAttempt]:
        assert self._snapshots
        return list(self._snapshots.pop(0))


class RecordingDetailRepairMetricsRecorder:
    def __init__(self) -> None:
        self.backlog_updates: list[dict[str, object]] = []
        self.attempts: list[dict[str, object]] = []
        self.terminal_status_updates: list[dict[str, object]] = []

    def set_detail_repair_backlog(self, **kwargs) -> None:
        self.backlog_updates.append(kwargs)

    def record_detail_repair_attempt(self, **kwargs) -> None:
        self.attempts.append(kwargs)

    def record_run_terminal_status(self, **kwargs) -> None:
        self.terminal_status_updates.append(kwargs)


def test_retry_failed_details_retries_backlog_and_promotes_run_when_cleared() -> None:
    run_id = uuid4()
    vacancy_one = uuid4()
    vacancy_two = uuid4()
    crawl_run = _build_crawl_run(
        run_id=run_id,
        status="completed_with_detail_errors",
    )
    run_repository = InMemoryCrawlRunRepository(crawl_run)
    backlog_item_one = _build_attempt(run_id=run_id, vacancy_id=vacancy_one, attempt=1)
    backlog_item_two = _build_attempt(run_id=run_id, vacancy_id=vacancy_two, attempt=3)
    attempt_repository = SequenceDetailFetchAttemptRepository(
        (
            (backlog_item_one, backlog_item_two),
            (),
        )
    )
    metrics_recorder = RecordingDetailRepairMetricsRecorder()
    events: list[tuple[UUID, str, int]] = []

    def fetch_vacancy_detail_step(command) -> FetchVacancyDetailResult:
        events.append((command.vacancy_id, command.reason, command.attempt))
        return _build_detail_result(command.vacancy_id)

    result = retry_failed_details(
        RetryFailedDetailsCommand(crawl_run_id=run_id, triggered_by="cli"),
        crawl_run_repository=run_repository,
        detail_fetch_attempt_repository=attempt_repository,
        fetch_vacancy_detail_step=fetch_vacancy_detail_step,
        metrics_recorder=metrics_recorder,
    )

    assert events == [
        (vacancy_one, "repair_backlog", 2),
        (vacancy_two, "repair_backlog", 4),
    ]
    assert result.status == "succeeded"
    assert result.run_status_before == "completed_with_detail_errors"
    assert result.run_status_after == "succeeded"
    assert result.backlog_size == 2
    assert result.retried_count == 2
    assert result.repaired_count == 2
    assert result.still_failing_count == 0
    assert result.remaining_backlog_count == 0
    assert run_repository.complete_calls[0]["status"] == "succeeded"
    assert metrics_recorder.backlog_updates == [
        {
            "run_id": str(run_id),
            "run_type": "weekly_sweep",
            "backlog_size": 2,
        },
        {
            "run_id": str(run_id),
            "run_type": "weekly_sweep",
            "backlog_size": 0,
        },
    ]
    assert metrics_recorder.attempts == [
        {
            "run_type": "weekly_sweep",
            "outcome": "succeeded",
            "retried_count": 2,
            "repaired_count": 2,
            "still_failing_count": 0,
        }
    ]
    assert metrics_recorder.terminal_status_updates == [
        {
            "run_type": "weekly_sweep",
            "status": "succeeded",
            "recorded_at": crawl_run.finished_at,
        }
    ]


def test_retry_failed_details_keeps_run_degraded_when_backlog_remains() -> None:
    run_id = uuid4()
    vacancy_one = uuid4()
    vacancy_two = uuid4()
    crawl_run = _build_crawl_run(
        run_id=run_id,
        status="completed_with_detail_errors",
    )
    run_repository = InMemoryCrawlRunRepository(crawl_run)
    backlog_item_one = _build_attempt(run_id=run_id, vacancy_id=vacancy_one, attempt=1)
    backlog_item_two = _build_attempt(run_id=run_id, vacancy_id=vacancy_two, attempt=2)
    attempt_repository = SequenceDetailFetchAttemptRepository(
        (
            (backlog_item_one, backlog_item_two),
            (backlog_item_two,),
        )
    )
    metrics_recorder = RecordingDetailRepairMetricsRecorder()

    def fetch_vacancy_detail_step(command) -> FetchVacancyDetailResult:
        if command.vacancy_id == vacancy_two:
            return _build_detail_result(command.vacancy_id, error_message="still failing")
        return _build_detail_result(command.vacancy_id)

    result = retry_failed_details(
        RetryFailedDetailsCommand(crawl_run_id=run_id, triggered_by="cli"),
        crawl_run_repository=run_repository,
        detail_fetch_attempt_repository=attempt_repository,
        fetch_vacancy_detail_step=fetch_vacancy_detail_step,
        metrics_recorder=metrics_recorder,
    )

    assert result.status == "completed_with_detail_errors"
    assert result.run_status_after == "completed_with_detail_errors"
    assert result.backlog_size == 2
    assert result.retried_count == 2
    assert result.repaired_count == 1
    assert result.still_failing_count == 1
    assert result.remaining_backlog_count == 1
    assert "still failing" in (result.error_message or "")
    assert run_repository.complete_calls[0]["status"] == "completed_with_detail_errors"
    assert metrics_recorder.backlog_updates == [
        {
            "run_id": str(run_id),
            "run_type": "weekly_sweep",
            "backlog_size": 2,
        },
        {
            "run_id": str(run_id),
            "run_type": "weekly_sweep",
            "backlog_size": 1,
        },
    ]
    assert metrics_recorder.attempts == [
        {
            "run_type": "weekly_sweep",
            "outcome": "completed_with_detail_errors",
            "retried_count": 2,
            "repaired_count": 1,
            "still_failing_count": 1,
        }
    ]
    assert metrics_recorder.terminal_status_updates == []


def test_retry_failed_details_is_noop_when_run_is_already_clean() -> None:
    run_id = uuid4()
    crawl_run = _build_crawl_run(run_id=run_id, status="succeeded")
    run_repository = InMemoryCrawlRunRepository(crawl_run)
    attempt_repository = SequenceDetailFetchAttemptRepository(((), ()))

    result = retry_failed_details(
        RetryFailedDetailsCommand(crawl_run_id=run_id),
        crawl_run_repository=run_repository,
        detail_fetch_attempt_repository=attempt_repository,
        fetch_vacancy_detail_step=lambda command: (_ for _ in ()).throw(
            AssertionError(f"unexpected detail fetch {command.vacancy_id}")
        ),
    )

    assert result.status == "succeeded"
    assert result.backlog_size == 0
    assert result.retried_count == 0
    assert result.remaining_backlog_count == 0
    assert run_repository.complete_calls == []


def test_retry_failed_details_rejects_non_repairable_run_status() -> None:
    run_id = uuid4()
    crawl_run = _build_crawl_run(run_id=run_id, status="completed_with_unresolved")

    with pytest.raises(
        RetryFailedDetailsNotAllowedError,
        match="status=completed_with_unresolved",
    ):
        retry_failed_details(
            RetryFailedDetailsCommand(crawl_run_id=run_id),
            crawl_run_repository=InMemoryCrawlRunRepository(crawl_run),
            detail_fetch_attempt_repository=SequenceDetailFetchAttemptRepository(((),)),
            fetch_vacancy_detail_step=lambda command: (_ for _ in ()).throw(
                AssertionError(f"unexpected detail fetch {command.vacancy_id}")
            ),
        )


def _build_crawl_run(*, run_id: UUID, status: str) -> CrawlRun:
    return CrawlRun(
        id=run_id,
        run_type="weekly_sweep",
        status=status,
        started_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
        finished_at=datetime(2026, 3, 20, 12, 30, tzinfo=UTC),
        triggered_by="pytest",
        config_snapshot_json={},
        partitions_total=3,
        partitions_done=3,
        partitions_failed=0,
        notes="initial note",
    )


def _build_attempt(*, run_id: UUID, vacancy_id: UUID, attempt: int) -> DetailFetchAttempt:
    return DetailFetchAttempt(
        id=attempt,
        vacancy_id=vacancy_id,
        crawl_run_id=run_id,
        reason="first_seen",
        attempt=attempt,
        status="failed",
        requested_at=datetime(2026, 3, 20, 12, attempt, tzinfo=UTC),
        finished_at=datetime(2026, 3, 20, 12, attempt, 30, tzinfo=UTC),
        error_message="failed before repair",
    )


def _build_detail_result(
    vacancy_id: UUID,
    *,
    error_message: str | None = None,
) -> FetchVacancyDetailResult:
    detail_fetch_status = "failed" if error_message is not None else "succeeded"
    return FetchVacancyDetailResult(
        vacancy_id=vacancy_id,
        hh_vacancy_id=f"hh-{vacancy_id}",
        detail_fetch_status=detail_fetch_status,
        snapshot_id=100 if error_message is None else None,
        request_log_id=200,
        raw_payload_id=300,
        detail_fetch_attempt_id=400,
        error_message=error_message,
    )

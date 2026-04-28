from __future__ import annotations

import argparse
from datetime import UTC, datetime

from sqlalchemy import text

from hhru_platform.application.commands.drain_first_detail_backlog import (
    FIRST_DETAIL_BACKLOG_REASON,
)
from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyVacancyCurrentStateRepository,
)
from hhru_platform.infrastructure.db.session import session_scope


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Print a key=value snapshot for first-detail backlog measurement."
    )
    parser.add_argument("--retry-cooldown-seconds", type=int, default=3600)
    parser.add_argument("--max-retry-cooldown-seconds", type=int, default=86400)
    args = parser.parse_args()

    now = datetime.now(UTC)
    with session_scope() as session:
        state_repository = SqlAlchemyVacancyCurrentStateRepository(session)
        active_backlog_size = state_repository.count_first_detail_backlog(
            include_inactive=False
        )
        active_ready_backlog_size = state_repository.count_first_detail_backlog_ready(
            include_inactive=False,
            retry_cooldown_seconds=args.retry_cooldown_seconds,
            max_retry_cooldown_seconds=args.max_retry_cooldown_seconds,
            now=now,
        )
        all_backlog_size = state_repository.count_first_detail_backlog(
            include_inactive=True
        )
        all_ready_backlog_size = state_repository.count_first_detail_backlog_ready(
            include_inactive=True,
            retry_cooldown_seconds=args.retry_cooldown_seconds,
            max_retry_cooldown_seconds=args.max_retry_cooldown_seconds,
            now=now,
        )
        db_size_bytes = int(
            session.scalar(text("select pg_database_size(current_database())")) or 0
        )
        vacancy_current_state_rows = int(
            session.scalar(text("select count(*) from vacancy_current_state")) or 0
        )
        vacancy_snapshot_rows = int(
            session.scalar(text("select count(*) from vacancy_snapshot")) or 0
        )
        detail_snapshot_rows = int(
            session.scalar(
                text("select count(*) from vacancy_snapshot where snapshot_type = 'detail'")
            )
            or 0
        )
        raw_payload_rows = int(session.scalar(text("select count(*) from raw_api_payload")) or 0)
        api_request_rows = int(session.scalar(text("select count(*) from api_request_log")) or 0)
        status_counts = _fetch_counts(
            session.execute(
                text(
                    """
                    select detail_fetch_status, count(*)
                    from vacancy_current_state
                    group by detail_fetch_status
                    order by detail_fetch_status
                    """
                )
            ).all()
        )
        first_detail_attempt_counts = _fetch_counts(
            session.execute(
                text(
                    """
                    select status, count(*)
                    from detail_fetch_attempt
                    where reason = :reason
                    group by status
                    order by status
                    """
                ),
                {"reason": FIRST_DETAIL_BACKLOG_REASON},
            ).all()
        )

    print(f"recorded_at={now.isoformat()}")
    print(f"db_size_bytes={db_size_bytes}")
    print(f"vacancy_current_state_rows={vacancy_current_state_rows}")
    print(f"vacancy_snapshot_rows={vacancy_snapshot_rows}")
    print(f"detail_snapshot_rows={detail_snapshot_rows}")
    print(f"raw_payload_rows={raw_payload_rows}")
    print(f"api_request_rows={api_request_rows}")
    print(f"active_backlog_size={active_backlog_size}")
    print(f"active_ready_backlog_size={active_ready_backlog_size}")
    print(
        "active_cooldown_backlog_size="
        f"{max(active_backlog_size - active_ready_backlog_size, 0)}"
    )
    print(f"all_backlog_size={all_backlog_size}")
    print(f"all_ready_backlog_size={all_ready_backlog_size}")
    print(f"all_cooldown_backlog_size={max(all_backlog_size - all_ready_backlog_size, 0)}")
    _print_counts("vacancy_current_state_status", status_counts)
    _print_counts("first_detail_attempt_status", first_detail_attempt_counts)
    return 0


def _fetch_counts(rows: list[tuple[str, int]]) -> dict[str, int]:
    return {str(key): int(value) for key, value in rows}


def _print_counts(prefix: str, counts: dict[str, int]) -> None:
    for key, value in counts.items():
        print(f"{prefix}.{key}={value}")


if __name__ == "__main__":
    raise SystemExit(main())

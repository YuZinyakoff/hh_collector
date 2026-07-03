from __future__ import annotations

from typing import cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session

from hhru_platform.infrastructure.db.repositories import (
    SqlAlchemyDetailFetchAttemptRepository,
)
from hhru_platform.infrastructure.db.repositories import (
    detail_fetch_attempt_repo as detail_attempt_repo_module,
)


def test_latest_attempt_numbers_lookup_is_chunked_and_deduplicated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        detail_attempt_repo_module,
        "DETAIL_ATTEMPT_LOOKUP_CHUNK_SIZE",
        2,
    )
    repository = SqlAlchemyDetailFetchAttemptRepository(cast(Session, object()))
    vacancy_ids = [uuid4() for _ in range(5)]
    latest_attempts = {
        vacancy_ids[0]: 1,
        vacancy_ids[1]: 2,
        vacancy_ids[2]: 3,
        vacancy_ids[4]: 5,
    }
    chunks: list[tuple[UUID, ...]] = []

    def lookup_chunk(vacancy_id_chunk: tuple[UUID, ...]) -> dict[UUID, int]:
        chunks.append(vacancy_id_chunk)
        return {
            vacancy_id: latest_attempts[vacancy_id]
            for vacancy_id in vacancy_id_chunk
            if vacancy_id in latest_attempts
        }

    monkeypatch.setattr(
        repository,
        "_latest_attempt_numbers_by_vacancy_id_chunk",
        lookup_chunk,
    )

    assert repository.latest_attempt_numbers_by_vacancy_ids(
        [
            vacancy_ids[0],
            vacancy_ids[1],
            vacancy_ids[0],
            vacancy_ids[2],
            vacancy_ids[3],
            vacancy_ids[4],
        ]
    ) == latest_attempts
    assert chunks == [
        (vacancy_ids[0], vacancy_ids[1]),
        (vacancy_ids[2], vacancy_ids[3]),
        (vacancy_ids[4],),
    ]

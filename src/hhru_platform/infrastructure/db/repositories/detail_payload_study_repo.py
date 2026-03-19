from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from hhru_platform.application.commands.study_detail_payloads import (
    DetailStudyCandidate,
    StoredRawPayload,
)
from hhru_platform.infrastructure.db.models.raw_api_payload import RawApiPayload
from hhru_platform.infrastructure.db.models.vacancy import Vacancy
from hhru_platform.infrastructure.db.models.vacancy_seen_event import VacancySeenEvent


class SqlAlchemyDetailPayloadStudyRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get_latest_crawl_run_id_with_search_payloads(self) -> UUID | None:
        statement = (
            select(VacancySeenEvent.crawl_run_id)
            .where(VacancySeenEvent.short_payload_ref_id.is_not(None))
            .order_by(VacancySeenEvent.seen_at.desc(), VacancySeenEvent.id.desc())
            .limit(1)
        )
        return self._session.scalar(statement)

    def list_recent_candidates(
        self,
        *,
        crawl_run_id: UUID,
        limit: int,
    ) -> list[DetailStudyCandidate]:
        latest_seen_events = (
            select(
                VacancySeenEvent.vacancy_id.label("vacancy_id"),
                Vacancy.hh_vacancy_id.label("hh_vacancy_id"),
                VacancySeenEvent.crawl_run_id.label("crawl_run_id"),
                VacancySeenEvent.seen_at.label("seen_at"),
                VacancySeenEvent.short_payload_ref_id.label("short_payload_ref_id"),
            )
            .join(Vacancy, Vacancy.id == VacancySeenEvent.vacancy_id)
            .where(
                VacancySeenEvent.crawl_run_id == crawl_run_id,
                VacancySeenEvent.short_payload_ref_id.is_not(None),
            )
            .order_by(
                VacancySeenEvent.vacancy_id,
                VacancySeenEvent.seen_at.desc(),
                VacancySeenEvent.id.desc(),
            )
            .distinct(VacancySeenEvent.vacancy_id)
            .subquery()
        )
        statement = (
            select(
                latest_seen_events.c.vacancy_id,
                latest_seen_events.c.hh_vacancy_id,
                latest_seen_events.c.crawl_run_id,
                latest_seen_events.c.seen_at,
                latest_seen_events.c.short_payload_ref_id,
            )
            .order_by(latest_seen_events.c.seen_at.desc())
            .limit(limit)
        )
        rows = self._session.execute(statement)
        return [
            DetailStudyCandidate(
                vacancy_id=vacancy_id,
                hh_vacancy_id=hh_vacancy_id,
                crawl_run_id=run_id,
                seen_at=seen_at,
                short_payload_ref_id=short_payload_ref_id,
            )
            for vacancy_id, hh_vacancy_id, run_id, seen_at, short_payload_ref_id in rows
            if short_payload_ref_id is not None
        ]

    def get_raw_payload(self, payload_id: int) -> StoredRawPayload | None:
        model = self._session.get(RawApiPayload, payload_id)
        if model is None:
            return None
        return StoredRawPayload(
            id=model.id,
            endpoint_type=model.endpoint_type,
            entity_hh_id=model.entity_hh_id,
            payload_hash=model.payload_hash,
            received_at=model.received_at,
            payload_json=model.payload_json,
        )

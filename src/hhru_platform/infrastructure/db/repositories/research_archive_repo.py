from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, cast

from sqlalchemy import Select, select
from sqlalchemy.orm import Session, aliased

from hhru_platform.infrastructure.db.models.api_request_log import ApiRequestLog
from hhru_platform.infrastructure.db.models.raw_api_payload import RawApiPayload
from hhru_platform.infrastructure.db.models.vacancy import Vacancy
from hhru_platform.infrastructure.db.models.vacancy_current_state import (
    VacancyCurrentState,
)
from hhru_platform.infrastructure.db.models.vacancy_seen_event import VacancySeenEvent
from hhru_platform.infrastructure.db.models.vacancy_snapshot import VacancySnapshot


class SqlAlchemyResearchArchiveRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def iter_dataset_records(
        self,
        *,
        dataset: str,
        batch_size: int,
        limit: int | None,
    ) -> Iterable[Mapping[str, Any]]:
        if batch_size < 1:
            raise ValueError("batch_size must be greater than or equal to one")
        if limit is not None and limit < 1:
            raise ValueError("limit must be greater than or equal to one")

        statement = self._build_statement(dataset)
        if limit is not None:
            statement = statement.limit(limit)

        result = self._session.execute(
            statement.execution_options(yield_per=batch_size, stream_results=True)
        )
        for row in result.mappings():
            yield cast(Mapping[str, Any], dict(row))

    def _build_statement(self, dataset: str) -> Select[tuple[Any, ...]]:
        if dataset == "bronze/raw_api_payload":
            return self._raw_api_payload_statement()
        if dataset == "silver/api_request_log":
            return self._api_request_log_statement()
        if dataset == "silver/vacancy":
            return self._vacancy_statement()
        if dataset == "silver/vacancy_snapshot":
            return self._vacancy_snapshot_statement()
        if dataset == "silver/vacancy_seen_event":
            return self._vacancy_seen_event_statement()
        if dataset == "silver/vacancy_current_state":
            return self._vacancy_current_state_statement()

        raise ValueError(f"unsupported research archive dataset: {dataset}")

    @staticmethod
    def _raw_api_payload_statement() -> Select[tuple[Any, ...]]:
        statement = (
            select(
                RawApiPayload.id.label("raw_api_payload_id"),
                RawApiPayload.api_request_log_id.label("api_request_log_id"),
                ApiRequestLog.crawl_run_id.label("crawl_run_id"),
                ApiRequestLog.crawl_partition_id.label("crawl_partition_id"),
                ApiRequestLog.request_type.label("request_type"),
                RawApiPayload.endpoint_type.label("endpoint_type"),
                ApiRequestLog.endpoint.label("endpoint"),
                ApiRequestLog.method.label("method"),
                ApiRequestLog.params_json.label("params_json"),
                ApiRequestLog.status_code.label("status_code"),
                ApiRequestLog.latency_ms.label("latency_ms"),
                ApiRequestLog.requested_at.label("requested_at"),
                ApiRequestLog.response_received_at.label("response_received_at"),
                RawApiPayload.entity_hh_id.label("entity_hh_id"),
                RawApiPayload.payload_hash.label("payload_hash"),
                RawApiPayload.received_at.label("received_at"),
                RawApiPayload.payload_json.label("payload_json"),
            )
            .join(ApiRequestLog, RawApiPayload.api_request_log_id == ApiRequestLog.id)
            .order_by(
                ApiRequestLog.request_type,
                RawApiPayload.received_at,
                RawApiPayload.id,
            )
        )
        return cast(Select[tuple[Any, ...]], statement)

    @staticmethod
    def _api_request_log_statement() -> Select[tuple[Any, ...]]:
        statement = (
            select(
                ApiRequestLog.id.label("api_request_log_id"),
                ApiRequestLog.crawl_run_id.label("crawl_run_id"),
                ApiRequestLog.crawl_partition_id.label("crawl_partition_id"),
                ApiRequestLog.request_type.label("request_type"),
                ApiRequestLog.endpoint.label("endpoint"),
                ApiRequestLog.method.label("method"),
                ApiRequestLog.params_json.label("params_json"),
                ApiRequestLog.status_code.label("status_code"),
                ApiRequestLog.latency_ms.label("latency_ms"),
                ApiRequestLog.attempt.label("attempt"),
                ApiRequestLog.requested_at.label("requested_at"),
                ApiRequestLog.response_received_at.label("response_received_at"),
                ApiRequestLog.error_type.label("error_type"),
                ApiRequestLog.error_message.label("error_message"),
                RawApiPayload.id.label("raw_api_payload_id"),
                RawApiPayload.payload_hash.label("payload_hash"),
            )
            .outerjoin(RawApiPayload, RawApiPayload.api_request_log_id == ApiRequestLog.id)
            .order_by(ApiRequestLog.requested_at, ApiRequestLog.id)
        )
        return cast(Select[tuple[Any, ...]], statement)

    @staticmethod
    def _vacancy_statement() -> Select[tuple[Any, ...]]:
        statement = select(
            Vacancy.id.label("vacancy_id"),
            Vacancy.hh_vacancy_id.label("hh_vacancy_id"),
            Vacancy.name_current.label("name_current"),
            Vacancy.employer_id.label("employer_id"),
            Vacancy.area_id.label("area_id"),
            Vacancy.published_at.label("published_at"),
            Vacancy.created_at_hh.label("created_at_hh"),
            Vacancy.archived_at_hh.label("archived_at_hh"),
            Vacancy.alternate_url.label("alternate_url"),
            Vacancy.employment_type_code.label("employment_type_code"),
            Vacancy.schedule_type_code.label("schedule_type_code"),
            Vacancy.experience_code.label("experience_code"),
            Vacancy.source_type.label("source_type"),
            Vacancy.created_at.label("created_at"),
            Vacancy.updated_at.label("updated_at"),
        ).order_by(Vacancy.updated_at, Vacancy.id)
        return cast(Select[tuple[Any, ...]], statement)

    @staticmethod
    def _vacancy_snapshot_statement() -> Select[tuple[Any, ...]]:
        short_payload = aliased(RawApiPayload)
        detail_payload = aliased(RawApiPayload)
        statement = (
            select(
                VacancySnapshot.id.label("snapshot_id"),
                VacancySnapshot.vacancy_id.label("vacancy_id"),
                Vacancy.hh_vacancy_id.label("hh_vacancy_id"),
                VacancySnapshot.snapshot_type.label("snapshot_type"),
                VacancySnapshot.captured_at.label("captured_at"),
                VacancySnapshot.crawl_run_id.label("crawl_run_id"),
                VacancySnapshot.short_hash.label("short_hash"),
                VacancySnapshot.detail_hash.label("detail_hash"),
                VacancySnapshot.short_payload_ref_id.label("short_payload_ref_id"),
                VacancySnapshot.detail_payload_ref_id.label("detail_payload_ref_id"),
                short_payload.payload_hash.label("short_payload_hash"),
                detail_payload.payload_hash.label("detail_payload_hash"),
                VacancySnapshot.change_reason.label("change_reason"),
            )
            .join(Vacancy, Vacancy.id == VacancySnapshot.vacancy_id)
            .outerjoin(short_payload, short_payload.id == VacancySnapshot.short_payload_ref_id)
            .outerjoin(detail_payload, detail_payload.id == VacancySnapshot.detail_payload_ref_id)
            .order_by(
                VacancySnapshot.snapshot_type,
                VacancySnapshot.captured_at,
                VacancySnapshot.id,
            )
        )
        return cast(Select[tuple[Any, ...]], statement)

    @staticmethod
    def _vacancy_seen_event_statement() -> Select[tuple[Any, ...]]:
        statement = (
            select(
                VacancySeenEvent.id.label("seen_event_id"),
                VacancySeenEvent.vacancy_id.label("vacancy_id"),
                Vacancy.hh_vacancy_id.label("hh_vacancy_id"),
                VacancySeenEvent.crawl_run_id.label("crawl_run_id"),
                VacancySeenEvent.crawl_partition_id.label("crawl_partition_id"),
                VacancySeenEvent.seen_at.label("seen_at"),
                VacancySeenEvent.list_position.label("list_position"),
                VacancySeenEvent.short_hash.label("short_hash"),
                VacancySeenEvent.short_payload_ref_id.label("short_payload_ref_id"),
                RawApiPayload.payload_hash.label("short_payload_hash"),
            )
            .join(Vacancy, Vacancy.id == VacancySeenEvent.vacancy_id)
            .outerjoin(RawApiPayload, RawApiPayload.id == VacancySeenEvent.short_payload_ref_id)
            .order_by(VacancySeenEvent.seen_at, VacancySeenEvent.id)
        )
        return cast(Select[tuple[Any, ...]], statement)

    @staticmethod
    def _vacancy_current_state_statement() -> Select[tuple[Any, ...]]:
        statement = (
            select(
                VacancyCurrentState.vacancy_id.label("vacancy_id"),
                Vacancy.hh_vacancy_id.label("hh_vacancy_id"),
                VacancyCurrentState.first_seen_at.label("first_seen_at"),
                VacancyCurrentState.last_seen_at.label("last_seen_at"),
                VacancyCurrentState.seen_count.label("seen_count"),
                VacancyCurrentState.consecutive_missing_runs.label("consecutive_missing_runs"),
                VacancyCurrentState.is_probably_inactive.label("is_probably_inactive"),
                VacancyCurrentState.last_seen_run_id.label("last_seen_run_id"),
                VacancyCurrentState.last_short_hash.label("last_short_hash"),
                VacancyCurrentState.last_detail_hash.label("last_detail_hash"),
                VacancyCurrentState.last_detail_fetched_at.label("last_detail_fetched_at"),
                VacancyCurrentState.detail_fetch_status.label("detail_fetch_status"),
                VacancyCurrentState.updated_at.label("updated_at"),
            )
            .join(Vacancy, Vacancy.id == VacancyCurrentState.vacancy_id)
            .order_by(VacancyCurrentState.updated_at, VacancyCurrentState.vacancy_id)
        )
        return cast(Select[tuple[Any, ...]], statement)

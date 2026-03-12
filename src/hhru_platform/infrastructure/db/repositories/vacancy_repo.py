from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hhru_platform.application.dto import (
    NormalizedVacancyShortRecord,
    StoredVacancyReference,
    VacancyUpsertResult,
)
from hhru_platform.infrastructure.db.models.area import Area as AreaModel
from hhru_platform.infrastructure.db.models.vacancy import Vacancy as VacancyModel

UPSERT_BATCH_SIZE = 1000


class SqlAlchemyVacancyRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_many(
        self,
        records: Sequence[NormalizedVacancyShortRecord],
    ) -> VacancyUpsertResult:
        deduplicated_records = _deduplicate_records(records)
        if not deduplicated_records:
            return VacancyUpsertResult(created_count=0, vacancies=[])

        hh_vacancy_ids = [record.hh_vacancy_id for record in deduplicated_records]
        existing_ids = set(
            self._session.scalars(
                select(VacancyModel.hh_vacancy_id).where(VacancyModel.hh_vacancy_id.in_(hh_vacancy_ids))
            )
        )
        area_ids_by_hh_id = self._load_area_ids(deduplicated_records)

        for record_batch in _batched(deduplicated_records, UPSERT_BATCH_SIZE):
            insert_values = [
                {
                    "hh_vacancy_id": record.hh_vacancy_id,
                    "area_id": (
                        area_ids_by_hh_id.get(record.area_hh_id)
                        if record.area_hh_id is not None
                        else None
                    ),
                    "name_current": record.name_current,
                    "published_at": record.published_at,
                    "created_at_hh": record.created_at_hh,
                    "alternate_url": record.alternate_url,
                    "employment_type_code": record.employment_type_code,
                    "schedule_type_code": record.schedule_type_code,
                    "experience_code": record.experience_code,
                    "source_type": "hh_api",
                }
                for record in record_batch
            ]
            insert_statement = insert(VacancyModel).values(insert_values)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=[VacancyModel.hh_vacancy_id],
                set_={
                    "area_id": insert_statement.excluded.area_id,
                    "name_current": insert_statement.excluded.name_current,
                    "published_at": insert_statement.excluded.published_at,
                    "created_at_hh": insert_statement.excluded.created_at_hh,
                    "alternate_url": insert_statement.excluded.alternate_url,
                    "employment_type_code": insert_statement.excluded.employment_type_code,
                    "schedule_type_code": insert_statement.excluded.schedule_type_code,
                    "experience_code": insert_statement.excluded.experience_code,
                    "source_type": insert_statement.excluded.source_type,
                    "updated_at": func.now(),
                },
            )
            self._session.execute(upsert_statement)

        self._session.flush()

        stored_models = {
            model.hh_vacancy_id: model
            for model in self._session.scalars(
                select(VacancyModel).where(VacancyModel.hh_vacancy_id.in_(hh_vacancy_ids))
            )
        }
        stored_references = [
            StoredVacancyReference(
                id=stored_models[hh_vacancy_id].id,
                hh_vacancy_id=hh_vacancy_id,
                name_current=stored_models[hh_vacancy_id].name_current,
            )
            for hh_vacancy_id in hh_vacancy_ids
        ]
        created_count = len(hh_vacancy_ids) - len(existing_ids)
        return VacancyUpsertResult(created_count=created_count, vacancies=stored_references)

    def _load_area_ids(
        self,
        records: Sequence[NormalizedVacancyShortRecord],
    ) -> dict[str, object]:
        area_hh_ids = sorted(
            {record.area_hh_id for record in records if record.area_hh_id is not None}
        )
        if not area_hh_ids:
            return {}

        rows = self._session.execute(
            select(AreaModel.hh_area_id, AreaModel.id).where(AreaModel.hh_area_id.in_(area_hh_ids))
        )
        return {hh_area_id: area_id for hh_area_id, area_id in rows}


def _deduplicate_records(
    records: Sequence[NormalizedVacancyShortRecord],
) -> list[NormalizedVacancyShortRecord]:
    records_by_hh_id: dict[str, NormalizedVacancyShortRecord] = {}
    for record in records:
        records_by_hh_id[record.hh_vacancy_id] = record
    return list(records_by_hh_id.values())


def _batched(
    records: Sequence[NormalizedVacancyShortRecord],
    batch_size: int,
) -> list[Sequence[NormalizedVacancyShortRecord]]:
    return [records[index : index + batch_size] for index in range(0, len(records), batch_size)]

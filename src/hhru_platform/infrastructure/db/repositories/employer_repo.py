from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hhru_platform.application.dto import NormalizedEmployerReference
from hhru_platform.infrastructure.db.models.area import Area as AreaModel
from hhru_platform.infrastructure.db.models.employer import Employer as EmployerModel

UPSERT_BATCH_SIZE = 1000


class SqlAlchemyEmployerRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_many(
        self,
        records: Sequence[NormalizedEmployerReference],
    ) -> dict[str, UUID]:
        deduplicated_records = _deduplicate_records(records)
        if not deduplicated_records:
            return {}

        employer_hh_ids = [record.hh_employer_id for record in deduplicated_records]
        insertable_records = [record for record in deduplicated_records if record.name is not None]
        area_ids_by_hh_id = self._load_area_ids(insertable_records)

        for record_batch in _batched(insertable_records, UPSERT_BATCH_SIZE):
            insert_values = [
                {
                    "hh_employer_id": record.hh_employer_id,
                    "name": record.name,
                    "alternate_url": record.alternate_url,
                    "site_url": record.site_url,
                    "area_id": (
                        area_ids_by_hh_id.get(record.area_hh_id)
                        if record.area_hh_id is not None
                        else None
                    ),
                    "is_trusted": record.is_trusted,
                }
                for record in record_batch
            ]
            insert_statement = insert(EmployerModel).values(insert_values)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=[EmployerModel.hh_employer_id],
                set_={
                    "name": func.coalesce(insert_statement.excluded.name, EmployerModel.name),
                    "alternate_url": func.coalesce(
                        insert_statement.excluded.alternate_url,
                        EmployerModel.alternate_url,
                    ),
                    "site_url": func.coalesce(
                        insert_statement.excluded.site_url,
                        EmployerModel.site_url,
                    ),
                    "area_id": func.coalesce(
                        insert_statement.excluded.area_id,
                        EmployerModel.area_id,
                    ),
                    "is_trusted": func.coalesce(
                        insert_statement.excluded.is_trusted,
                        EmployerModel.is_trusted,
                    ),
                    "raw_last_seen_at": func.now(),
                    "updated_at": func.now(),
                },
            )
            self._session.execute(upsert_statement)

        self._session.flush()

        rows = self._session.execute(
            select(EmployerModel.hh_employer_id, EmployerModel.id).where(
                EmployerModel.hh_employer_id.in_(employer_hh_ids)
            )
        )
        return {hh_employer_id: employer_id for hh_employer_id, employer_id in rows}

    def _load_area_ids(
        self,
        records: Sequence[NormalizedEmployerReference],
    ) -> dict[str, UUID]:
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
    records: Sequence[NormalizedEmployerReference],
) -> list[NormalizedEmployerReference]:
    records_by_hh_id: dict[str, NormalizedEmployerReference] = {}
    for record in records:
        records_by_hh_id[record.hh_employer_id] = record
    return list(records_by_hh_id.values())


def _batched(
    records: Sequence[NormalizedEmployerReference],
    batch_size: int,
) -> list[Sequence[NormalizedEmployerReference]]:
    return [records[index : index + batch_size] for index in range(0, len(records), batch_size)]

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hhru_platform.application.dto import DictionaryPersistSummary
from hhru_platform.domain.entities.area import Area
from hhru_platform.infrastructure.db.models.area import Area as AreaModel
from hhru_platform.infrastructure.normalization.dictionary_normalizers import (
    NormalizedAreaRecord,
)

UPSERT_BATCH_SIZE = 1000


class SqlAlchemyAreaRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def list_active_root_areas(self) -> list[Area]:
        statement = (
            select(AreaModel)
            .where(
                AreaModel.is_active.is_(True),
                AreaModel.parent_area_id.is_(None),
            )
            .order_by(
                AreaModel.level.asc().nullsfirst(),
                AreaModel.path_text.asc().nullslast(),
                AreaModel.name.asc(),
                AreaModel.hh_area_id.asc(),
            )
        )
        return [self._to_entity(model) for model in self._session.scalars(statement)]

    def list_active_children_by_hh_area_id(self, parent_hh_area_id: str) -> list[Area]:
        parent_area_id = self._session.scalar(
            select(AreaModel.id).where(AreaModel.hh_area_id == parent_hh_area_id)
        )
        if parent_area_id is None:
            return []

        statement = (
            select(AreaModel)
            .where(
                AreaModel.is_active.is_(True),
                AreaModel.parent_area_id == parent_area_id,
            )
            .order_by(
                AreaModel.level.asc().nullsfirst(),
                AreaModel.path_text.asc().nullslast(),
                AreaModel.name.asc(),
                AreaModel.hh_area_id.asc(),
            )
        )
        return [self._to_entity(model) for model in self._session.scalars(statement)]

    def upsert_many(self, records: Sequence[NormalizedAreaRecord]) -> DictionaryPersistSummary:
        if not records:
            return DictionaryPersistSummary(created_count=0, updated_count=0, deactivated_count=0)

        hh_area_ids = [record.hh_area_id for record in records]
        existing_ids = set(
            self._session.scalars(
                select(AreaModel.hh_area_id).where(AreaModel.hh_area_id.in_(hh_area_ids))
            )
        )

        for record_batch in _batched(records, UPSERT_BATCH_SIZE):
            insert_values = [
                {
                    "hh_area_id": record.hh_area_id,
                    "name": record.name,
                    "level": record.level,
                    "path_text": record.path_text,
                    "is_active": True,
                }
                for record in record_batch
            ]
            insert_statement = insert(AreaModel).values(insert_values)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=[AreaModel.hh_area_id],
                set_={
                    "name": insert_statement.excluded.name,
                    "level": insert_statement.excluded.level,
                    "path_text": insert_statement.excluded.path_text,
                    "is_active": True,
                    "updated_at": func.now(),
                },
            )
            self._session.execute(upsert_statement)
        self._session.flush()

        stored_models = {
            model.hh_area_id: model
            for model in self._session.scalars(
                select(AreaModel).where(AreaModel.hh_area_id.in_(hh_area_ids))
            )
        }
        hh_to_internal_id = {hh_area_id: model.id for hh_area_id, model in stored_models.items()}

        for record in records:
            parent_area_id = (
                hh_to_internal_id.get(record.parent_hh_area_id)
                if record.parent_hh_area_id is not None
                else None
            )
            stored_models[record.hh_area_id].parent_area_id = parent_area_id

        deactivated_count = int(
            self._session.scalar(
                select(func.count())
                .select_from(AreaModel)
                .where(
                    AreaModel.is_active.is_(True),
                    AreaModel.hh_area_id.not_in(hh_area_ids),
                )
            )
            or 0
        )
        self._session.execute(
            update(AreaModel)
            .where(
                AreaModel.is_active.is_(True),
                AreaModel.hh_area_id.not_in(hh_area_ids),
            )
            .values(is_active=False, updated_at=func.now())
        )
        self._session.flush()

        created_count = len(hh_area_ids) - len(existing_ids)
        return DictionaryPersistSummary(
            created_count=created_count,
            updated_count=len(hh_area_ids) - created_count,
            deactivated_count=deactivated_count,
        )

    @staticmethod
    def _to_entity(model: AreaModel) -> Area:
        return Area(
            id=model.id,
            hh_area_id=model.hh_area_id,
            name=model.name,
            parent_area_id=model.parent_area_id,
            level=model.level,
            path_text=model.path_text,
            is_active=model.is_active,
        )


def _batched(
    records: Sequence[NormalizedAreaRecord],
    batch_size: int,
) -> list[Sequence[NormalizedAreaRecord]]:
    return [records[index : index + batch_size] for index in range(0, len(records), batch_size)]

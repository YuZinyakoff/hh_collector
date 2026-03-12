from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hhru_platform.application.dto import DictionaryPersistSummary
from hhru_platform.infrastructure.db.models.professional_role import (
    ProfessionalRole as ProfessionalRoleModel,
)
from hhru_platform.infrastructure.normalization.dictionary_normalizers import (
    NormalizedProfessionalRoleRecord,
)

UPSERT_BATCH_SIZE = 1000


class SqlAlchemyProfessionalRoleRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_many(
        self,
        records: Sequence[NormalizedProfessionalRoleRecord],
    ) -> DictionaryPersistSummary:
        if not records:
            return DictionaryPersistSummary(created_count=0, updated_count=0, deactivated_count=0)

        hh_role_ids = [record.hh_professional_role_id for record in records]
        existing_ids = set(
            self._session.scalars(
                select(ProfessionalRoleModel.hh_professional_role_id).where(
                    ProfessionalRoleModel.hh_professional_role_id.in_(hh_role_ids)
                )
            )
        )

        for record_batch in _batched(records, UPSERT_BATCH_SIZE):
            insert_values = [
                {
                    "hh_professional_role_id": record.hh_professional_role_id,
                    "name": record.name,
                    "category_name": record.category_name,
                    "is_active": True,
                }
                for record in record_batch
            ]
            insert_statement = insert(ProfessionalRoleModel).values(insert_values)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=[ProfessionalRoleModel.hh_professional_role_id],
                set_={
                    "name": insert_statement.excluded.name,
                    "category_name": insert_statement.excluded.category_name,
                    "is_active": True,
                    "updated_at": func.now(),
                },
            )
            self._session.execute(upsert_statement)

        deactivated_count = int(
            self._session.scalar(
                select(func.count())
                .select_from(ProfessionalRoleModel)
                .where(
                    ProfessionalRoleModel.is_active.is_(True),
                    ProfessionalRoleModel.hh_professional_role_id.not_in(hh_role_ids),
                )
            )
            or 0
        )
        self._session.execute(
            update(ProfessionalRoleModel)
            .where(
                ProfessionalRoleModel.is_active.is_(True),
                ProfessionalRoleModel.hh_professional_role_id.not_in(hh_role_ids),
            )
            .values(is_active=False, updated_at=func.now())
        )
        self._session.flush()

        created_count = len(hh_role_ids) - len(existing_ids)
        return DictionaryPersistSummary(
            created_count=created_count,
            updated_count=len(hh_role_ids) - created_count,
            deactivated_count=deactivated_count,
        )


def _batched(
    records: Sequence[NormalizedProfessionalRoleRecord],
    batch_size: int,
) -> list[Sequence[NormalizedProfessionalRoleRecord]]:
    return [records[index : index + batch_size] for index in range(0, len(records), batch_size)]

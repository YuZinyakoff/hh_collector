from __future__ import annotations

from collections.abc import Mapping, Sequence
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hhru_platform.infrastructure.db.models.professional_role import (
    ProfessionalRole as ProfessionalRoleModel,
)
from hhru_platform.infrastructure.db.models.vacancy_professional_role import (
    VacancyProfessionalRole as VacancyProfessionalRoleModel,
)


class SqlAlchemyVacancyProfessionalRoleRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_links(
        self,
        *,
        vacancy_role_hh_ids: Mapping[UUID, Sequence[str]],
    ) -> None:
        role_ids_by_hh_id = self._load_role_ids(vacancy_role_hh_ids)
        insert_values = [
            {
                "vacancy_id": vacancy_id,
                "professional_role_id": role_ids_by_hh_id[role_hh_id],
            }
            for vacancy_id, role_hh_ids in vacancy_role_hh_ids.items()
            for role_hh_id in dict.fromkeys(role_hh_ids)
            if role_hh_id in role_ids_by_hh_id
        ]
        if not insert_values:
            return

        insert_statement = insert(VacancyProfessionalRoleModel).values(insert_values)
        self._session.execute(
            insert_statement.on_conflict_do_nothing(
                index_elements=[
                    VacancyProfessionalRoleModel.vacancy_id,
                    VacancyProfessionalRoleModel.professional_role_id,
                ]
            )
        )
        self._session.flush()

    def _load_role_ids(
        self,
        vacancy_role_hh_ids: Mapping[UUID, Sequence[str]],
    ) -> dict[str, UUID]:
        role_hh_ids = sorted(
            {
                role_hh_id
                for role_hh_ids in vacancy_role_hh_ids.values()
                for role_hh_id in role_hh_ids
            }
        )
        if not role_hh_ids:
            return {}

        rows = self._session.execute(
            select(ProfessionalRoleModel.hh_professional_role_id, ProfessionalRoleModel.id).where(
                ProfessionalRoleModel.hh_professional_role_id.in_(role_hh_ids)
            )
        )
        return {hh_role_id: role_id for hh_role_id, role_id in rows}

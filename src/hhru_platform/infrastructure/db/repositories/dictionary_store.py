from __future__ import annotations

from typing import Any

from hhru_platform.application.dto import DictionaryPersistSummary
from hhru_platform.infrastructure.db.repositories.area_repo import SqlAlchemyAreaRepository
from hhru_platform.infrastructure.db.repositories.professional_role_repo import (
    SqlAlchemyProfessionalRoleRepository,
)
from hhru_platform.infrastructure.normalization.dictionary_normalizers import (
    normalize_areas,
    normalize_professional_roles,
)


class SqlAlchemyDictionaryStore:
    def __init__(
        self,
        area_repository: SqlAlchemyAreaRepository,
        professional_role_repository: SqlAlchemyProfessionalRoleRepository,
    ) -> None:
        self._area_repository = area_repository
        self._professional_role_repository = professional_role_repository

    def sync(self, dictionary_name: str, payload_json: Any) -> DictionaryPersistSummary:
        if dictionary_name == "areas":
            return self._area_repository.upsert_many(normalize_areas(payload_json))

        if dictionary_name == "professional_roles":
            return self._professional_role_repository.upsert_many(
                normalize_professional_roles(payload_json)
            )

        raise ValueError(f"Unsupported dictionary_name {dictionary_name!r}")

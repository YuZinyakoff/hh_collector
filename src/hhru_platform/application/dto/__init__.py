"""Application DTOs."""

from hhru_platform.application.dto.dictionary_sync import (
    SUPPORTED_DICTIONARY_NAMES,
    DictionaryFetchResponse,
    DictionaryPersistSummary,
)
from hhru_platform.application.dto.vacancy_search import (
    NormalizedVacancySearchPage,
    NormalizedVacancyShortRecord,
    ObservedVacancyRecord,
    StoredVacancyReference,
    VacancySearchResponse,
    VacancyUpsertResult,
)

__all__ = [
    "DictionaryFetchResponse",
    "DictionaryPersistSummary",
    "NormalizedVacancySearchPage",
    "NormalizedVacancyShortRecord",
    "ObservedVacancyRecord",
    "SUPPORTED_DICTIONARY_NAMES",
    "StoredVacancyReference",
    "VacancySearchResponse",
    "VacancyUpsertResult",
]

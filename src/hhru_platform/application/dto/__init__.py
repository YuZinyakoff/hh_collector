"""Application DTOs."""

from hhru_platform.application.dto.dictionary_sync import (
    SUPPORTED_DICTIONARY_NAMES,
    DictionaryFetchResponse,
    DictionaryPersistSummary,
)
from hhru_platform.application.dto.vacancy_detail import (
    NormalizedVacancyDetail,
    VacancyDetailResponse,
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
    "NormalizedVacancyDetail",
    "NormalizedVacancySearchPage",
    "NormalizedVacancyShortRecord",
    "ObservedVacancyRecord",
    "SUPPORTED_DICTIONARY_NAMES",
    "StoredVacancyReference",
    "VacancyDetailResponse",
    "VacancySearchResponse",
    "VacancyUpsertResult",
]

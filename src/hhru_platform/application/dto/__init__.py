"""Application DTOs."""

from hhru_platform.application.dto.dictionary_sync import (
    SUPPORTED_DICTIONARY_NAMES,
    DictionaryFetchResponse,
    DictionaryPersistSummary,
)

__all__ = [
    "DictionaryFetchResponse",
    "DictionaryPersistSummary",
    "SUPPORTED_DICTIONARY_NAMES",
]

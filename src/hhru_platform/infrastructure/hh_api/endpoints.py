from __future__ import annotations

from dataclasses import dataclass

from hhru_platform.application.dto import SUPPORTED_DICTIONARY_NAMES


@dataclass(slots=True, frozen=True)
class DictionaryEndpointDefinition:
    name: str
    endpoint: str


DICTIONARY_ENDPOINTS: dict[str, DictionaryEndpointDefinition] = {
    "areas": DictionaryEndpointDefinition(name="areas", endpoint="/areas"),
    "professional_roles": DictionaryEndpointDefinition(
        name="professional_roles",
        endpoint="/professional_roles",
    ),
}

VACANCY_SEARCH_ENDPOINT = "/vacancies"
VACANCY_DETAIL_ENDPOINT_TEMPLATE = "/vacancies/{vacancy_id}"


def get_dictionary_endpoint(dictionary_name: str) -> DictionaryEndpointDefinition:
    try:
        return DICTIONARY_ENDPOINTS[dictionary_name]
    except KeyError as error:
        supported = ", ".join(SUPPORTED_DICTIONARY_NAMES)
        raise ValueError(
            f"Unsupported dictionary_name {dictionary_name!r}. Expected one of: {supported}."
        ) from error


def get_vacancy_detail_endpoint(hh_vacancy_id: str) -> str:
    return VACANCY_DETAIL_ENDPOINT_TEMPLATE.format(vacancy_id=hh_vacancy_id)

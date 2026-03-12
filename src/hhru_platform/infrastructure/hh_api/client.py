from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from json import JSONDecodeError
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from hhru_platform.application.dto import (
    DictionaryFetchResponse,
    VacancyDetailResponse,
    VacancySearchResponse,
)
from hhru_platform.infrastructure.hh_api.endpoints import (
    VACANCY_SEARCH_ENDPOINT,
    get_dictionary_endpoint,
    get_vacancy_detail_endpoint,
)


@dataclass(slots=True, frozen=True)
class _JSONGetResponse:
    method: str
    params_json: dict[str, object]
    request_headers_json: dict[str, str]
    status_code: int
    headers: dict[str, str]
    latency_ms: int
    requested_at: datetime
    response_received_at: datetime | None
    payload_json: object | None
    error_type: str | None
    error_message: str | None


class HHApiClient:
    def __init__(
        self,
        *,
        base_url: str = "https://api.hh.ru",
        timeout: float = 30.0,
        user_agent: str = "hhru-platform/0.1",
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._user_agent = user_agent

    def fetch_dictionary(self, dictionary_name: str) -> DictionaryFetchResponse:
        endpoint = get_dictionary_endpoint(dictionary_name)
        response = self._perform_get(endpoint.endpoint, params_json={})
        return DictionaryFetchResponse(
            dictionary_name=dictionary_name,
            endpoint=endpoint.endpoint,
            method=response.method,
            params_json=response.params_json,
            request_headers_json=response.request_headers_json,
            status_code=response.status_code,
            headers=response.headers,
            latency_ms=response.latency_ms,
            requested_at=response.requested_at,
            response_received_at=response.response_received_at,
            payload_json=response.payload_json,
            error_type=response.error_type,
            error_message=response.error_message,
        )

    def search_vacancies(self, params_json: Mapping[str, object]) -> VacancySearchResponse:
        response = self._perform_get(VACANCY_SEARCH_ENDPOINT, params_json=params_json)
        return VacancySearchResponse(
            endpoint=VACANCY_SEARCH_ENDPOINT,
            method=response.method,
            params_json=response.params_json,
            request_headers_json=response.request_headers_json,
            status_code=response.status_code,
            headers=response.headers,
            latency_ms=response.latency_ms,
            requested_at=response.requested_at,
            response_received_at=response.response_received_at,
            payload_json=response.payload_json,
            error_type=response.error_type,
            error_message=response.error_message,
        )

    def fetch_vacancy_detail(self, hh_vacancy_id: str) -> VacancyDetailResponse:
        endpoint = get_vacancy_detail_endpoint(hh_vacancy_id)
        response = self._perform_get(endpoint, params_json={})
        return VacancyDetailResponse(
            endpoint=endpoint,
            method=response.method,
            params_json=response.params_json,
            request_headers_json=response.request_headers_json,
            status_code=response.status_code,
            headers=response.headers,
            latency_ms=response.latency_ms,
            requested_at=response.requested_at,
            response_received_at=response.response_received_at,
            payload_json=response.payload_json,
            error_type=response.error_type,
            error_message=response.error_message,
        )

    def _perform_get(
        self,
        endpoint: str,
        *,
        params_json: Mapping[str, object],
    ) -> _JSONGetResponse:
        request_headers = {
            "Accept": "application/json",
            "User-Agent": self._user_agent,
        }
        request = Request(
            url=_build_url(self._base_url, endpoint, params_json),
            headers=request_headers,
            method="GET",
        )

        requested_at = datetime.now(UTC)
        try:
            with urlopen(request, timeout=self._timeout) as response:
                body = response.read()
                response_headers = {key.lower(): value for key, value in response.headers.items()}
                status_code = int(response.status)
        except HTTPError as error:
            body = error.read()
            response_headers = {key.lower(): value for key, value in error.headers.items()}
            status_code = int(error.code)
        except URLError as error:
            response_received_at = datetime.now(UTC)
            return _JSONGetResponse(
                method="GET",
                params_json=dict(params_json),
                request_headers_json=request_headers,
                status_code=0,
                headers={},
                latency_ms=_latency_ms(requested_at, response_received_at),
                requested_at=requested_at,
                response_received_at=response_received_at,
                payload_json=None,
                error_type=error.__class__.__name__,
                error_message=str(error.reason),
            )

        response_received_at = datetime.now(UTC)
        payload_json, error_type, error_message = _decode_json_body(body)
        return _JSONGetResponse(
            method="GET",
            params_json=dict(params_json),
            request_headers_json=request_headers,
            status_code=status_code,
            headers=response_headers,
            latency_ms=_latency_ms(requested_at, response_received_at),
            requested_at=requested_at,
            response_received_at=response_received_at,
            payload_json=payload_json,
            error_type=error_type,
            error_message=error_message,
        )


def _decode_json_body(body: bytes) -> tuple[object | None, str | None, str | None]:
    if not body:
        return None, None, None

    try:
        return json.loads(body.decode("utf-8")), None, None
    except (UnicodeDecodeError, JSONDecodeError) as error:
        return None, error.__class__.__name__, str(error)


def _latency_ms(started_at: datetime, finished_at: datetime) -> int:
    return int((finished_at - started_at).total_seconds() * 1000)


def _build_url(base_url: str, endpoint: str, params_json: Mapping[str, object]) -> str:
    query_params: list[tuple[str, str | int | float]] = []
    for key, value in params_json.items():
        if value is None:
            continue

        if isinstance(value, Sequence) and not isinstance(value, str):
            for item in value:
                if item is None:
                    continue
                query_params.append((key, _query_param_value(item)))
            continue

        query_params.append((key, _query_param_value(value)))

    query_string = urlencode(query_params, doseq=True)
    if not query_string:
        return f"{base_url}{endpoint}"
    return f"{base_url}{endpoint}?{query_string}"


def _query_param_value(value: object) -> str | int | float:
    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, str | int | float):
        return value

    return str(value)

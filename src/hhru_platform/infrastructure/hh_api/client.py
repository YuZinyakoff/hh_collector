from __future__ import annotations

import json
from datetime import UTC, datetime
from json import JSONDecodeError
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from hhru_platform.application.dto import DictionaryFetchResponse
from hhru_platform.infrastructure.hh_api.endpoints import get_dictionary_endpoint


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
        request_headers = {
            "Accept": "application/json",
            "User-Agent": self._user_agent,
        }
        request = Request(
            url=f"{self._base_url}{endpoint.endpoint}",
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
            return DictionaryFetchResponse(
                dictionary_name=dictionary_name,
                endpoint=endpoint.endpoint,
                method="GET",
                params_json={},
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
        return DictionaryFetchResponse(
            dictionary_name=dictionary_name,
            endpoint=endpoint.endpoint,
            method="GET",
            params_json={},
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

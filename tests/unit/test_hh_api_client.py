from __future__ import annotations

from http.client import IncompleteRead
from io import BytesIO
from urllib.error import HTTPError

import pytest

from hhru_platform.infrastructure.hh_api import client as hh_client_module
from hhru_platform.infrastructure.hh_api.client import HHApiClient
from hhru_platform.infrastructure.hh_api.user_agent import (
    HHApiUserAgentValidationError,
    validate_live_vacancy_search_user_agent,
)


@pytest.mark.parametrize(
    ("user_agent", "expected_reason"),
    [
        ("hhru-platform/0.1", "placeholder value is not allowed"),
        (
            "hhru-platform/0.1 (contact: change-me@example.com)",
            "placeholder value is not allowed",
        ),
        (
            "collector/1.0 (contact: your_email@example.com)",
            "placeholder contact address is not allowed",
        ),
    ],
)
def test_validate_live_vacancy_search_user_agent_rejects_placeholders(
    user_agent: str,
    expected_reason: str,
) -> None:
    with pytest.raises(HHApiUserAgentValidationError, match=expected_reason):
        validate_live_vacancy_search_user_agent(user_agent)


def test_hh_api_client_rejects_invalid_user_agent_before_live_search_request() -> None:
    client = HHApiClient(
        base_url="https://example.test",
        user_agent="hhru-platform/0.1 (contact: change-me@example.com)",
    )

    with pytest.raises(HHApiUserAgentValidationError, match="Invalid HH API User-Agent"):
        client.search_vacancies({"text": "python"})


def test_hh_api_client_includes_bearer_token_when_configured(monkeypatch) -> None:
    captured_headers: dict[str, str] = {}

    class FakeResponse:
        status = 200
        headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return b'{"items":[],"found":0,"pages":0}'

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    def fake_urlopen(request, timeout):
        del timeout
        captured_headers.update(dict(request.header_items()))
        return FakeResponse()

    monkeypatch.setattr(hh_client_module, "urlopen", fake_urlopen)

    client = HHApiClient(
        base_url="https://example.test",
        user_agent="hhru-platform/0.1 (contact: ops@example.com)",
        application_token="secret-token",
    )

    response = client.search_vacancies({"text": "python"})

    assert response.status_code == 200
    assert captured_headers["Authorization"] == "Bearer secret-token"


def test_hh_api_client_extracts_captcha_error_type_from_http_error_payload(monkeypatch) -> None:
    def fake_urlopen(request, timeout):
        del request, timeout
        raise HTTPError(
            url="https://example.test/vacancies",
            code=403,
            msg="Forbidden",
            hdrs={"Content-Type": "application/json"},
            fp=BytesIO(
                b'{"errors":[{"type":"captcha_required","captcha_url":"https://captcha.hh.ru"}]}'
            ),
        )

    monkeypatch.setattr(hh_client_module, "urlopen", fake_urlopen)

    client = HHApiClient(
        base_url="https://example.test",
        user_agent="hhru-platform/0.1 (contact: ops@example.com)",
    )

    response = client.search_vacancies({"text": "python"})

    assert response.status_code == 403
    assert response.error_type == "captcha_required"
    assert response.error_message == "https://captcha.hh.ru"
    assert response.payload_json == {
        "errors": [{"type": "captcha_required", "captcha_url": "https://captcha.hh.ru"}]
    }


def test_hh_api_client_classifies_incomplete_read_as_transport_error(monkeypatch) -> None:
    class FakeResponse:
        status = 200
        headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            raise IncompleteRead(b'{"items":[]', 32)

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    def fake_urlopen(request, timeout):
        del request, timeout
        return FakeResponse()

    monkeypatch.setattr(hh_client_module, "urlopen", fake_urlopen)

    client = HHApiClient(
        base_url="https://example.test",
        user_agent="hhru-platform/0.1 (contact: ops@example.com)",
    )

    response = client.search_vacancies({"text": "python"})

    assert response.status_code == 0
    assert response.error_type == "IncompleteRead"
    assert response.payload_json is None
    assert response.error_message is not None

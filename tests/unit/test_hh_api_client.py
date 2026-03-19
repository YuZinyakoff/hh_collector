from __future__ import annotations

import pytest

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

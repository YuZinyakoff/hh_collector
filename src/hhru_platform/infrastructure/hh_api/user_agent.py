from __future__ import annotations

import re

_EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_DISALLOWED_EXACT_VALUES = frozenset(
    {
        "hhru-platform/0.1",
        "hhru-platform/0.1 (contact: change-me@example.com)",
    }
)
_DISALLOWED_MARKERS = (
    "change-me@example.com",
    "your_email@example.com",
    "your-email@example.com",
)


class HHApiUserAgentValidationError(ValueError):
    def __init__(self, user_agent: str, reason: str) -> None:
        normalized_user_agent = user_agent.strip() or "<empty>"
        super().__init__(
            "Invalid HH API User-Agent for live vacancy search: "
            f"{reason}. Replace it with a real identifier and contact, "
            "for example 'hhru-platform/0.1 (contact: ops@your-domain.example)'. "
            f"Current value: {normalized_user_agent!r}"
        )
        self.user_agent = normalized_user_agent
        self.reason = reason


def validate_live_vacancy_search_user_agent(user_agent: str) -> str:
    normalized_user_agent = user_agent.strip()
    lowered_user_agent = normalized_user_agent.lower()

    if not normalized_user_agent:
        raise HHApiUserAgentValidationError(user_agent, "value must not be empty")
    if lowered_user_agent in _DISALLOWED_EXACT_VALUES:
        raise HHApiUserAgentValidationError(user_agent, "placeholder value is not allowed")
    if any(marker in lowered_user_agent for marker in _DISALLOWED_MARKERS):
        raise HHApiUserAgentValidationError(
            user_agent,
            "placeholder contact address is not allowed",
        )
    if _EMAIL_PATTERN.search(normalized_user_agent) is None:
        raise HHApiUserAgentValidationError(
            user_agent,
            "value must include a real contact address",
        )

    return normalized_user_agent


def is_live_vacancy_search_user_agent_valid(user_agent: str) -> bool:
    try:
        validate_live_vacancy_search_user_agent(user_agent)
    except HHApiUserAgentValidationError:
        return False
    return True

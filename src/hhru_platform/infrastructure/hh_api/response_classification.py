from __future__ import annotations

from collections.abc import Mapping, Sequence

CAPTCHA_REQUIRED_ERROR_TYPE = "captcha_required"


def extract_api_error(payload_json: object | None) -> tuple[str | None, str | None]:
    if not isinstance(payload_json, Mapping):
        return None, None

    errors = payload_json.get("errors")
    if not isinstance(errors, Sequence) or isinstance(errors, str | bytes) or not errors:
        return None, None

    first_error = errors[0]
    if not isinstance(first_error, Mapping):
        return None, None

    error_type = first_error.get("type")
    if not isinstance(error_type, str):
        return None, None

    normalized_error_type = error_type.strip()
    if not normalized_error_type:
        return None, None

    return normalized_error_type, _extract_api_error_message(first_error)


def is_captcha_response(*, status_code: int, error_type: str | None) -> bool:
    return status_code == 403 and error_type == CAPTCHA_REQUIRED_ERROR_TYPE


def is_transport_response(*, status_code: int, error_type: str | None) -> bool:
    del error_type
    return status_code == 0


def build_response_error_message(
    *,
    error_type: str | None,
    error_message: str | None,
    default_message: str,
) -> str:
    if error_type is None:
        return error_message or default_message
    return f"{error_type}: {error_message or default_message}"


def _extract_api_error_message(first_error: Mapping[object, object]) -> str | None:
    for key in (
        "value",
        "description",
        "message",
        "captcha_url",
        "captcha_url_with_backurl",
    ):
        value = first_error.get(key)
        if isinstance(value, str):
            normalized_value = value.strip()
            if normalized_value:
                return normalized_value
    return None

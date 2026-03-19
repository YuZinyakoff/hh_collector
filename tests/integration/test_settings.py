from hhru_platform.config.settings import Settings
from hhru_platform.infrastructure.hh_api.client import HHApiClient


def test_settings_build_database_url() -> None:
    settings = Settings(
        db_user="user",
        db_password="pass",
        db_host="db",
        db_port=5432,
        db_name="name",
    )

    assert settings.database_url == "postgresql+psycopg://user:pass@db:5432/name"


def test_hh_api_client_from_settings_uses_runtime_configuration() -> None:
    settings = Settings(
        hh_api_base_url="https://example.test/",
        hh_api_timeout_seconds=12.5,
        hh_api_user_agent="hhru-platform/0.1 (contact: ops@example.com)",
    )

    client = HHApiClient.from_settings(settings)

    assert client._base_url == "https://example.test"
    assert client._timeout == 12.5
    assert client._user_agent == "hhru-platform/0.1 (contact: ops@example.com)"

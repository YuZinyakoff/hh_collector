import os
import subprocess
import sys
from pathlib import Path

from hhru_platform.config.settings import Settings
from hhru_platform.infrastructure.hh_api.client import HHApiClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]


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
        hh_api_application_token="secret-token",
    )

    client = HHApiClient.from_settings(settings)

    assert client._base_url == "https://example.test"
    assert client._timeout == 12.5
    assert client._user_agent == "hhru-platform/0.1 (contact: ops@example.com)"
    assert client._application_token == "secret-token"


def test_settings_ignore_compose_only_env_variables_from_env_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    for key in tuple(os.environ):
        if key.startswith("HHRU_"):
            monkeypatch.delenv(key, raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "HHRU_ENV=production",
                "HHRU_DB_HOST=db.internal",
                "HHRU_DB_PORT=5432",
                "HHRU_DB_NAME=hhru_platform",
                "HHRU_DB_USER=hhru",
                "HHRU_DB_PASSWORD=secret",
                "HHRU_METRICS_HOST=0.0.0.0",
                "HHRU_METRICS_PORT=8001",
                "HHRU_DB_BIND_HOST=127.0.0.1",
                "HHRU_REDIS_BIND_HOST=127.0.0.1",
                "HHRU_PROMETHEUS_PORT=9090",
                "HHRU_GRAFANA_ADMIN_PASSWORD=admin",
            )
        ),
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.env == "production"
    assert settings.database_url == "postgresql+psycopg://hhru:secret@db.internal:5432/hhru_platform"
    assert settings.metrics_host == "0.0.0.0"
    assert settings.metrics_port == 8001


def test_health_check_cli_works_with_current_env_example(tmp_path: Path) -> None:
    result = _run_cli_with_env_example(tmp_path, "health-check")

    assert result.returncode == 0
    assert "env=local" in result.stdout
    assert (
        "database_url=postgresql+psycopg://hhru:change-me@localhost:5432/hhru_platform"
        in result.stdout
    )
    assert "metrics_endpoint=http://0.0.0.0:8001/metrics" in result.stdout


def test_run_once_help_cli_works_with_current_env_example(tmp_path: Path) -> None:
    result = _run_cli_with_env_example(tmp_path, "run-once", "--help")

    assert result.returncode == 0
    assert "usage: hhru-platform run-once" in result.stdout
    assert "--sync-dictionaries {yes,no}" in result.stdout
    assert "--detail-limit DETAIL_LIMIT" in result.stdout


def _run_cli_with_env_example(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env_file = tmp_path / ".env"
    env_file.write_text(
        (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("HHRU_")
    }
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")

    return subprocess.run(
        [sys.executable, "-m", "hhru_platform.interfaces.cli.main", *args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

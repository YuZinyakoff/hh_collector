from hhru_platform.config.settings import Settings


def test_settings_build_database_url() -> None:
    settings = Settings(
        db_user="user",
        db_password="pass",
        db_host="db",
        db_port=5432,
        db_name="name",
    )

    assert settings.database_url == "postgresql+psycopg://user:pass@db:5432/name"

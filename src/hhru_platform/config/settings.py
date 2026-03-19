from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="HHRU_",
        case_sensitive=False,
        extra="ignore",
    )

    env: str = "local"
    log_level: str = "INFO"
    log_format: str = "json"
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "hhru_platform"
    db_user: str = "hhru"
    db_password: str = Field(default="hhru", repr=False)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    hh_api_base_url: str = "https://api.hh.ru"
    hh_api_timeout_seconds: float = 30.0
    hh_api_user_agent: str = "hhru-platform/0.1 (contact: change-me@example.com)"
    metrics_host: str = "0.0.0.0"
    metrics_port: int = 8001
    metrics_state_path: str = ".state/metrics/metrics.json"
    backup_dir: str = ".state/backups"
    backup_retention_days: int = 7
    backup_prefix: str = "hhru-platform"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

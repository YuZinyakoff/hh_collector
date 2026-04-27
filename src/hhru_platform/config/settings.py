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
    hh_api_application_token: str | None = Field(default=None, repr=False)
    metrics_host: str = "0.0.0.0"
    metrics_port: int = 8001
    metrics_state_path: str = ".state/metrics/metrics.json"
    backup_dir: str = ".state/backups"
    backup_retention_days: int = 7
    backup_prefix: str = "hhru-platform"
    backup_restore_drill_target_db: str = "hhru_platform_restore_drill"
    backup_restore_drill_drop_existing: bool = True
    housekeeping_raw_api_payload_retention_days: int = 90
    housekeeping_vacancy_snapshot_retention_days: int = 0
    housekeeping_finished_crawl_run_retention_days: int = 90
    housekeeping_detail_fetch_attempt_retention_days: int = 180
    housekeeping_report_artifact_retention_days: int = 30
    housekeeping_report_artifact_dir: str = ".state/reports/detail-payload-study"
    housekeeping_archive_dir: str = ".state/archive/retention"
    housekeeping_archive_offsite_url: str = ""
    housekeeping_archive_offsite_root: str = "/hhru-platform"
    housekeeping_archive_offsite_username: str | None = None
    housekeeping_archive_offsite_password: str | None = Field(default=None, repr=False)
    housekeeping_archive_offsite_bearer_token: str | None = Field(default=None, repr=False)
    housekeeping_archive_offsite_timeout_seconds: float = 60.0
    housekeeping_delete_limit_per_target: int = 10_000
    detail_worker_batch_size: int = 100
    detail_worker_interval_seconds: float = 300.0
    detail_worker_include_inactive: bool = False
    detail_worker_triggered_by: str = "detail-worker"
    detail_worker_retry_cooldown_seconds: int = 3600
    detail_worker_max_retry_cooldown_seconds: int = 86400

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

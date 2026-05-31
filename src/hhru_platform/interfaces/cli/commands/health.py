from __future__ import annotations

import argparse

from hhru_platform.config.settings import Settings, get_settings
from hhru_platform.infrastructure.hh_api.user_agent import (
    HHApiUserAgentValidationError,
    validate_live_vacancy_search_user_agent,
)


def register_health_commands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser("health-check", help="Show basic platform configuration status.")
    parser.set_defaults(handler=handle_health_check)


def handle_health_check(_: argparse.Namespace) -> int:
    settings = get_settings()
    user_agent_validation_error: str | None = None
    try:
        validate_live_vacancy_search_user_agent(settings.hh_api_user_agent)
    except HHApiUserAgentValidationError as error:
        user_agent_validation_error = str(error)

    print(f"env={settings.env}")
    print(f"database_url={settings.database_url}")
    print(f"redis_url={settings.redis_url}")
    print(f"hh_api_base_url={settings.hh_api_base_url}")
    print(f"hh_api_timeout_seconds={settings.hh_api_timeout_seconds}")
    print(f"hh_api_user_agent={settings.hh_api_user_agent}")
    print(
        "hh_api_application_token_configured="
        f"{'yes' if bool(settings.hh_api_application_token) else 'no'}"
    )
    print(
        "hh_api_default_auth_mode="
        f"{'application_token' if bool(settings.hh_api_application_token) else 'anonymous'}"
    )
    print(
        "hh_api_user_agent_live_search_valid="
        f"{'no' if user_agent_validation_error is not None else 'yes'}"
    )
    print(f"hh_api_user_agent_live_search_error={user_agent_validation_error or '-'}")
    print(f"metrics_state_path={settings.metrics_state_path}")
    print(f"metrics_endpoint=http://{settings.metrics_host}:{settings.metrics_port}/metrics")
    print(
        "alert_webhook_endpoint="
        f"http://{settings.alert_webhook_host}:{settings.alert_webhook_port}/alertmanager"
    )
    print(f"alert_telegram_configured={'yes' if _is_alert_telegram_configured(settings) else 'no'}")
    print(f"backup_dir={settings.backup_dir}")
    print(f"backup_retention_days={settings.backup_retention_days}")
    print(f"backup_restore_drill_target_db={settings.backup_restore_drill_target_db}")
    print(
        "backup_restore_drill_drop_existing="
        f"{'yes' if settings.backup_restore_drill_drop_existing else 'no'}"
    )
    backup_offsite_auth_mode = _backup_offsite_auth_mode(settings)
    print(f"backup_offsite_backend={settings.backup_offsite_backend}")
    print(
        "backup_offsite_configured="
        f"{'yes' if _is_backup_offsite_configured(settings, backup_offsite_auth_mode) else 'no'}"
    )
    print(f"backup_offsite_url={_backup_offsite_url(settings) or '-'}")
    print(f"backup_offsite_root={settings.backup_offsite_root}")
    print(f"backup_offsite_auth_mode={backup_offsite_auth_mode}")
    print(f"backup_offsite_timeout_seconds={settings.backup_offsite_timeout_seconds}")
    print(f"backup_offsite_chunk_size_bytes={settings.backup_offsite_chunk_size_bytes}")
    print(f"backup_offsite_s3_endpoint_url={settings.backup_offsite_s3_endpoint_url or '-'}")
    print(f"backup_offsite_s3_bucket={settings.backup_offsite_s3_bucket or '-'}")
    print(f"backup_offsite_s3_region={settings.backup_offsite_s3_region}")
    print(
        "backup_offsite_s3_access_key_configured="
        f"{'yes' if settings.backup_offsite_s3_access_key_id else 'no'}"
    )
    print(
        "housekeeping_raw_api_payload_retention_days="
        f"{settings.housekeeping_raw_api_payload_retention_days}"
    )
    print(
        "housekeeping_vacancy_snapshot_retention_days="
        f"{settings.housekeeping_vacancy_snapshot_retention_days}"
    )
    print(
        "housekeeping_finished_crawl_run_retention_days="
        f"{settings.housekeeping_finished_crawl_run_retention_days}"
    )
    print(
        "housekeeping_detail_fetch_attempt_retention_days="
        f"{settings.housekeeping_detail_fetch_attempt_retention_days}"
    )
    print(
        "housekeeping_report_artifact_retention_days="
        f"{settings.housekeeping_report_artifact_retention_days}"
    )
    print(f"housekeeping_report_artifact_dir={settings.housekeeping_report_artifact_dir}")
    print(f"housekeeping_archive_dir={settings.housekeeping_archive_dir}")
    auth_mode = "none"
    if settings.housekeeping_archive_offsite_bearer_token:
        auth_mode = "bearer"
    elif (
        settings.housekeeping_archive_offsite_username
        and settings.housekeeping_archive_offsite_password
    ):
        auth_mode = "basic"
    print(
        "housekeeping_archive_offsite_configured="
        f"{'yes' if _is_archive_offsite_configured(settings, auth_mode) else 'no'}"
    )
    print(f"housekeeping_archive_offsite_url={settings.housekeeping_archive_offsite_url or '-'}")
    print(f"housekeeping_archive_offsite_root={settings.housekeeping_archive_offsite_root}")
    print(f"housekeeping_archive_offsite_auth_mode={auth_mode}")
    print(
        "housekeeping_archive_offsite_timeout_seconds="
        f"{settings.housekeeping_archive_offsite_timeout_seconds}"
    )
    print(f"housekeeping_delete_limit_per_target={settings.housekeeping_delete_limit_per_target}")
    print(f"research_archive_dir={settings.research_archive_dir}")
    research_archive_offsite_auth_mode = _research_archive_offsite_auth_mode(settings)
    print(f"research_archive_offsite_backend={settings.research_archive_offsite_backend}")
    print(
        "research_archive_offsite_configured="
        f"{'yes' if _is_research_archive_offsite_configured(settings) else 'no'}"
    )
    print(f"research_archive_offsite_url={_research_archive_offsite_url(settings) or '-'}")
    print(f"research_archive_offsite_root={settings.research_archive_offsite_root}")
    print(f"research_archive_offsite_auth_mode={research_archive_offsite_auth_mode}")
    print(
        "research_archive_offsite_s3_endpoint_url="
        f"{_research_archive_s3_endpoint_url(settings) or '-'}"
    )
    print(f"research_archive_offsite_s3_bucket={_research_archive_s3_bucket(settings) or '-'}")
    print(f"research_archive_offsite_s3_region={_research_archive_s3_region(settings)}")
    print(
        "research_archive_offsite_s3_access_key_configured="
        f"{'yes' if _research_archive_s3_access_key_id(settings) else 'no'}"
    )
    print(f"detail_worker_batch_size={settings.detail_worker_batch_size}")
    print(f"detail_worker_interval_seconds={settings.detail_worker_interval_seconds}")
    print(
        "detail_worker_include_inactive="
        f"{'yes' if settings.detail_worker_include_inactive else 'no'}"
    )
    print(f"detail_worker_triggered_by={settings.detail_worker_triggered_by}")
    print(f"detail_worker_retry_cooldown_seconds={settings.detail_worker_retry_cooldown_seconds}")
    print(
        "detail_worker_max_retry_cooldown_seconds="
        f"{settings.detail_worker_max_retry_cooldown_seconds}"
    )
    print(f"detail_worker_lease_seconds={settings.detail_worker_lease_seconds}")
    return 0


def _is_archive_offsite_configured(settings: Settings, auth_mode: str) -> bool:
    return bool(settings.housekeeping_archive_offsite_url) and auth_mode != "none"


def _is_backup_offsite_configured(settings: Settings, auth_mode: str) -> bool:
    if settings.backup_offsite_backend.strip().lower() == "s3":
        return bool(
            settings.backup_offsite_s3_endpoint_url
            and settings.backup_offsite_s3_bucket
            and settings.backup_offsite_s3_access_key_id
            and settings.backup_offsite_s3_secret_access_key
        )
    return bool(_backup_offsite_url(settings)) and auth_mode != "none"


def _backup_offsite_url(settings: Settings) -> str:
    if settings.backup_offsite_backend.strip().lower() == "s3":
        endpoint_url = settings.backup_offsite_s3_endpoint_url.strip().rstrip("/")
        bucket = settings.backup_offsite_s3_bucket.strip()
        if endpoint_url and bucket:
            return f"{endpoint_url}/{bucket}"
        return endpoint_url
    return settings.backup_offsite_url or settings.housekeeping_archive_offsite_url


def _backup_offsite_auth_mode(settings: Settings) -> str:
    if settings.backup_offsite_backend.strip().lower() == "s3":
        if (
            settings.backup_offsite_s3_access_key_id
            and settings.backup_offsite_s3_secret_access_key
        ):
            return "s3"
        return "none"
    if settings.backup_offsite_bearer_token or settings.housekeeping_archive_offsite_bearer_token:
        return "bearer"
    if (settings.backup_offsite_username and settings.backup_offsite_password) or (
        settings.housekeeping_archive_offsite_username
        and settings.housekeeping_archive_offsite_password
    ):
        return "basic"
    return "none"


def _is_research_archive_offsite_configured(settings: Settings) -> bool:
    if settings.research_archive_offsite_backend.strip().lower() != "s3":
        return False
    return bool(
        _research_archive_s3_endpoint_url(settings)
        and _research_archive_s3_bucket(settings)
        and _research_archive_s3_access_key_id(settings)
        and _research_archive_s3_secret_access_key(settings)
    )


def _research_archive_offsite_url(settings: Settings) -> str:
    if settings.research_archive_offsite_backend.strip().lower() != "s3":
        return ""
    endpoint_url = _research_archive_s3_endpoint_url(settings).strip().rstrip("/")
    bucket = _research_archive_s3_bucket(settings).strip()
    if endpoint_url and bucket:
        return f"{endpoint_url}/{bucket}"
    return endpoint_url


def _research_archive_offsite_auth_mode(settings: Settings) -> str:
    if settings.research_archive_offsite_backend.strip().lower() != "s3":
        return "none"
    if _research_archive_s3_access_key_id(settings) and _research_archive_s3_secret_access_key(
        settings
    ):
        return "s3"
    return "none"


def _research_archive_s3_endpoint_url(settings: Settings) -> str:
    return (
        settings.research_archive_offsite_s3_endpoint_url or settings.backup_offsite_s3_endpoint_url
    )


def _research_archive_s3_bucket(settings: Settings) -> str:
    return settings.research_archive_offsite_s3_bucket or settings.backup_offsite_s3_bucket


def _research_archive_s3_region(settings: Settings) -> str:
    return (
        settings.research_archive_offsite_s3_region or settings.backup_offsite_s3_region or "ru-1"
    )


def _research_archive_s3_access_key_id(settings: Settings) -> str | None:
    return (
        settings.research_archive_offsite_s3_access_key_id
        or settings.backup_offsite_s3_access_key_id
    )


def _research_archive_s3_secret_access_key(settings: Settings) -> str | None:
    return (
        settings.research_archive_offsite_s3_secret_access_key
        or settings.backup_offsite_s3_secret_access_key
    )


def _is_alert_telegram_configured(settings: Settings) -> bool:
    return bool(settings.alert_telegram_bot_token and settings.alert_telegram_chat_id)

from __future__ import annotations

import hashlib
import os
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from hhru_platform.config.settings import Settings, get_settings

BACKUP_DRILL_REQUIRED_TABLES: Final[tuple[str, ...]] = (
    "crawl_run",
    "crawl_partition",
    "raw_api_payload",
    "vacancy_snapshot",
    "vacancy_current_state",
)


@dataclass(slots=True, frozen=True)
class BackupArchiveSummary:
    backup_file: Path
    size_bytes: int
    sha256: str
    archive_entry_count: int


@dataclass(slots=True, frozen=True)
class RestoreDrillSummary:
    backup_file: Path
    target_db: str
    archive_entry_count: int
    required_tables: tuple[str, ...]
    present_table_count: int
    schema_verified: bool


class BackupToolError(RuntimeError):
    """Raised when backup tooling fails or returns malformed output."""


class BackupService:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        repo_root: Path | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._runner = runner
        self._repo_root = repo_root or Path(__file__).resolve().parents[4]

    def create_backup(self) -> BackupArchiveSummary:
        result = self._run_command(
            ["bash", str(self._script_path("backup_postgres.sh"))],
            env=self._database_environment(
                HHRU_BACKUP_DIR=self._settings.backup_dir,
                HHRU_BACKUP_RETENTION_DAYS=str(self._settings.backup_retention_days),
                HHRU_BACKUP_PREFIX=self._settings.backup_prefix,
            ),
        )
        payload = _parse_key_value_output(result.stdout)
        backup_file_value = payload.get("backup_file")
        if not backup_file_value:
            raise BackupToolError("backup script did not report backup_file")
        return self.inspect_backup_file(Path(backup_file_value))

    def inspect_backup_file(self, backup_file: Path | str) -> BackupArchiveSummary:
        backup_path = Path(backup_file).expanduser()
        if not backup_path.is_file():
            raise BackupToolError(f"backup file not found: {backup_path}")

        archive_result = self._run_command(["pg_restore", "--list", str(backup_path)])
        archive_entry_count = _count_archive_entries(archive_result.stdout)
        if archive_entry_count <= 0:
            raise BackupToolError(f"backup archive has no restorable entries: {backup_path}")

        return BackupArchiveSummary(
            backup_file=backup_path,
            size_bytes=backup_path.stat().st_size,
            sha256=_sha256(backup_path),
            archive_entry_count=archive_entry_count,
        )

    def restore_to_target_db(
        self,
        *,
        backup_file: Path | str,
        target_db: str,
        drop_existing: bool = True,
    ) -> RestoreDrillSummary:
        backup_summary = self.inspect_backup_file(backup_file)
        target_db_name = target_db.strip()
        if not target_db_name:
            raise ValueError("target_db must not be empty")

        result = self._run_command(
            ["bash", str(self._script_path("restore_postgres.sh"))],
            env=self._database_environment(
                HHRU_RESTORE_FILE=str(backup_summary.backup_file),
                HHRU_RESTORE_TARGET_DB=target_db_name,
                HHRU_RESTORE_DROP_TARGET_DB="yes" if drop_existing else "no",
                HHRU_RESTORE_CONFIRM="yes",
            ),
        )
        payload = _parse_key_value_output(result.stdout)
        restored_target_db = payload.get("restored_to_db")
        if restored_target_db != target_db_name:
            raise BackupToolError(
                "restore script reported unexpected target database: "
                f"{restored_target_db or '-'}"
            )

        present_table_count = self._count_present_tables(target_db_name)
        summary = RestoreDrillSummary(
            backup_file=backup_summary.backup_file,
            target_db=target_db_name,
            archive_entry_count=backup_summary.archive_entry_count,
            required_tables=BACKUP_DRILL_REQUIRED_TABLES,
            present_table_count=present_table_count,
            schema_verified=present_table_count == len(BACKUP_DRILL_REQUIRED_TABLES),
        )
        if not summary.schema_verified:
            raise BackupToolError(
                "restore drill verification failed: "
                f"expected {len(summary.required_tables)} core tables, "
                f"found {summary.present_table_count} in {summary.target_db}"
            )
        return summary

    def _count_present_tables(self, database_name: str) -> int:
        expressions = " + ".join(
            (
                f"(CASE WHEN to_regclass('public.{table_name}') IS NULL "
                "THEN 0 ELSE 1 END)"
            )
            for table_name in BACKUP_DRILL_REQUIRED_TABLES
        )
        query = f"SELECT {expressions};"
        result = self._run_command(
            [
                "psql",
                "--host",
                self._settings.db_host,
                "--port",
                str(self._settings.db_port),
                "--username",
                self._settings.db_user,
                "--dbname",
                database_name,
                "--no-align",
                "--tuples-only",
                "--command",
                query,
            ],
            env=self._database_environment(),
        )
        output = result.stdout.strip()
        try:
            return int(output)
        except ValueError as error:
            raise BackupToolError(
                f"restore verification returned non-integer output for {database_name}: {output}"
            ) from error

    def _run_command(
        self,
        args: Sequence[str],
        *,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        try:
            result = self._runner(
                list(args),
                check=False,
                capture_output=True,
                text=True,
                cwd=str(self._repo_root),
                env=env,
            )
        except FileNotFoundError as error:
            raise BackupToolError(f"backup tool not found: {args[0]}") from error
        if result.returncode != 0:
            details = (result.stderr or result.stdout).strip() or "unknown error"
            raise BackupToolError(f"{args[0]} failed: {details}")
        return result

    def _database_environment(self, **extra_env: str) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "HHRU_DB_HOST": self._settings.db_host,
                "HHRU_DB_PORT": str(self._settings.db_port),
                "HHRU_DB_NAME": self._settings.db_name,
                "HHRU_DB_USER": self._settings.db_user,
                "HHRU_DB_PASSWORD": self._settings.db_password,
            }
        )
        env.update(extra_env)
        return env

    def _script_path(self, script_name: str) -> Path:
        script_path = self._repo_root / "scripts" / "backup" / script_name
        if not script_path.is_file():
            raise BackupToolError(f"backup script not found: {script_path}")
        return script_path


def _count_archive_entries(payload: str) -> int:
    return sum(
        1
        for line in payload.splitlines()
        if line.strip() and not line.lstrip().startswith(";")
    )


def _parse_key_value_output(payload: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

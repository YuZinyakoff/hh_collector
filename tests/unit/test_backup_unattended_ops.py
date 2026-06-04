from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DAILY_SCRIPT = REPO_ROOT / "scripts" / "ops" / "run_daily_backup.sh"
RESTORE_SCRIPT = REPO_ROOT / "scripts" / "ops" / "run_weekly_backup_restore_drill.sh"
NOTIFY_SCRIPT = REPO_ROOT / "scripts" / "ops" / "notify_systemd_failure.sh"
SYSTEMD_ROOT = REPO_ROOT / "deploy" / "systemd"


def test_daily_backup_driver_runs_verified_non_destructive_pipeline(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "docker-calls.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_DOCKER_CALLS"
if [[ "$*" == *" run-backup "* ]]; then
  printf 'status=succeeded\\nbackup_file=.state/backups/fake.dump\\n'
elif [[ "$*" == *" sync-backup-offsite "* ]]; then
  printf 'status=succeeded\\nscanned_backup_count=1\\n'
  printf 'backup=/app/.state/backups/fake.dump uploaded=yes skipped=no\\n'
else
  printf 'status=succeeded\\n'
fi
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls_file),
        "HHRU_BACKUP_DAILY_ROOT_DIR": str(tmp_path),
    }

    result = subprocess.run(
        ["bash", str(DAILY_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "operation=daily_backup status=succeeded" in result.stdout
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert [call.split()[6] for call in calls] == [
        "run-backup",
        "verify-backup-file",
        "sync-backup-offsite",
        "verify-backup-offsite",
    ]
    assert all("cleanup-backup-offsite" not in call for call in calls)


def test_daily_backup_driver_fails_before_offsite_when_local_verify_fails(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "docker-calls.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_DOCKER_CALLS"
if [[ "$*" == *" run-backup "* ]]; then
  printf 'status=succeeded\\nbackup_file=.state/backups/fake.dump\\n'
elif [[ "$*" == *" verify-backup-file "* ]]; then
  printf 'broken backup\\n' >&2
  exit 1
fi
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls_file),
        "HHRU_BACKUP_DAILY_ROOT_DIR": str(tmp_path),
    }

    result = subprocess.run(
        ["bash", str(DAILY_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 1
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert [call.split()[6] for call in calls] == [
        "run-backup",
        "verify-backup-file",
    ]


def test_weekly_restore_driver_uses_verified_offsite_backup_and_drops_drill_db(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / ".state" / "backups"
    backup_dir.mkdir(parents=True)
    backup_file = backup_dir / "fake.dump"
    Path(f"{backup_file}.manifest.json").write_text("{}", encoding="utf-8")
    Path(f"{backup_file}.offsite.verified.json").write_text("{}", encoding="utf-8")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "docker-calls.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_DOCKER_CALLS"
printf 'status=succeeded\\n'
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls_file),
        "HHRU_BACKUP_RESTORE_DRILL_ROOT_DIR": str(tmp_path),
        "HHRU_BACKUP_RESTORE_DRILL_BACKUP_DIR": ".state/backups",
    }

    result = subprocess.run(
        ["bash", str(RESTORE_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "operation=weekly_backup_restore_drill status=succeeded" in result.stdout
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert "run-backup-offsite-restore-drill" in calls[0]
    assert ".state/backups/fake.dump" in calls[0]
    assert calls[1].startswith("compose exec -T postgres ")
    assert "dropdb" in calls[1]


def test_weekly_restore_driver_attempts_cleanup_after_restore_failure(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / ".state" / "backups"
    backup_dir.mkdir(parents=True)
    backup_file = backup_dir / "fake.dump"
    Path(f"{backup_file}.manifest.json").write_text("{}", encoding="utf-8")
    Path(f"{backup_file}.offsite.verified.json").write_text("{}", encoding="utf-8")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "docker-calls.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_DOCKER_CALLS"
if [[ "$*" == *" run-backup-offsite-restore-drill "* ]]; then
  exit 1
fi
printf 'status=succeeded\\n'
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls_file),
        "HHRU_BACKUP_RESTORE_DRILL_ROOT_DIR": str(tmp_path),
    }

    result = subprocess.run(
        ["bash", str(RESTORE_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 1
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert "run-backup-offsite-restore-drill" in calls[0]
    assert calls[1].startswith("compose exec -T postgres ")
    assert "dropdb" in calls[1]


def test_systemd_failure_notifier_posts_synthetic_alertmanager_payload(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    curl_calls = tmp_path / "curl-calls.log"
    fake_curl = fake_bin / "curl"
    fake_curl.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" > "$FAKE_CURL_CALLS"
""",
        encoding="utf-8",
    )
    fake_curl.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_CURL_CALLS": str(curl_calls),
    }

    result = subprocess.run(
        ["bash", str(NOTIFY_SCRIPT), "hhru-daily-backup.service"],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    curl_call = curl_calls.read_text(encoding="utf-8")
    assert "HHRUPlatformSystemdUnitFailed" in curl_call
    assert "hhru-daily-backup.service" in curl_call
    assert "http://127.0.0.1:8010/alertmanager" in curl_call


def test_unattended_systemd_units_have_expected_safe_schedules_and_failure_hooks() -> None:
    daily_service = (SYSTEMD_ROOT / "hhru-daily-backup.service").read_text()
    daily_timer = (SYSTEMD_ROOT / "hhru-daily-backup.timer").read_text()
    restore_service = (
        SYSTEMD_ROOT / "hhru-weekly-backup-restore-drill.service"
    ).read_text()
    restore_timer = (SYSTEMD_ROOT / "hhru-weekly-backup-restore-drill.timer").read_text()
    archive_service = (SYSTEMD_ROOT / "hhru-research-archive.service").read_text()
    daily_script = DAILY_SCRIPT.read_text()
    restore_script = RESTORE_SCRIPT.read_text()
    archive_script = (
        REPO_ROOT / "scripts" / "ops" / "run_daily_research_archive.sh"
    ).read_text()

    assert "Environment=HHRU_BACKUP_DAILY_LOCAL_RETENTION_DAYS=2" in daily_service
    assert "OnCalendar=*-*-* 00:30:00 UTC" in daily_timer
    assert "OnCalendar=Sun *-*-* 06:00:00 UTC" in restore_timer
    assert "run_weekly_backup_restore_drill.sh" in restore_service
    assert "OnFailure=hhru-ops-failure-notify@%n.service" in daily_service
    assert "OnFailure=hhru-ops-failure-notify@%n.service" in restore_service
    assert "OnFailure=hhru-ops-failure-notify@%n.service" in archive_service
    assert all(
        "HHRU_HEAVY_OPS_LOCK_FILE" in script
        for script in (daily_script, restore_script, archive_script)
    )
    assert "cleanup-backup-offsite" not in daily_script
    assert "apply-research-archive-housekeeping" not in archive_script

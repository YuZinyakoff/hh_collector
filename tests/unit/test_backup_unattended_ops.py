from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DAILY_SCRIPT = REPO_ROOT / "scripts" / "ops" / "run_daily_backup.sh"
DUMP_SCRIPT = REPO_ROOT / "scripts" / "backup" / "backup_postgres.sh"
RESTORE_SCRIPT = REPO_ROOT / "scripts" / "ops" / "run_weekly_backup_restore_drill.sh"
CLEANUP_SCRIPT = REPO_ROOT / "scripts" / "ops" / "run_weekly_backup_offsite_cleanup.sh"
NOTIFY_SCRIPT = REPO_ROOT / "scripts" / "ops" / "notify_systemd_failure.sh"
SYSTEMD_ROOT = REPO_ROOT / "deploy" / "systemd"


def _fake_daily_backup(tmp_path: Path) -> tuple[dict[str, str], Path, Path]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "docker-calls.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_DOCKER_CALLS"
dump=.state/backups/fake.dump
if [[ "$*" == *" run-backup "* ]]; then
  printf 'fake backup payload' > "$dump"
  printf 'status=succeeded\\nbackup_file=.state/backups/fake.dump\\n'
elif [[ "$*" == *" verify-backup-file "* ]]; then
  if [[ "${FAKE_LOCAL_VERIFY_FAILURE:-no}" == yes ]]; then
    printf 'broken backup\\n' >&2
    exit 1
  fi
  sha="$(sha256sum "$dump")"
  printf 'backup_sha256=%s\\n' "${sha%% *}"
elif [[ "$*" == *" sync-backup-offsite "* ]]; then
  if [[ "${FAKE_SYNC_FAILURE:-no}" == yes ]]; then
    printf 'S3 read timeout\\n' >&2
    exit 1
  fi
  python3 - "$dump" <<'PY'
import hashlib
import json
import sys
from pathlib import Path
dump = Path(sys.argv[1])
payload = dump.read_bytes()
manifest = {
    "backup_file": dump.name,
    "backup_size_bytes": len(payload),
    "backup_sha256": hashlib.sha256(payload).hexdigest(),
    "chunk_size_bytes": 64,
    "parts": [{}],
}
raw = json.dumps(manifest).encode()
Path(f"{dump}.manifest.json").write_bytes(raw)
receipt = {
    **manifest,
    "part_count": 1,
    "manifest_sha256": hashlib.sha256(raw).hexdigest(),
    "offsite_url": "https://example.test",
    "offsite_root": "/backups",
    "remote_backup_path": f"/backups/{dump.name}.parts",
    "remote_manifest_path": f"/backups/{dump.name}.manifest.json",
    "verified_object_count": 2,
}
Path(f"{dump}.offsite.json").write_text(json.dumps(receipt))
PY
  printf 'status=succeeded\\nscanned_backup_count=1\\n'
  printf 'backup=/app/.state/backups/fake.dump uploaded=yes skipped=no\\n'
elif [[ "$*" == *" verify-backup-offsite "* ]]; then
  cp "$dump.offsite.json" "$dump.offsite.verified.json"
  sha="$(sha256sum "$dump")"
  printf 'status=succeeded\\nbackup_sha256=%s\\n' "${sha%% *}"
else
  printf 'status=succeeded\\n'
fi
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    fake_df = fake_bin / "df"
    fake_df.write_text(
        '#!/bin/bash\nprintf "Filesystem 1-blocks Used Available Capacity Mounted on\\n'
        'test 60000000000 10000000000 %s 25%% /\\n" "$FAKE_FREE_BYTES"\n',
        encoding="utf-8",
    )
    fake_df.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls_file),
        "FAKE_FREE_BYTES": "50000000000",
        "HHRU_BACKUP_DAILY_RESERVE_BYTES": "10737418240",
        "HHRU_BACKUP_DAILY_PRUNE_VERIFIED_LOCAL_DUMPS": "true",
        "HHRU_BACKUP_DAILY_ROOT_DIR": str(tmp_path),
    }
    backup_file = tmp_path / ".state" / "backups" / "fake.dump"
    backup_file.parent.mkdir(parents=True)
    return env, calls_file, backup_file


def _run_daily_backup(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(DAILY_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )


def _backup_commands(calls_file: Path) -> list[str]:
    calls = [line.split() for line in calls_file.read_text(encoding="utf-8").splitlines()]
    return [call[call.index("app") + 1] for call in calls]


def test_daily_backup_driver_runs_verified_pipeline_and_prunes_local_dump(
    tmp_path: Path,
) -> None:
    env, calls_file, backup_file = _fake_daily_backup(tmp_path)
    result = _run_daily_backup(env)
    assert result.returncode == 0, result.stderr
    assert "operation=daily_backup status=succeeded" in result.stdout
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert _backup_commands(calls_file) == [
        "run-backup",
        "verify-backup-file",
        "sync-backup-offsite",
        "verify-backup-offsite",
    ]
    assert all("cleanup-backup-offsite" not in call for call in calls)
    assert "HHRU_BACKUP_MAX_DUMP_BYTES=39262581760" in calls[0]
    assert "--backup-file .state/backups/fake.dump" in calls[2]
    assert not backup_file.exists()
    assert not Path(f"{backup_file}.local-ready").exists()
    assert Path(f"{backup_file}.manifest.json").is_file()
    assert Path(f"{backup_file}.offsite.json").is_file()
    assert Path(f"{backup_file}.offsite.verified.json").is_file()
    prune_logs = list(
        (tmp_path / ".state" / "logs" / "backup-daily").glob(
            "*/prune-local-verified-dumps.log"
        )
    )
    assert len(prune_logs) == 1
    prune_log = prune_logs[0].read_text(encoding="utf-8")
    assert "local_prune_enabled=yes" in prune_log
    assert "local_deleted_dump_count=1" in prune_log


def test_daily_backup_driver_fails_before_offsite_when_local_verify_fails(
    tmp_path: Path,
) -> None:
    env, calls_file, backup_file = _fake_daily_backup(tmp_path)
    env["FAKE_LOCAL_VERIFY_FAILURE"] = "yes"
    result = _run_daily_backup(env)
    assert result.returncode == 1
    assert not Path(f"{backup_file}.local-ready").exists()
    assert _backup_commands(calls_file) == [
        "run-backup",
        "verify-backup-file",
    ]


def test_daily_backup_resumes_one_previously_checked_dump_instead_of_creating_another(
    tmp_path: Path,
) -> None:
    env, calls_file, backup_file = _fake_daily_backup(tmp_path)
    assert _run_daily_backup({**env, "FAKE_SYNC_FAILURE": "yes"}).returncode == 1
    assert backup_file.exists()
    assert Path(f"{backup_file}.local-ready").exists()

    calls_file.unlink()
    result = _run_daily_backup({**env, "FAKE_FREE_BYTES": "12000000000"})
    assert result.returncode == 0, result.stderr
    assert "action=resume" in result.stdout
    assert _backup_commands(calls_file) == [
        "verify-backup-file",
        "sync-backup-offsite",
        "verify-backup-offsite",
    ]
    assert not backup_file.exists()


def test_daily_backup_blocks_legacy_unconfirmed_dump(tmp_path: Path) -> None:
    env, calls_file, backup_file = _fake_daily_backup(tmp_path)
    backup_file.write_bytes(b"partial or unknown")
    result = _run_daily_backup(env)
    assert result.returncode == 1
    assert "reason=unconfirmed_local_dump" in result.stderr
    assert not calls_file.exists()
    assert backup_file.read_bytes() == b"partial or unknown"


def test_daily_backup_blocks_changed_pending_dump(tmp_path: Path) -> None:
    env, calls_file, backup_file = _fake_daily_backup(tmp_path)
    assert _run_daily_backup({**env, "FAKE_SYNC_FAILURE": "yes"}).returncode == 1
    backup_file.write_bytes(b"changed")
    calls_file.unlink()
    result = _run_daily_backup(env)
    assert result.returncode == 1
    assert "reason=local_backup_hash_mismatch" in result.stderr
    assert _backup_commands(calls_file) == ["verify-backup-file"]
    assert backup_file.read_bytes() == b"changed"


def test_daily_backup_blocks_low_space_based_on_latest_verified_size(tmp_path: Path) -> None:
    env, calls_file, backup_file = _fake_daily_backup(tmp_path)
    receipt = Path(f"{backup_file}.offsite.verified.json")
    receipt.write_text(json.dumps({"backup_size_bytes": 23084281929}), encoding="utf-8")
    result = _run_daily_backup({**env, "FAKE_FREE_BYTES": "34000000000"})
    assert result.returncode == 1
    assert "reason=insufficient_disk_space_preflight" in result.stderr
    assert not calls_file.exists()


def _run_dump_script(
    tmp_path: Path, *, dump_body: str, max_bytes: str = ""
) -> subprocess.CompletedProcess[str]:
    fake_dump = tmp_path / "pg_dump"
    fake_dump.write_text(
        "#!/bin/bash\nset -euo pipefail\n"
        'for arg in "$@"; do\n'
        '  if [[ "$arg" == --file=* ]]; then out="${arg#--file=}"; fi\n'
        "done\n"
        '[[ "$out" == *.dump.partial.* ]]\n'
        + dump_body
        + "\n",
        encoding="utf-8",
    )
    fake_dump.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "HHRU_BACKUP_DIR": str(tmp_path / "backups"),
        "HHRU_BACKUP_MAX_DUMP_BYTES": max_bytes,
        "HHRU_DB_NAME": "test",
        "HHRU_DB_USER": "test",
        "HHRU_DB_PASSWORD": "test",
    }
    return subprocess.run(
        ["bash", str(DUMP_SCRIPT)], env=env, check=False, capture_output=True, text=True,
    )


def test_dump_is_published_only_after_success(tmp_path: Path) -> None:
    result = _run_dump_script(tmp_path, dump_body='printf complete >"$out"')
    assert result.returncode == 0, result.stderr
    dump, = (tmp_path / "backups").iterdir()
    assert dump.suffix == ".dump"
    assert dump.read_bytes() == b"complete"
    assert f"backup_file={dump}" in result.stdout


def test_failed_dump_removes_partial_file(tmp_path: Path) -> None:
    result = _run_dump_script(tmp_path, dump_body='printf partial >"$out"\nexit 9')
    assert result.returncode == 9
    assert not list((tmp_path / "backups").iterdir())
    assert "backup_file=" not in result.stdout


def test_dump_cannot_grow_past_reserved_file_budget(tmp_path: Path) -> None:
    result = _run_dump_script(
        tmp_path, dump_body='dd if=/dev/zero of="$out" bs=1024 count=4 status=none',
        max_bytes="2048",
    )
    assert result.returncode != 0
    assert not list((tmp_path / "backups").iterdir())
    assert "backup_file=" not in result.stdout


def test_weekly_restore_driver_defaults_to_disk_light_integrity_drill(
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
    assert "mode=integrity" in result.stdout
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert len(calls) == 1
    assert "run-backup-offsite-integrity-drill" in calls[0]
    assert ".state/backups/fake.dump" in calls[0]
    success_markers = list(
        (tmp_path / ".state" / "logs" / "backup-restore-drill").glob(
            "*/success.env"
        )
    )
    assert len(success_markers) == 1
    success_marker = success_markers[0].read_text(encoding="utf-8")
    assert "status=succeeded" in success_marker
    assert "drill_mode=integrity" in success_marker


def test_weekly_restore_driver_full_mode_restores_and_drops_drill_db(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / ".state" / "backups"
    backup_dir.mkdir(parents=True)
    backup_file = backup_dir / "fake.dump"
    Path(f"{backup_file}.manifest.json").write_text(
        '{"backup_size_bytes": 1}',
        encoding="utf-8",
    )
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
  printf 'status=succeeded\\n'
  exit 0
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
        "HHRU_BACKUP_RESTORE_DRILL_MODE": "full",
        "HHRU_BACKUP_RESTORE_DRILL_FULL_REQUIRED_FREE_MULTIPLIER": "1",
        "HHRU_BACKUP_RESTORE_DRILL_FULL_RESERVE_BYTES": "0",
    }

    result = subprocess.run(
        ["bash", str(RESTORE_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "disk_preflight_status=passed" in result.stdout
    assert "mode=full" in result.stdout
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert "run-backup-offsite-restore-drill" in calls[0]
    assert calls[1].startswith("compose exec -T postgres ")
    assert "dropdb" in calls[1]


def test_weekly_restore_driver_full_mode_fails_before_restore_when_disk_is_too_small(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / ".state" / "backups"
    backup_dir.mkdir(parents=True)
    backup_file = backup_dir / "fake.dump"
    Path(f"{backup_file}.manifest.json").write_text(
        '{"backup_size_bytes": 999999999999999999}',
        encoding="utf-8",
    )
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
        "HHRU_BACKUP_RESTORE_DRILL_MODE": "full",
        "HHRU_BACKUP_RESTORE_DRILL_FULL_REQUIRED_FREE_MULTIPLIER": "1",
        "HHRU_BACKUP_RESTORE_DRILL_FULL_RESERVE_BYTES": "0",
    }

    result = subprocess.run(
        ["bash", str(RESTORE_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 1
    assert "reason=insufficient_disk_space_preflight" in result.stderr
    assert not calls_file.exists()


def test_weekly_backup_offsite_cleanup_driver_dry_runs_by_default(
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
printf 'status=succeeded\\n'
printf 'apply=no\\n'
printf 'delete_candidate_count=7\\n'
printf 'deleted_generation_count=0\\n'
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls_file),
        "HHRU_BACKUP_OFFSITE_CLEANUP_ROOT_DIR": str(tmp_path),
    }

    result = subprocess.run(
        ["bash", str(CLEANUP_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "operation=weekly_backup_offsite_cleanup status=succeeded" in result.stdout
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert len(calls) == 1
    assert "cleanup-backup-offsite" in calls[0]
    assert "--keep-latest 2" in calls[0]
    assert "--keep-weekly 0" in calls[0]
    assert "--apply" not in calls[0]


def test_weekly_backup_offsite_cleanup_driver_can_apply_after_recent_restore_drill(
    tmp_path: Path,
) -> None:
    restore_log_dir = (
        tmp_path
        / ".state"
        / "logs"
        / "backup-restore-drill"
        / "20260614T061441Z-1"
    )
    restore_log_dir.mkdir(parents=True)
    (restore_log_dir / "success.env").write_text(
        "status=succeeded\nrun_id=20260614T061441Z-1\n",
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "docker-calls.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_DOCKER_CALLS"
printf 'status=succeeded\\n'
printf 'apply=yes\\n'
printf 'delete_candidate_count=0\\n'
printf 'deleted_generation_count=7\\n'
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls_file),
        "HHRU_BACKUP_OFFSITE_CLEANUP_ROOT_DIR": str(tmp_path),
        "HHRU_BACKUP_OFFSITE_CLEANUP_APPLY": "true",
        "HHRU_BACKUP_OFFSITE_CLEANUP_REQUIRE_RECENT_RESTORE_DRILL": "true",
    }

    result = subprocess.run(
        ["bash", str(CLEANUP_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "restore_drill_success_marker=" in result.stdout
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert len(calls) == 1
    assert "cleanup-backup-offsite" in calls[0]
    assert "--apply" in calls[0]


def test_weekly_backup_offsite_cleanup_requires_recent_restore_marker_when_enabled(
    tmp_path: Path,
) -> None:
    env = {
        **os.environ,
        "HHRU_BACKUP_OFFSITE_CLEANUP_ROOT_DIR": str(tmp_path),
        "HHRU_BACKUP_OFFSITE_CLEANUP_REQUIRE_RECENT_RESTORE_DRILL": "true",
    }

    result = subprocess.run(
        ["bash", str(CLEANUP_SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 1
    assert "reason=restore_log_root_not_found" in result.stderr


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
    cleanup_service = (
        SYSTEMD_ROOT / "hhru-weekly-backup-offsite-cleanup.service"
    ).read_text()
    cleanup_timer = (
        SYSTEMD_ROOT / "hhru-weekly-backup-offsite-cleanup.timer"
    ).read_text()
    archive_service = (SYSTEMD_ROOT / "hhru-research-archive.service").read_text()
    daily_script = DAILY_SCRIPT.read_text()
    restore_script = RESTORE_SCRIPT.read_text()
    cleanup_script = CLEANUP_SCRIPT.read_text()
    archive_script = (
        REPO_ROOT / "scripts" / "ops" / "run_daily_research_archive.sh"
    ).read_text()

    assert "Environment=HHRU_BACKUP_DAILY_LOCAL_RETENTION_DAYS=1" in daily_service
    assert "OnCalendar=*-*-* 00:30:00 UTC" in daily_timer
    assert "OnCalendar=Sun *-*-* 06:00:00 UTC" in restore_timer
    assert "OnCalendar=Sun *-*-* 08:30:00 UTC" in cleanup_timer
    assert "run_weekly_backup_restore_drill.sh" in restore_service
    assert "run_weekly_backup_offsite_cleanup.sh" in cleanup_service
    assert (
        "Environment=HHRU_BACKUP_OFFSITE_CLEANUP_REQUIRE_RECENT_RESTORE_DRILL=true"
        in cleanup_service
    )
    assert "OnFailure=hhru-ops-failure-notify@%n.service" in daily_service
    assert "OnFailure=hhru-ops-failure-notify@%n.service" in restore_service
    assert "OnFailure=hhru-ops-failure-notify@%n.service" in cleanup_service
    assert "OnFailure=hhru-ops-failure-notify@%n.service" in archive_service
    assert all(
        "HHRU_HEAVY_OPS_LOCK_FILE" in script
        for script in (daily_script, restore_script, cleanup_script, archive_script)
    )
    assert "cleanup-backup-offsite" not in daily_script
    assert "cleanup-backup-offsite" in cleanup_script
    assert "HHRU_RESEARCH_ARCHIVE_DAILY_HOUSEKEEPING_APPLY" in archive_script
    assert "apply-research-archive-housekeeping" in archive_script

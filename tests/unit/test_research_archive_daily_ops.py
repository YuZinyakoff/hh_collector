from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "ops" / "run_daily_research_archive.sh"


def test_daily_research_archive_driver_runs_complete_non_destructive_pipeline(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "docker-calls.log"
    export_count_file = tmp_path / "export-count"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_DOCKER_CALLS"
if [[ "$*" == *" export-research-archive "* ]]; then
  count=0
  if [[ -f "$FAKE_DOCKER_EXPORT_COUNT" ]]; then
    count="$(cat "$FAKE_DOCKER_EXPORT_COUNT")"
  fi
  count=$((count + 1))
  printf '%s\\n' "$count" > "$FAKE_DOCKER_EXPORT_COUNT"
  if (( count == 1 )); then
    printf 'total_row_count=5\\n'
  else
    printf 'total_row_count=0\\n'
  fi
elif [[ "$*" == *" audit-research-archive-coverage "* ]]; then
  printf 'status=complete\\nissue_count=0\\n'
elif [[ "$*" == *" preview-research-archive-housekeeping "* ]]; then
  printf 'status=ready\\ntotal_action_count=0\\n'
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
        "FAKE_DOCKER_EXPORT_COUNT": str(export_count_file),
        "HHRU_RESEARCH_ARCHIVE_DAILY_ROOT_DIR": str(tmp_path),
        "HHRU_RESEARCH_ARCHIVE_DAILY_MAX_EXPORT_BATCHES": "3",
    }

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "operation=daily_research_archive status=succeeded" in result.stdout
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    commands = [
        "export-research-archive",
        "export-research-archive",
        "verify-research-archive",
        "sync-research-archive-offsite",
        "verify-research-archive-offsite",
        "audit-research-archive-coverage",
        "preview-research-archive-housekeeping",
    ]
    assert [call.split()[6] for call in calls] == commands
    assert all("apply-research-archive-housekeeping" not in call for call in calls)
    assert len(list((tmp_path / ".state/logs/research-archive-daily").iterdir())) == 1


def test_daily_research_archive_driver_can_apply_housekeeping_when_enabled(
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
if [[ "$*" == *" export-research-archive "* ]]; then
  printf 'total_row_count=0\\n'
elif [[ "$*" == *" audit-research-archive-coverage "* ]]; then
  printf 'status=complete\\nissue_count=0\\n'
elif [[ "$*" == *" preview-research-archive-housekeeping "* ]]; then
  printf 'status=ready\\ntotal_action_count=0\\n'
elif [[ "$*" == *" apply-research-archive-housekeeping "* ]]; then
  printf 'status=succeeded\\ntotal_deleted_count=0\\n'
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
        "HHRU_RESEARCH_ARCHIVE_DAILY_ROOT_DIR": str(tmp_path),
        "HHRU_RESEARCH_ARCHIVE_DAILY_HOUSEKEEPING_APPLY": "true",
    }

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "step=housekeeping-apply status=succeeded" in result.stdout
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert [call.split()[6] for call in calls] == [
        "export-research-archive",
        "verify-research-archive",
        "sync-research-archive-offsite",
        "verify-research-archive-offsite",
        "audit-research-archive-coverage",
        "preview-research-archive-housekeeping",
        "apply-research-archive-housekeeping",
    ]
    assert "--apply" in calls[-1]


def test_daily_research_archive_driver_can_catch_up_and_prune_verified_chunks(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "docker-calls.log"
    apply_count_file = tmp_path / "apply-count"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_DOCKER_CALLS"
if [[ "$*" == *" export-research-archive "* ]]; then
  printf 'total_row_count=0\\n'
elif [[ "$*" == *" preview-research-archive-housekeeping "* ]]; then
  printf 'status=ready\\ntotal_action_count=1\\n'
elif [[ "$*" == *" apply-research-archive-housekeeping "* ]]; then
  count=0
  if [[ -f "$FAKE_DOCKER_APPLY_COUNT" ]]; then
    count="$(cat "$FAKE_DOCKER_APPLY_COUNT")"
  fi
  count=$((count + 1))
  printf '%s\\n' "$count" > "$FAKE_DOCKER_APPLY_COUNT"
  if (( count == 1 )); then
    printf 'status=succeeded\\ntotal_deleted_count=50000\\n'
  else
    printf 'status=succeeded\\ntotal_deleted_count=0\\n'
  fi
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
        "FAKE_DOCKER_APPLY_COUNT": str(apply_count_file),
        "HHRU_RESEARCH_ARCHIVE_DAILY_ROOT_DIR": str(tmp_path),
        "HHRU_RESEARCH_ARCHIVE_DAILY_HOUSEKEEPING_APPLY": "true",
        "HHRU_RESEARCH_ARCHIVE_DAILY_HOUSEKEEPING_MAX_APPLY_BATCHES": "3",
        "HHRU_RESEARCH_ARCHIVE_DAILY_PRUNE_VERIFIED_LOCAL_CHUNKS": "true",
        "HHRU_RESEARCH_ARCHIVE_DAILY_LOCAL_CHUNK_RETENTION_HOURS": "0",
    }

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert sum(" apply-research-archive-housekeeping " in call for call in calls) == 2
    assert sum(" prune-research-archive-local " in call for call in calls) == 1
    local_verify_call = next(
        call for call in calls if " verify-research-archive " in call
    )
    assert "--allow-offsite-only" in local_verify_call
    prune_call = next(call for call in calls if " prune-research-archive-local " in call)
    assert "--apply" in prune_call
    assert "--min-age-hours 0" in prune_call


def test_daily_research_archive_driver_passes_housekeeping_retention_overrides(
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
if [[ "$*" == *" export-research-archive "* ]]; then
  printf 'total_row_count=0\\n'
elif [[ "$*" == *" audit-research-archive-coverage "* ]]; then
  printf 'status=complete\\nissue_count=0\\n'
elif [[ "$*" == *" preview-research-archive-housekeeping "* ]]; then
  printf 'status=ready\\ntotal_action_count=0\\n'
elif [[ "$*" == *" apply-research-archive-housekeeping "* ]]; then
  printf 'status=succeeded\\ntotal_deleted_count=0\\n'
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
        "HHRU_RESEARCH_ARCHIVE_DAILY_ROOT_DIR": str(tmp_path),
        "HHRU_RESEARCH_ARCHIVE_DAILY_HOUSEKEEPING_APPLY": "true",
        "HHRU_RESEARCH_ARCHIVE_DAILY_RAW_API_PAYLOAD_RETENTION_DAYS": "14",
        "HHRU_RESEARCH_ARCHIVE_DAILY_VACANCY_SNAPSHOT_RETENTION_DAYS": "0",
        "HHRU_RESEARCH_ARCHIVE_DAILY_DETAIL_FETCH_ATTEMPT_RETENTION_DAYS": "30",
        "HHRU_RESEARCH_ARCHIVE_DAILY_FINISHED_CRAWL_RUN_RETENTION_DAYS": "30",
        "HHRU_RESEARCH_ARCHIVE_DAILY_DELETE_LIMIT_PER_TARGET": "50000",
    }

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    preview_call = next(
        call for call in calls if " preview-research-archive-housekeeping " in call
    )
    apply_call = next(
        call for call in calls if " apply-research-archive-housekeeping " in call
    )
    expected_flags = [
        "--raw-api-payload-retention-days 14",
        "--vacancy-snapshot-retention-days 0",
        "--detail-fetch-attempt-retention-days 30",
        "--finished-crawl-run-retention-days 30",
        "--delete-limit-per-target 50000",
    ]
    assert all(flag in preview_call for flag in expected_flags)
    assert all(flag in apply_call for flag in expected_flags)


def test_daily_research_archive_driver_rejects_invalid_housekeeping_retention(
    tmp_path: Path,
) -> None:
    env = {
        **os.environ,
        "HHRU_RESEARCH_ARCHIVE_DAILY_ROOT_DIR": str(tmp_path),
        "HHRU_RESEARCH_ARCHIVE_DAILY_RAW_API_PAYLOAD_RETENTION_DAYS": "not-a-number",
    }

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 2
    assert (
        "HHRU_RESEARCH_ARCHIVE_DAILY_RAW_API_PAYLOAD_RETENTION_DAYS "
        "must be a non-negative integer"
    ) in result.stderr


def test_daily_research_archive_driver_syncs_before_reporting_export_backlog(
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
printf 'total_row_count=1\\n'
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls_file),
        "HHRU_RESEARCH_ARCHIVE_DAILY_ROOT_DIR": str(tmp_path),
        "HHRU_RESEARCH_ARCHIVE_DAILY_MAX_EXPORT_BATCHES": "2",
    }

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 1
    assert "reason=max_export_batches_exhausted" in result.stderr
    calls = calls_file.read_text(encoding="utf-8").splitlines()
    assert [call.split()[6] for call in calls] == [
        "export-research-archive",
        "export-research-archive",
        "verify-research-archive",
        "sync-research-archive-offsite",
        "verify-research-archive-offsite",
    ]

from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "ops" / "run_detail_catchup_controller.sh"
SYSTEMD_ROOT = REPO_ROOT / "deploy" / "systemd"


def test_detail_catchup_controller_starts_workers_when_backlog_is_ready(
    tmp_path: Path,
) -> None:
    result, calls = _run_controller(
        tmp_path,
        active_runs=0,
        backlog_size=25,
        ready_backlog_size=25,
        running_workers=0,
        apply=True,
    )

    assert result.returncode == 0, result.stderr
    assert "action=started" in result.stdout
    assert any(
        "compose --profile ops up -d --scale detail-worker=3 detail-worker" in call
        for call in calls
    )


def test_detail_catchup_controller_stops_workers_when_backlog_is_empty(
    tmp_path: Path,
) -> None:
    result, calls = _run_controller(
        tmp_path,
        active_runs=0,
        backlog_size=0,
        ready_backlog_size=0,
        running_workers=3,
        apply=True,
    )

    assert result.returncode == 0, result.stderr
    assert "action=stopped_empty_backlog" in result.stdout
    assert any("compose --profile ops stop detail-worker" in call for call in calls)


def test_detail_catchup_controller_stops_workers_during_active_crawl_run(
    tmp_path: Path,
) -> None:
    result, calls = _run_controller(
        tmp_path,
        active_runs=1,
        backlog_size=100,
        ready_backlog_size=100,
        running_workers=3,
        apply=True,
    )

    assert result.returncode == 0, result.stderr
    assert "action=stopped_for_active_run" in result.stdout
    assert any("compose --profile ops stop detail-worker" in call for call in calls)
    assert not any("compose --profile ops up " in call for call in calls)


def test_detail_catchup_controller_is_dry_run_by_default(tmp_path: Path) -> None:
    result, calls = _run_controller(
        tmp_path,
        active_runs=0,
        backlog_size=25,
        ready_backlog_size=25,
        running_workers=0,
        apply=False,
    )

    assert result.returncode == 0, result.stderr
    assert "action=would_start" in result.stdout
    assert "dry_run_command=docker compose --profile ops up" in result.stdout
    assert not any("compose --profile ops up " in call for call in calls)


def test_detail_catchup_controller_systemd_units_are_guarded() -> None:
    service = (
        SYSTEMD_ROOT / "hhru-detail-catchup-controller.service"
    ).read_text(encoding="utf-8")
    timer = (
        SYSTEMD_ROOT / "hhru-detail-catchup-controller.timer"
    ).read_text(encoding="utf-8")

    assert "EnvironmentFile=-/etc/hhru-platform/detail-catchup-controller.env" in service
    assert "OnFailure=hhru-ops-failure-notify@%n.service" in service
    assert "SuccessExitStatus=75" in service
    assert "run_detail_catchup_controller.sh" in service
    assert "OnCalendar=*-*-* *:00/5:00 UTC" in timer
    assert "RandomizedDelaySec=30s" in timer
    assert "Persistent=true" in timer


def test_detail_catchup_controller_shell_syntax_is_valid() -> None:
    subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


def _run_controller(
    tmp_path: Path,
    *,
    active_runs: int,
    backlog_size: int,
    ready_backlog_size: int,
    running_workers: int,
    apply: bool,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "docker-calls.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_DOCKER_CALLS"
if [[ "$*" == *" psql "* && "$*" == *"from crawl_run"* ]]; then
  printf '%s\\n' "$FAKE_ACTIVE_RUNS"
  exit 0
fi
if [[ "$*" == *" psql "* && "$*" == *"first_detail_lease_expires_at"* ]]; then
  printf '%s\\n' "$FAKE_READY_BACKLOG"
  exit 0
fi
if [[ "$*" == *" psql "* && "$*" == *"from vacancy_current_state"* ]]; then
  printf '%s\\n' "$FAKE_BACKLOG"
  exit 0
fi
if [[ "$*" == *" ps "* && "$*" == *"detail-worker"* ]]; then
  for ((i = 1; i <= FAKE_RUNNING_WORKERS; i++)); do
    printf 'detail-worker-%s\\n' "$i"
  done
  exit 0
fi
exit 0
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls_file),
        "FAKE_ACTIVE_RUNS": str(active_runs),
        "FAKE_BACKLOG": str(backlog_size),
        "FAKE_READY_BACKLOG": str(ready_backlog_size),
        "FAKE_RUNNING_WORKERS": str(running_workers),
        "HHRU_DETAIL_CATCHUP_ROOT_DIR": str(tmp_path),
        "HHRU_DETAIL_CATCHUP_CONTROLLER_APPLY": "true" if apply else "false",
    }

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )
    calls = (
        calls_file.read_text(encoding="utf-8").splitlines()
        if calls_file.exists()
        else []
    )
    return result, calls

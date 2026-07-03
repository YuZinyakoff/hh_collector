from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "ops" / "run_production_search_controller.sh"
SYSTEMD_ROOT = REPO_ROOT / "deploy" / "systemd"


def test_production_search_controller_starts_due_search(tmp_path: Path) -> None:
    result, calls = _run_controller(
        tmp_path,
        active_runs=0,
        backlog_size=0,
        last_success_epoch=0,
        running_detail_workers=0,
        failed_units=0,
        free_bytes=30 * 1024 * 1024 * 1024,
        apply=True,
    )

    assert result.returncode == 0, result.stderr
    assert "action=started_search" in result.stdout
    assert "search_status=succeeded" in result.stdout
    assert any(
        "compose --profile ops run --rm app trigger-run-now" in call
        and "--detail-limit 0" in call
        and "--run-type production_weekly_sweep" in call
        for call in calls
    )


def test_production_search_controller_skips_when_not_due(tmp_path: Path) -> None:
    result, calls = _run_controller(
        tmp_path,
        active_runs=0,
        backlog_size=0,
        last_success_epoch=int(time.time()),
        running_detail_workers=0,
        failed_units=0,
        free_bytes=30 * 1024 * 1024 * 1024,
        apply=True,
    )

    assert result.returncode == 0, result.stderr
    assert "action=skipped_not_due" in result.stdout
    assert not any("trigger-run-now" in call for call in calls)


def test_production_search_controller_skips_when_detail_backlog_remains(
    tmp_path: Path,
) -> None:
    result, calls = _run_controller(
        tmp_path,
        active_runs=0,
        backlog_size=1,
        last_success_epoch=0,
        running_detail_workers=0,
        failed_units=0,
        free_bytes=30 * 1024 * 1024 * 1024,
        apply=True,
    )

    assert result.returncode == 0, result.stderr
    assert "action=skipped_backlog_not_empty" in result.stdout
    assert not any("trigger-run-now" in call for call in calls)


def test_production_search_controller_stops_detail_workers_before_search(
    tmp_path: Path,
) -> None:
    result, calls = _run_controller(
        tmp_path,
        active_runs=0,
        backlog_size=0,
        last_success_epoch=0,
        running_detail_workers=3,
        failed_units=0,
        free_bytes=30 * 1024 * 1024 * 1024,
        apply=True,
    )

    assert result.returncode == 0, result.stderr
    stop_index = next(
        index
        for index, call in enumerate(calls)
        if "compose --profile ops stop detail-worker" in call
    )
    run_index = next(index for index, call in enumerate(calls) if "trigger-run-now" in call)
    assert stop_index < run_index


def test_production_search_controller_is_dry_run_by_default(tmp_path: Path) -> None:
    result, calls = _run_controller(
        tmp_path,
        active_runs=0,
        backlog_size=0,
        last_success_epoch=0,
        running_detail_workers=0,
        failed_units=0,
        free_bytes=30 * 1024 * 1024 * 1024,
        apply=False,
    )

    assert result.returncode == 0, result.stderr
    assert "action=would_start_search" in result.stdout
    assert "dry_run_command=docker compose --profile ops run" in result.stdout
    assert not any("trigger-run-now" in call for call in calls)


def test_production_search_controller_systemd_units_are_guarded() -> None:
    service = (
        SYSTEMD_ROOT / "hhru-production-search-controller.service"
    ).read_text(encoding="utf-8")
    timer = (
        SYSTEMD_ROOT / "hhru-production-search-controller.timer"
    ).read_text(encoding="utf-8")

    assert "EnvironmentFile=-/etc/hhru-platform/production-search-controller.env" in service
    assert "OnFailure=hhru-ops-failure-notify@%n.service" in service
    assert "SuccessExitStatus=75" in service
    assert "TimeoutStartSec=infinity" in service
    assert "run_production_search_controller.sh" in service
    assert "OnCalendar=*-*-* *:17:00 UTC" in timer
    assert "RandomizedDelaySec=5m" in timer
    assert "Persistent=true" in timer


def test_production_search_controller_shell_syntax_is_valid() -> None:
    subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


def _run_controller(
    tmp_path: Path,
    *,
    active_runs: int,
    backlog_size: int,
    last_success_epoch: int,
    running_detail_workers: int,
    failed_units: int,
    free_bytes: int,
    apply: bool,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_file = tmp_path / "calls.log"
    _write_fake_docker(
        fake_bin / "docker",
        calls_file=calls_file,
        active_runs=active_runs,
        backlog_size=backlog_size,
        last_success_epoch=last_success_epoch,
        running_detail_workers=running_detail_workers,
    )
    _write_fake_systemctl(fake_bin / "systemctl", calls_file=calls_file, failed_units=failed_units)
    _write_fake_df(fake_bin / "df", calls_file=calls_file, free_bytes=free_bytes)

    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "HHRU_PRODUCTION_SEARCH_ROOT_DIR": str(tmp_path),
        "HHRU_PRODUCTION_SEARCH_CONTROLLER_APPLY": "true" if apply else "false",
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


def _write_fake_docker(
    path: Path,
    *,
    calls_file: Path,
    active_runs: int,
    backlog_size: int,
    last_success_epoch: int,
    running_detail_workers: int,
) -> None:
    path.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> {calls_file}
if [[ "$*" == *" psql "* && "$*" == *"extract(epoch from max(started_at))"* ]]; then
  printf '%s\\n' "{last_success_epoch}"
  exit 0
fi
if [[ "$*" == *" psql "* && "$*" == *"to_char(max(started_at)"* ]]; then
  printf '2026-06-30T07:38:46Z\\n'
  exit 0
fi
if [[ "$*" == *" psql "* && "$*" == *"from crawl_run"* ]]; then
  printf '%s\\n' "{active_runs}"
  exit 0
fi
if [[ "$*" == *" psql "* && "$*" == *"from vacancy_current_state"* ]]; then
  printf '%s\\n' "{backlog_size}"
  exit 0
fi
if [[ "$*" == *" ps "* && "$*" == *"detail-worker"* ]]; then
  for ((i = 1; i <= {running_detail_workers}; i++)); do
    printf 'detail-worker-%s\\n' "$i"
  done
  exit 0
fi
if [[ "$*" == *" trigger-run-now "* ]]; then
  printf 'completed trigger-run-now execution\\n'
  printf 'status=succeeded\\n'
  printf 'run_id=11111111-1111-4111-8111-111111111111\\n'
  exit 0
fi
exit 0
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_fake_systemctl(path: Path, *, calls_file: Path, failed_units: int) -> None:
    path.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
printf 'systemctl %s\\n' "$*" >> {calls_file}
for ((i = 1; i <= {failed_units}; i++)); do
  printf 'failed-unit-%s.service loaded failed failed synthetic\\n' "$i"
done
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_fake_df(path: Path, *, calls_file: Path, free_bytes: int) -> None:
    path.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
printf 'df %s\\n' "$*" >> {calls_file}
printf 'Filesystem 1B-blocks Used Available Use%% Mounted on\\n'
printf '/dev/sda1 100000000000 1 {free_bytes} 1%% /\\n'
""",
        encoding="utf-8",
    )
    path.chmod(0o755)

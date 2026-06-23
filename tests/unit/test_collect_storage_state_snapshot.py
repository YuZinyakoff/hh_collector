from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType


def _load_module() -> ModuleType:
    script_path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "ops"
        / "collect_storage_state_snapshot.py"
    )
    spec = importlib.util.spec_from_file_location("collect_storage_state_snapshot", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_collect_backup_generations_reads_manifest_and_sidecars(tmp_path: Path) -> None:
    module = _load_module()
    backup_dir = tmp_path / ".state" / "backups"
    backup_dir.mkdir(parents=True)
    dump_path = backup_dir / "hhru-platform_hhru_platform_20260623T003239Z.dump"
    dump_path.write_bytes(b"dump")
    Path(f"{dump_path}.manifest.json").write_text(
        json.dumps(
            {
                "backup_size_bytes": 123,
                "parts": [
                    {"path": "000001.part"},
                    {"path": "000002.part"},
                ],
            }
        ),
        encoding="utf-8",
    )
    Path(f"{dump_path}.offsite.json").write_text("{}", encoding="utf-8")
    Path(f"{dump_path}.offsite.verified.json").write_text("{}", encoding="utf-8")

    generations = module.collect_backup_generations(backup_dir)

    assert len(generations) == 1
    generation = generations[0]
    assert generation.dump_name == dump_path.name
    assert generation.backup_size_bytes == 123
    assert generation.part_count == 2
    assert generation.has_local_dump is True
    assert generation.has_upload_receipt is True
    assert generation.has_verified_receipt is True


def test_latest_json_event_returns_last_matching_event(tmp_path: Path) -> None:
    module = _load_module()
    log_path = tmp_path / "offsite-verify.log"
    log_path.write_text(
        "\n".join(
            [
                "noise",
                json.dumps({"event": "target", "status": "started"}),
                json.dumps({"event": "other", "status": "ignored"}),
                json.dumps({"event": "target", "status": "succeeded", "count": 2}),
            ]
        ),
        encoding="utf-8",
    )

    payload = module.latest_json_event(log_path, "target")

    assert payload == {"event": "target", "status": "succeeded", "count": 2}

import json
from pathlib import Path
from uuid import uuid4

import pytest

from hhru_platform.infrastructure.research_archive.run_tree_metadata import (
    DATA_FILE,
    MANIFEST_FILE,
    RECEIPT_FILE,
    iter_bundle_records,
    records_digest,
    sync_and_verify_bundle,
    verify_bundle,
    verify_offsite_receipt,
    write_bundle,
)

URL = "https://s3.example.test/bucket"
ROOT = "/hhru-platform/research-archive"


class MemoryRemote:
    def __init__(self, *, corrupt: bool = False) -> None:
        self.objects: dict[str, bytes] = {}
        self.corrupt = corrupt

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        self.objects[remote_path] = local_file.read_bytes()

    def download_file(self, *, local_file: Path, remote_path: str) -> None:
        local_file.write_bytes(b"corrupt" if self.corrupt else self.objects[remote_path])


def metadata_records(run_id):
    parent, child = str(uuid4()), str(uuid4())
    return [
        {
            "table": "crawl_run",
            "row": {
                "id": str(run_id),
                "status": "succeeded",
                "finished_at": "2026-09-01T00:00:00+00:00",
                "config_snapshot_json": {"policy": "v2", "values": [1, True, None]},
                "notes": "preserve technical context",
            },
        },
        {
            "table": "crawl_partition",
            "row": {
                "id": parent,
                "crawl_run_id": str(run_id),
                "parent_partition_id": None,
                "params_json": {"area": "113"},
                "last_error_message": None,
            },
        },
        {
            "table": "crawl_partition",
            "row": {
                "id": child,
                "crawl_run_id": str(run_id),
                "parent_partition_id": parent,
                "params_json": {"area": "113", "text": "engineer"},
                "retry_count": 2,
            },
        },
    ]


def test_bundle_preserves_values_and_is_unique_private(tmp_path) -> None:
    run_id = uuid4()
    records = metadata_records(run_id)
    first = write_bundle(output_dir=tmp_path, run_id=run_id, records=iter(records))
    second = write_bundle(output_dir=tmp_path, run_id=run_id, records=iter(records))
    assert first.directory != second.directory
    assert first.partition_count == 2
    assert list(iter_bundle_records(first.directory)) == records
    assert first.content_sha256 == records_digest(records)
    assert first.directory.stat().st_mode & 0o777 == 0o700
    assert (first.directory / DATA_FILE).stat().st_mode & 0o777 == 0o600
    assert (first.directory / MANIFEST_FILE).stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("problem", ["active", "other_run", "missing_parent", "cycle", "duplicate"])
def test_bundle_rejects_invalid_roots_and_graphs(tmp_path, problem) -> None:
    run_id = uuid4()
    records = metadata_records(run_id)
    if problem == "active":
        records[0]["row"]["status"] = "created"
    elif problem == "other_run":
        records[2]["row"]["crawl_run_id"] = str(uuid4())
    elif problem == "missing_parent":
        records[2]["row"]["parent_partition_id"] = str(uuid4())
    elif problem == "cycle":
        records[1]["row"]["parent_partition_id"] = records[2]["row"]["id"]
    else:
        records.append(records[-1])
    with pytest.raises(ValueError):
        write_bundle(output_dir=tmp_path, run_id=run_id, records=records)


def test_checksums_detect_data_and_manifest_changes(tmp_path) -> None:
    run_id = uuid4()
    bundle = write_bundle(output_dir=tmp_path, run_id=run_id, records=metadata_records(run_id))
    manifest_file = bundle.directory / MANIFEST_FILE
    manifest = json.loads(manifest_file.read_text())
    manifest["record_count"] += 1
    manifest_file.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="record count mismatch"):
        verify_bundle(bundle.directory)
    (bundle.directory / DATA_FILE).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="data checksum mismatch"):
        verify_bundle(bundle.directory)


def test_full_readback_receipt_is_bound_to_destination_and_retry_safe(tmp_path) -> None:
    run_id = uuid4()
    bundle = write_bundle(output_dir=tmp_path, run_id=run_id, records=metadata_records(run_id))
    remote = MemoryRemote()
    with pytest.raises(FileNotFoundError):
        verify_offsite_receipt(bundle.directory, offsite_url=URL, offsite_root=ROOT)
    sync_and_verify_bundle(
        bundle.directory, remote_store=remote, offsite_url=URL, offsite_root=ROOT
    )
    assert len(remote.objects) == 2
    assert (
        verify_offsite_receipt(bundle.directory, offsite_url=URL, offsite_root=ROOT).run_id
        == run_id
    )
    with pytest.raises(ValueError, match="receipt mismatch"):
        verify_offsite_receipt(bundle.directory, offsite_url=URL + "-other", offsite_root=ROOT)
    remote.corrupt = True
    old_receipt = (bundle.directory / RECEIPT_FILE).read_bytes()
    with pytest.raises(ValueError, match="readback checksum mismatch"):
        sync_and_verify_bundle(
            bundle.directory, remote_store=remote, offsite_url=URL, offsite_root=ROOT
        )
    assert (bundle.directory / RECEIPT_FILE).read_bytes() == old_receipt


def test_failed_initial_readback_never_creates_a_receipt(tmp_path) -> None:
    run_id = uuid4()
    bundle = write_bundle(output_dir=tmp_path, run_id=run_id, records=metadata_records(run_id))
    with pytest.raises(ValueError, match="readback checksum mismatch"):
        sync_and_verify_bundle(
            bundle.directory,
            remote_store=MemoryRemote(corrupt=True),
            offsite_url=URL,
            offsite_root=ROOT,
        )
    assert not (bundle.directory / RECEIPT_FILE).exists()


def test_changed_manifest_invalidates_old_offsite_receipt(tmp_path) -> None:
    run_id = uuid4()
    bundle = write_bundle(output_dir=tmp_path, run_id=run_id, records=metadata_records(run_id))
    sync_and_verify_bundle(
        bundle.directory, remote_store=MemoryRemote(), offsite_url=URL, offsite_root=ROOT
    )
    manifest_file = bundle.directory / MANIFEST_FILE
    manifest = json.loads(manifest_file.read_text())
    manifest["created_at"] = "changed"
    manifest_file.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="receipt mismatch"):
        verify_offsite_receipt(bundle.directory, offsite_url=URL, offsite_root=ROOT)

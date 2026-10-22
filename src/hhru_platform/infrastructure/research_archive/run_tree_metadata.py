from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

SCHEMA = "run-tree-metadata-v1"
DATA_FILE = "metadata.jsonl.gz"
MANIFEST_FILE = "manifest.json"
RECEIPT_FILE = "offsite.verified.json"
MAX_LINE_BYTES = 8 * 1024 * 1024


class MetadataRemoteStore(Protocol):
    def upload_file(self, *, local_file: Path, remote_path: str) -> None: ...

    def download_file(self, *, local_file: Path, remote_path: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RunTreeMetadataBundle:
    directory: Path
    run_id: UUID
    partition_count: int
    content_sha256: str


def _json_default(value: object) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime) and value.utcoffset() is not None:
        return value.astimezone(UTC).isoformat()
    raise TypeError(f"unsupported metadata value: {type(value).__name__}")


def encode_record(record: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(record, default=_json_default, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def records_digest(records: Iterable[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for record in records:
        digest.update(encode_record(record))
    return digest.hexdigest()


def file_digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
        json.dump(value, output, sort_keys=True, default=_json_default)
        output.write("\n")


def write_bundle(
    *, output_dir: Path, run_id: UUID, records: Iterable[Mapping[str, Any]]
) -> RunTreeMetadataBundle:
    output_dir.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix=f"{run_id}-", dir=output_dir))
    data_file = directory / DATA_FILE
    digest = hashlib.sha256()
    count = 0
    with os.fdopen(os.open(data_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as raw:
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as output:
            for record in records:
                line = encode_record(record)
                if len(line) > MAX_LINE_BYTES:
                    raise ValueError("metadata record exceeds size limit")
                digest.update(line)
                output.write(line)
                count += 1
    _write_json(
        directory / MANIFEST_FILE,
        {
            "schema_version": SCHEMA,
            "run_id": str(run_id),
            "created_at": datetime.now(UTC),
            "data_file": DATA_FILE,
            "record_count": count,
            "data_sha256": file_digest(data_file),
            "content_sha256": digest.hexdigest(),
        },
    )
    return verify_bundle(directory)


def iter_bundle_records(directory: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(directory / DATA_FILE, "rb") as source:
        while line := source.readline(MAX_LINE_BYTES + 1):
            if len(line) > MAX_LINE_BYTES:
                raise ValueError("metadata record exceeds size limit")
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError("metadata record must be an object")
            yield record


def verify_bundle(directory: Path) -> RunTreeMetadataBundle:
    manifest = json.loads((directory / MANIFEST_FILE).read_text())
    if manifest["schema_version"] != SCHEMA or manifest["data_file"] != DATA_FILE:
        raise ValueError("unsupported metadata manifest")
    if file_digest(directory / DATA_FILE) != manifest["data_sha256"]:
        raise ValueError("metadata data checksum mismatch")
    run_id = UUID(manifest["run_id"])
    digest = hashlib.sha256()
    count = 0
    parents: dict[str, str | None] = {}
    for record in iter_bundle_records(directory):
        digest.update(encode_record(record))
        row = record["row"]
        if count == 0:
            if record["table"] != "crawl_run" or UUID(row["id"]) != run_id:
                raise ValueError("metadata root does not match manifest")
            if row["status"] == "created" or row["finished_at"] is None:
                raise ValueError("cannot archive an unfinished run")
        else:
            if record["table"] != "crawl_partition" or UUID(row["crawl_run_id"]) != run_id:
                raise ValueError("metadata partition belongs to another run")
            partition_id = str(UUID(row["id"]))
            if partition_id in parents:
                raise ValueError("duplicate metadata partition id")
            parents[partition_id] = row["parent_partition_id"]
        count += 1
    if count == 0 or count != manifest["record_count"]:
        raise ValueError("metadata record count mismatch")
    if digest.hexdigest() != manifest["content_sha256"]:
        raise ValueError("metadata logical checksum mismatch")
    # Resolve each parent chain once; reject missing parents and cycles before restore.
    resolved: set[str] = set()
    for partition_id in parents:
        chain: set[str] = set()
        cursor: str | None = partition_id
        while cursor is not None and cursor not in resolved:
            if cursor in chain or cursor not in parents:
                raise ValueError("metadata parent graph is cyclic or incomplete")
            chain.add(cursor)
            cursor = parents[cursor]
        resolved.update(chain)
    return RunTreeMetadataBundle(directory, run_id, count - 1, digest.hexdigest())


def _remote_prefix(directory: Path, *, run_id: UUID, offsite_root: str) -> str:
    return f"{offsite_root.rstrip('/')}/run-tree-metadata/{run_id}/{directory.name}"


def sync_and_verify_bundle(
    directory: Path, *, remote_store: MetadataRemoteStore, offsite_url: str, offsite_root: str
) -> RunTreeMetadataBundle:
    bundle = verify_bundle(directory)
    if offsite_root.rstrip("/") != "/hhru-platform/research-archive":
        raise ValueError("run-tree metadata requires canonical research archive root")
    prefix = _remote_prefix(directory, run_id=bundle.run_id, offsite_root=offsite_root)
    with tempfile.TemporaryDirectory(prefix="hhru-metadata-readback-") as temp:
        for name in (DATA_FILE, MANIFEST_FILE):
            remote_store.upload_file(local_file=directory / name, remote_path=f"{prefix}/{name}")
            downloaded = Path(temp) / name
            remote_store.download_file(local_file=downloaded, remote_path=f"{prefix}/{name}")
            if file_digest(downloaded) != file_digest(directory / name):
                raise ValueError(f"metadata S3 readback checksum mismatch: {name}")
        verify_bundle(Path(temp))
    receipt = {
        "schema_version": SCHEMA,
        "run_id": str(bundle.run_id),
        "manifest_sha256": file_digest(directory / MANIFEST_FILE),
        "offsite_url": offsite_url.rstrip("/"),
        "offsite_root": offsite_root.rstrip("/"),
        "remote_prefix": prefix,
        "verified_at": datetime.now(UTC),
        "full_readback": True,
    }
    # A retry must not destroy an earlier valid receipt on a failed readback.
    with tempfile.TemporaryDirectory(prefix=".receipt-", dir=directory) as temp:
        temporary = Path(temp) / RECEIPT_FILE
        _write_json(temporary, receipt)
        temporary.replace(directory / RECEIPT_FILE)
    return bundle


def verify_offsite_receipt(
    directory: Path, *, offsite_url: str, offsite_root: str
) -> RunTreeMetadataBundle:
    bundle = verify_bundle(directory)
    receipt = json.loads((directory / RECEIPT_FILE).read_text())
    expected = {
        "schema_version": SCHEMA,
        "run_id": str(bundle.run_id),
        "manifest_sha256": file_digest(directory / MANIFEST_FILE),
        "offsite_url": offsite_url.rstrip("/"),
        "offsite_root": offsite_root.rstrip("/"),
        "remote_prefix": _remote_prefix(directory, run_id=bundle.run_id, offsite_root=offsite_root),
        "full_readback": True,
    }
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise ValueError("metadata offsite verification receipt mismatch")
    return bundle

from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID


@dataclass(slots=True, frozen=True)
class ResearchArchiveDatasetSpec:
    layer: str
    dataset_name: str
    id_field: str
    observed_at_field: str
    columns: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class ResearchArchiveChunkSummary:
    dataset: str
    layer: str
    partition: dict[str, str]
    chunk_index: int
    row_count: int
    data_file: Path
    manifest_file: Path
    data_size_bytes: int
    data_sha256: str
    source_min_id: str | None
    source_max_id: str | None


@dataclass(slots=True, frozen=True)
class ResearchArchiveVerificationSummary:
    manifest_file: Path
    data_file: Path
    dataset: str
    layer: str
    row_count: int
    data_size_bytes: int
    verified: bool


DATASET_SPECS: dict[str, ResearchArchiveDatasetSpec] = {
    "bronze/raw_api_payload": ResearchArchiveDatasetSpec(
        layer="bronze",
        dataset_name="raw_api_payload",
        id_field="raw_api_payload_id",
        observed_at_field="received_at",
        columns=(
            "archive_schema_version",
            "dataset",
            "raw_api_payload_id",
            "api_request_log_id",
            "crawl_run_id",
            "crawl_partition_id",
            "request_type",
            "endpoint_type",
            "endpoint",
            "method",
            "params_json",
            "status_code",
            "latency_ms",
            "requested_at",
            "response_received_at",
            "entity_hh_id",
            "payload_hash",
            "received_at",
            "payload_json",
        ),
    ),
    "silver/api_request_log": ResearchArchiveDatasetSpec(
        layer="silver",
        dataset_name="api_request_log",
        id_field="api_request_log_id",
        observed_at_field="requested_at",
        columns=(
            "archive_schema_version",
            "dataset",
            "api_request_log_id",
            "crawl_run_id",
            "crawl_partition_id",
            "request_type",
            "endpoint",
            "method",
            "params_json",
            "status_code",
            "latency_ms",
            "attempt",
            "requested_at",
            "response_received_at",
            "error_type",
            "error_message",
            "raw_api_payload_id",
            "payload_hash",
        ),
    ),
    "silver/vacancy": ResearchArchiveDatasetSpec(
        layer="silver",
        dataset_name="vacancy",
        id_field="vacancy_id",
        observed_at_field="updated_at",
        columns=(
            "archive_schema_version",
            "dataset",
            "vacancy_id",
            "hh_vacancy_id",
            "name_current",
            "employer_id",
            "area_id",
            "published_at",
            "created_at_hh",
            "archived_at_hh",
            "alternate_url",
            "employment_type_code",
            "schedule_type_code",
            "experience_code",
            "source_type",
            "created_at",
            "updated_at",
        ),
    ),
    "silver/vacancy_snapshot": ResearchArchiveDatasetSpec(
        layer="silver",
        dataset_name="vacancy_snapshot",
        id_field="snapshot_id",
        observed_at_field="captured_at",
        columns=(
            "archive_schema_version",
            "dataset",
            "snapshot_id",
            "vacancy_id",
            "hh_vacancy_id",
            "snapshot_type",
            "captured_at",
            "crawl_run_id",
            "short_hash",
            "detail_hash",
            "short_payload_ref_id",
            "detail_payload_ref_id",
            "short_payload_hash",
            "detail_payload_hash",
            "change_reason",
        ),
    ),
    "silver/vacancy_seen_event": ResearchArchiveDatasetSpec(
        layer="silver",
        dataset_name="vacancy_seen_event",
        id_field="seen_event_id",
        observed_at_field="seen_at",
        columns=(
            "archive_schema_version",
            "dataset",
            "seen_event_id",
            "vacancy_id",
            "hh_vacancy_id",
            "crawl_run_id",
            "crawl_partition_id",
            "seen_at",
            "list_position",
            "short_hash",
            "short_payload_ref_id",
            "short_payload_hash",
        ),
    ),
    "silver/vacancy_current_state": ResearchArchiveDatasetSpec(
        layer="silver",
        dataset_name="vacancy_current_state",
        id_field="vacancy_id",
        observed_at_field="updated_at",
        columns=(
            "archive_schema_version",
            "dataset",
            "snapshot_date",
            "vacancy_id",
            "hh_vacancy_id",
            "first_seen_at",
            "last_seen_at",
            "seen_count",
            "consecutive_missing_runs",
            "is_probably_inactive",
            "last_seen_run_id",
            "last_short_hash",
            "last_detail_hash",
            "last_detail_fetched_at",
            "detail_fetch_status",
            "updated_at",
        ),
    ),
}


class LocalResearchArchiveStore:
    def write_dataset(
        self,
        *,
        archive_dir: Path,
        schema_version: str,
        dataset: str,
        records: Iterable[Mapping[str, Any]],
        chunk_size: int,
        created_at: datetime,
        archive_kind: str,
        source_database: str,
        source_git_revision: str,
        source_command: str,
        triggered_by: str,
    ) -> tuple[ResearchArchiveChunkSummary, ...]:
        spec = _dataset_spec(dataset)
        archive_root = archive_dir / "v1"
        inventory_file = archive_root / "inventory" / "archive-inventory.jsonl"
        inventory_file.parent.mkdir(parents=True, exist_ok=True)

        normalized_created_at = created_at.astimezone(UTC)
        chunk_indices_by_partition: dict[tuple[tuple[str, str], ...], int] = {}
        summaries: list[ResearchArchiveChunkSummary] = []
        active_partition_key: tuple[tuple[str, str], ...] | None = None
        active_partition: dict[str, str] | None = None
        active_records: list[Mapping[str, Any]] = []

        def flush_active() -> None:
            nonlocal active_records, active_partition_key, active_partition
            if not active_records or active_partition_key is None or active_partition is None:
                return
            next_index = chunk_indices_by_partition.get(active_partition_key, 0) + 1
            chunk_indices_by_partition[active_partition_key] = next_index
            summary = self._write_chunk(
                archive_dir=archive_dir,
                archive_root=archive_root,
                inventory_file=inventory_file,
                spec=spec,
                schema_version=schema_version,
                dataset=dataset,
                records=active_records,
                chunk_size=chunk_size,
                chunk_index=next_index,
                partition=active_partition,
                created_at=normalized_created_at,
                archive_kind=archive_kind,
                source_database=source_database,
                source_git_revision=source_git_revision,
                source_command=source_command,
                triggered_by=triggered_by,
            )
            summaries.append(summary)
            active_records = []

        for record in records:
            normalized_record = _normalize_record(
                schema_version=schema_version,
                spec=spec,
                record=record,
            )
            partition = _partition_for_record(
                spec=spec,
                record=normalized_record,
                created_at=normalized_created_at,
            )
            if spec.dataset_name == "vacancy_current_state":
                normalized_record.setdefault("snapshot_date", partition["snapshot_date"])
            partition_key = tuple(sorted(partition.items()))
            if active_partition_key is None:
                active_partition_key = partition_key
                active_partition = partition
            if partition_key != active_partition_key or len(active_records) >= chunk_size:
                flush_active()
                active_partition_key = partition_key
                active_partition = partition
            active_records.append(normalized_record)

        flush_active()
        return tuple(summaries)

    def _write_chunk(
        self,
        *,
        archive_dir: Path,
        archive_root: Path,
        inventory_file: Path,
        spec: ResearchArchiveDatasetSpec,
        schema_version: str,
        dataset: str,
        records: list[Mapping[str, Any]],
        chunk_size: int,
        chunk_index: int,
        partition: dict[str, str],
        created_at: datetime,
        archive_kind: str,
        source_database: str,
        source_git_revision: str,
        source_command: str,
        triggered_by: str,
    ) -> ResearchArchiveChunkSummary:
        partition_path = _partition_path(partition)
        dataset_dir = archive_root / spec.layer / spec.dataset_name / partition_path
        dataset_dir.mkdir(parents=True, exist_ok=True)

        export_id = created_at.strftime("%Y%m%dT%H%M%SZ")
        chunk_basename = f"{export_id}-chunk-{chunk_index:06d}"
        data_file = dataset_dir / f"{chunk_basename}.jsonl.gz"
        manifest_file = dataset_dir / f"{chunk_basename}.manifest.json"
        if data_file.exists() or manifest_file.exists():
            raise FileExistsError(
                f"research archive chunk already exists: {data_file} or {manifest_file}"
            )

        with gzip.open(data_file, "wt", encoding="utf-8") as handle:
            for record in records:
                handle.write(
                    json.dumps(
                        dict(record),
                        ensure_ascii=True,
                        sort_keys=True,
                        default=_json_default,
                    )
                )
                handle.write("\n")

        data_sha256 = _sha256(data_file)
        data_size_bytes = data_file.stat().st_size
        observed_values = [
            value for record in records if (value := record.get(spec.observed_at_field)) is not None
        ]
        id_values = [
            value for record in records if (value := record.get(spec.id_field)) is not None
        ]
        relative_data_file = data_file.relative_to(archive_dir)
        relative_manifest_file = manifest_file.relative_to(archive_dir)
        source_min_id = _min_source_id(id_values)
        source_max_id = _max_source_id(id_values)
        manifest_payload = {
            "archive_schema_version": schema_version,
            "archive_kind": archive_kind,
            "dataset": spec.dataset_name,
            "dataset_key": dataset,
            "layer": spec.layer,
            "partition": partition,
            "chunk_index": chunk_index,
            "chunk_size": chunk_size,
            "created_at": created_at.isoformat(),
            "source_database": source_database,
            "source_git_revision": source_git_revision,
            "source_command": source_command,
            "triggered_by": triggered_by,
            "row_count": len(records),
            "source_min_id": source_min_id,
            "source_max_id": source_max_id,
            "source_min_observed_at": _min_observed_at(observed_values),
            "source_max_observed_at": _max_observed_at(observed_values),
            "data_file": str(relative_data_file),
            "data_size_bytes": data_size_bytes,
            "data_sha256": data_sha256,
            "compression": "gzip",
            "format": "jsonl",
            "columns": list(spec.columns),
        }
        manifest_file.write_text(
            json.dumps(
                manifest_payload,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
                default=_json_default,
            )
            + "\n",
            encoding="utf-8",
        )

        inventory_entry = {
            "archive_schema_version": schema_version,
            "archive_kind": archive_kind,
            "dataset": spec.dataset_name,
            "dataset_key": dataset,
            "layer": spec.layer,
            "partition": partition,
            "manifest_file": str(relative_manifest_file),
            "data_file": str(relative_data_file),
            "row_count": len(records),
            "data_size_bytes": data_size_bytes,
            "data_sha256": data_sha256,
            "source_min_id": manifest_payload["source_min_id"],
            "source_max_id": manifest_payload["source_max_id"],
            "source_min_observed_at": manifest_payload["source_min_observed_at"],
            "source_max_observed_at": manifest_payload["source_max_observed_at"],
            "created_at": created_at.isoformat(),
            "status": "exported",
        }
        with inventory_file.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    inventory_entry,
                    ensure_ascii=True,
                    sort_keys=True,
                    default=_json_default,
                )
            )
            handle.write("\n")

        return ResearchArchiveChunkSummary(
            dataset=dataset,
            layer=spec.layer,
            partition=partition,
            chunk_index=chunk_index,
            row_count=len(records),
            data_file=data_file,
            manifest_file=manifest_file,
            data_size_bytes=data_size_bytes,
            data_sha256=data_sha256,
            source_min_id=source_min_id,
            source_max_id=source_max_id,
        )


class ResearchArchiveManifestVerifier:
    def verify(
        self,
        *,
        archive_dir: Path,
        manifest_files: tuple[Path, ...] = (),
        limit: int | None = None,
    ) -> tuple[ResearchArchiveVerificationSummary, ...]:
        selected_manifest_files = _select_manifest_files(
            archive_dir=archive_dir,
            manifest_files=manifest_files,
            limit=limit,
        )
        inventory_manifest_files = _load_inventory_manifest_files(archive_dir)
        return tuple(
            self._verify_one(
                archive_dir=archive_dir,
                manifest_file=manifest_file,
                inventory_manifest_files=inventory_manifest_files,
            )
            for manifest_file in selected_manifest_files
        )

    def _verify_one(
        self,
        *,
        archive_dir: Path,
        manifest_file: Path,
        inventory_manifest_files: set[str],
    ) -> ResearchArchiveVerificationSummary:
        manifest_payload = json.loads(manifest_file.read_text(encoding="utf-8"))
        _require_manifest_fields(manifest_file, manifest_payload)

        data_file_value = str(manifest_payload["data_file"])
        data_file = Path(data_file_value)
        if not data_file.is_absolute():
            data_file = archive_dir / data_file
        if not data_file.is_file():
            raise ValueError(
                f"archive data file not found for manifest {manifest_file}: {data_file}"
            )

        expected_sha256 = str(manifest_payload["data_sha256"])
        actual_sha256 = _sha256(data_file)
        if actual_sha256 != expected_sha256:
            raise ValueError(
                f"archive sha256 mismatch for {data_file}: "
                f"expected {expected_sha256}, got {actual_sha256}"
            )

        expected_size = int(manifest_payload["data_size_bytes"])
        actual_size = data_file.stat().st_size
        if actual_size != expected_size:
            raise ValueError(
                f"archive size mismatch for {data_file}: "
                f"expected {expected_size}, got {actual_size}"
            )

        expected_row_count = int(manifest_payload["row_count"])
        actual_row_count = _count_jsonl_gzip_rows(data_file)
        if actual_row_count != expected_row_count:
            raise ValueError(
                f"archive row_count mismatch for {data_file}: "
                f"expected {expected_row_count}, got {actual_row_count}"
            )

        relative_manifest_file = _relative_to_archive(manifest_file, archive_dir)
        if inventory_manifest_files and relative_manifest_file not in inventory_manifest_files:
            raise ValueError(
                f"manifest is missing from archive inventory: {relative_manifest_file}"
            )

        return ResearchArchiveVerificationSummary(
            manifest_file=manifest_file,
            data_file=data_file,
            dataset=str(manifest_payload["dataset_key"]),
            layer=str(manifest_payload["layer"]),
            row_count=actual_row_count,
            data_size_bytes=actual_size,
            verified=True,
        )


def _dataset_spec(dataset: str) -> ResearchArchiveDatasetSpec:
    try:
        return DATASET_SPECS[dataset]
    except KeyError as error:
        supported = ", ".join(sorted(DATASET_SPECS))
        raise ValueError(
            f"unsupported research archive dataset: {dataset}; supported: {supported}"
        ) from error


def _normalize_record(
    *,
    schema_version: str,
    spec: ResearchArchiveDatasetSpec,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = dict(record)
    normalized["archive_schema_version"] = schema_version
    normalized["dataset"] = spec.dataset_name
    return normalized


def _partition_for_record(
    *,
    spec: ResearchArchiveDatasetSpec,
    record: Mapping[str, Any],
    created_at: datetime,
) -> dict[str, str]:
    if spec.dataset_name == "raw_api_payload":
        observed_at = _datetime_value(record.get("received_at"), fallback=created_at)
        return {
            "request_type": _safe_partition_value(record.get("request_type") or "unknown"),
            **_date_partition(observed_at),
        }
    if spec.dataset_name == "api_request_log":
        return _date_partition(_datetime_value(record.get("requested_at"), fallback=created_at))
    if spec.dataset_name == "vacancy":
        return {"snapshot_date": created_at.date().isoformat()}
    if spec.dataset_name == "vacancy_snapshot":
        observed_at = _datetime_value(record.get("captured_at"), fallback=created_at)
        return {
            "snapshot_type": _safe_partition_value(record.get("snapshot_type") or "unknown"),
            **_date_partition(observed_at),
        }
    if spec.dataset_name == "vacancy_seen_event":
        return _date_partition(_datetime_value(record.get("seen_at"), fallback=created_at))
    if spec.dataset_name == "vacancy_current_state":
        return {"snapshot_date": created_at.date().isoformat()}
    return {"export_date": created_at.date().isoformat()}


def _date_partition(value: datetime) -> dict[str, str]:
    normalized = value.astimezone(UTC)
    return {
        "year": f"{normalized.year:04d}",
        "month": f"{normalized.month:02d}",
        "day": f"{normalized.day:02d}",
    }


def _partition_path(partition: Mapping[str, str]) -> Path:
    path = Path()
    for key, value in partition.items():
        path /= f"{_safe_partition_value(key)}={_safe_partition_value(value)}"
    return path


def _safe_partition_value(value: object) -> str:
    text = str(value)
    return "".join(char if char.isalnum() or char in {"-", "_", "."} else "_" for char in text)


def _datetime_value(value: object, *, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return fallback


def _min_observed_at(values: list[object]) -> str | None:
    normalized_values = sorted(str(_json_default(value)) for value in values)
    return normalized_values[0] if normalized_values else None


def _max_observed_at(values: list[object]) -> str | None:
    normalized_values = sorted(str(_json_default(value)) for value in values)
    return normalized_values[-1] if normalized_values else None


def _min_source_id(values: list[object]) -> str | None:
    return _extreme_source_id(values, minimum=True)


def _max_source_id(values: list[object]) -> str | None:
    return _extreme_source_id(values, minimum=False)


def _extreme_source_id(values: list[object], *, minimum: bool) -> str | None:
    if not values:
        return None
    select_value = min if minimum else max
    return str(select_value(values, key=_source_id_sort_key))


def _source_id_sort_key(value: object) -> tuple[int, int | str]:
    if isinstance(value, int) and not isinstance(value, bool):
        return 0, value
    return 1, str(value)


def _select_manifest_files(
    *,
    archive_dir: Path,
    manifest_files: tuple[Path, ...],
    limit: int | None,
) -> tuple[Path, ...]:
    if manifest_files:
        selected = tuple(_resolve_manifest_path(archive_dir, path) for path in manifest_files)
    else:
        selected = tuple(sorted((archive_dir / "v1").rglob("*.manifest.json")))
    if limit is not None:
        return selected[:limit]
    return selected


def _resolve_manifest_path(archive_dir: Path, path: Path) -> Path:
    if path.is_absolute() or path.exists():
        return path
    return archive_dir / path


def _relative_to_archive(path: Path, archive_dir: Path) -> str:
    try:
        return str(path.relative_to(archive_dir))
    except ValueError:
        return str(path)


def _load_inventory_manifest_files(archive_dir: Path) -> set[str]:
    inventory_file = archive_dir / "v1" / "inventory" / "archive-inventory.jsonl"
    if not inventory_file.is_file():
        return set()
    manifest_files: set[str] = set()
    with inventory_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            manifest_files.add(str(payload["manifest_file"]))
    return manifest_files


def _require_manifest_fields(manifest_file: Path, payload: Mapping[str, Any]) -> None:
    required_fields = {
        "archive_schema_version",
        "dataset",
        "dataset_key",
        "layer",
        "partition",
        "chunk_index",
        "created_at",
        "source_database",
        "source_git_revision",
        "source_command",
        "row_count",
        "data_file",
        "data_size_bytes",
        "data_sha256",
        "compression",
        "format",
        "columns",
    }
    missing = sorted(required_fields - set(payload))
    if missing:
        raise ValueError(f"manifest {manifest_file} is missing fields: {', '.join(missing)}")
    if payload["compression"] != "gzip":
        raise ValueError(f"manifest {manifest_file} has unsupported compression")
    if payload["format"] != "jsonl":
        raise ValueError(f"manifest {manifest_file} has unsupported format")


def _count_jsonl_gzip_rows(path: Path) -> int:
    count = 0
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            json.loads(line)
            count += 1
    return count


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_default(value: object) -> str | float | int | bool | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID | Path | Enum):
        return str(value)
    if value is None or isinstance(value, str | float | int | bool):
        return value
    return str(value)

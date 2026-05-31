from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from hhru_platform.infrastructure.backup.backup_offsite_receipt_store import (
    BackupOffsiteUploadReceipt,
)
from hhru_platform.infrastructure.backup.backup_offsite_verification_receipt_store import (
    BackupOffsiteVerificationReceipt,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

BACKUP_OFFSITE_CLEANUP_STATUS_SUCCEEDED = "succeeded"
BACKUP_TIMESTAMP_PATTERN = re.compile(r"_(\d{8}T\d{6}Z)\.dump$")


@dataclass(slots=True, frozen=True)
class CleanupBackupOffsiteCommand:
    backup_dir: Path = Path(".state/backups")
    offsite_url: str = ""
    offsite_root: str = "/hhru-platform/backups"
    keep_latest: int = 3
    keep_weekly: int = 4
    apply: bool = False
    protected_backup_files: tuple[Path, ...] = ()
    triggered_by: str = "cleanup-backup-offsite"
    evaluated_at: datetime | None = None

    def __post_init__(self) -> None:
        normalized_triggered_by = self.triggered_by.strip()
        normalized_offsite_url = self.offsite_url.strip().rstrip("/")
        normalized_offsite_root = _normalize_offsite_root(self.offsite_root)
        if not normalized_triggered_by:
            raise ValueError("triggered_by must not be empty")
        if not normalized_offsite_url:
            raise ValueError("offsite_url must not be empty")
        if self.keep_latest < 1:
            raise ValueError("keep_latest must be greater than or equal to one")
        if self.keep_weekly < 0:
            raise ValueError("keep_weekly must be greater than or equal to zero")
        object.__setattr__(self, "backup_dir", Path(self.backup_dir))
        object.__setattr__(self, "offsite_url", normalized_offsite_url)
        object.__setattr__(self, "offsite_root", normalized_offsite_root)
        object.__setattr__(self, "triggered_by", normalized_triggered_by)
        object.__setattr__(
            self,
            "protected_backup_files",
            tuple(Path(path) for path in self.protected_backup_files),
        )


@dataclass(slots=True, frozen=True)
class BackupOffsiteCleanupSummary:
    backup_file: Path
    backup_at: datetime | None
    action: str
    reason: str
    remote_backup_path: str | None = None
    remote_manifest_path: str | None = None
    remote_deleted_object_count: int = 0
    local_deleted_sidecar_count: int = 0


@dataclass(slots=True, frozen=True)
class CleanupBackupOffsiteResult:
    status: str
    triggered_by: str
    evaluated_at: datetime
    backup_dir: Path
    offsite_url: str
    offsite_root: str
    keep_latest: int
    keep_weekly: int
    apply: bool
    summaries: tuple[BackupOffsiteCleanupSummary, ...]

    @property
    def scanned_receipt_count(self) -> int:
        return len(self.summaries)

    @property
    def delete_candidate_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.action == "delete_candidate")

    @property
    def deleted_generation_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.action == "deleted")

    @property
    def retained_generation_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.action == "retained")

    @property
    def skipped_generation_count(self) -> int:
        return sum(1 for summary in self.summaries if summary.action.startswith("skipped_"))

    @property
    def remote_deleted_object_count(self) -> int:
        return sum(summary.remote_deleted_object_count for summary in self.summaries)

    @property
    def local_deleted_sidecar_count(self) -> int:
        return sum(summary.local_deleted_sidecar_count for summary in self.summaries)


class BackupOffsiteCleanupRemoteStore(Protocol):
    def delete_file(self, *, remote_path: str) -> None:
        """Delete one offsite backup object."""


class BackupOffsiteUploadReceiptStore(Protocol):
    def load_receipt(self, *, backup_file: Path) -> BackupOffsiteUploadReceipt | None:
        """Load one upload receipt."""


class BackupOffsiteVerificationReceiptStore(Protocol):
    def load_receipt(
        self,
        *,
        backup_file: Path,
    ) -> BackupOffsiteVerificationReceipt | None:
        """Load one successful offsite verification receipt."""


@dataclass(slots=True, frozen=True)
class _BackupGeneration:
    backup_file: Path
    backup_at: datetime
    upload_receipt: BackupOffsiteUploadReceipt
    verification_receipt: BackupOffsiteVerificationReceipt
    protected: bool


def cleanup_backup_offsite(
    command: CleanupBackupOffsiteCommand,
    *,
    remote_store: BackupOffsiteCleanupRemoteStore,
    upload_receipt_store: BackupOffsiteUploadReceiptStore,
    verification_receipt_store: BackupOffsiteVerificationReceiptStore,
) -> CleanupBackupOffsiteResult:
    started_at = log_operation_started(
        LOGGER,
        operation="cleanup_backup_offsite",
        backup_dir=str(command.backup_dir),
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        keep_latest=command.keep_latest,
        keep_weekly=command.keep_weekly,
        apply=command.apply,
        triggered_by=command.triggered_by,
    )
    try:
        result = _cleanup_backup_offsite(
            command=command,
            remote_store=remote_store,
            upload_receipt_store=upload_receipt_store,
            verification_receipt_store=verification_receipt_store,
        )
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="cleanup_backup_offsite",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            backup_dir=str(command.backup_dir),
            offsite_url=command.offsite_url,
            offsite_root=command.offsite_root,
            keep_latest=command.keep_latest,
            keep_weekly=command.keep_weekly,
            apply=command.apply,
            triggered_by=command.triggered_by,
        )
        raise

    record_operation_succeeded(
        LOGGER,
        operation="cleanup_backup_offsite",
        started_at=started_at,
        backup_dir=str(result.backup_dir),
        offsite_url=result.offsite_url,
        offsite_root=result.offsite_root,
        keep_latest=result.keep_latest,
        keep_weekly=result.keep_weekly,
        apply=result.apply,
        triggered_by=result.triggered_by,
        scanned_receipt_count=result.scanned_receipt_count,
        delete_candidate_count=result.delete_candidate_count,
        deleted_generation_count=result.deleted_generation_count,
        retained_generation_count=result.retained_generation_count,
        skipped_generation_count=result.skipped_generation_count,
        remote_deleted_object_count=result.remote_deleted_object_count,
        local_deleted_sidecar_count=result.local_deleted_sidecar_count,
    )
    return result


def _cleanup_backup_offsite(
    *,
    command: CleanupBackupOffsiteCommand,
    remote_store: BackupOffsiteCleanupRemoteStore,
    upload_receipt_store: BackupOffsiteUploadReceiptStore,
    verification_receipt_store: BackupOffsiteVerificationReceiptStore,
) -> CleanupBackupOffsiteResult:
    backup_root = command.backup_dir.resolve()
    protected_backup_files = {
        backup_file.resolve() for backup_file in command.protected_backup_files
    }
    generations: list[_BackupGeneration] = []
    summaries: list[BackupOffsiteCleanupSummary] = []
    for receipt_file in _list_upload_receipt_files(backup_root):
        backup_file = _backup_file_for_upload_receipt(receipt_file)
        try:
            upload_receipt = upload_receipt_store.load_receipt(backup_file=backup_file)
        except Exception as error:
            summaries.append(
                _skip_summary(
                    backup_file=backup_file,
                    action="skipped_invalid",
                    reason=f"invalid upload receipt: {error}",
                )
            )
            continue
        if upload_receipt is None:
            summaries.append(
                _skip_summary(
                    backup_file=backup_file,
                    action="skipped_invalid",
                    reason="upload receipt disappeared during scan",
                )
            )
            continue
        if (
            upload_receipt.offsite_url != command.offsite_url
            or upload_receipt.offsite_root != command.offsite_root
        ):
            summaries.append(
                _skip_summary(
                    backup_file=backup_file,
                    action="skipped_foreign",
                    reason="upload receipt belongs to a different offsite target",
                    upload_receipt=upload_receipt,
                )
            )
            continue
        if upload_receipt.part_count < 1:
            summaries.append(
                _skip_summary(
                    backup_file=backup_file,
                    action="skipped_invalid",
                    reason="upload receipt part_count must be greater than zero",
                    upload_receipt=upload_receipt,
                )
            )
            continue
        try:
            verification_receipt = verification_receipt_store.load_receipt(
                backup_file=backup_file
            )
        except Exception as error:
            summaries.append(
                _skip_summary(
                    backup_file=backup_file,
                    action="skipped_invalid",
                    reason=f"invalid verification receipt: {error}",
                    upload_receipt=upload_receipt,
                )
            )
            continue
        if verification_receipt is None or not _verification_matches_upload(
            verification_receipt,
            upload_receipt=upload_receipt,
        ):
            summaries.append(
                _skip_summary(
                    backup_file=backup_file,
                    action="skipped_unverified",
                    reason="matching successful verification receipt is required",
                    upload_receipt=upload_receipt,
                )
            )
            continue
        generations.append(
            _BackupGeneration(
                backup_file=backup_file,
                backup_at=_backup_timestamp(
                    backup_file=backup_file,
                    fallback=upload_receipt.uploaded_at,
                ),
                upload_receipt=upload_receipt,
                verification_receipt=verification_receipt,
                protected=(
                    backup_file.resolve() in protected_backup_files
                    or _milestone_marker_path(backup_file).exists()
                ),
            )
        )

    generations.sort(
        key=lambda generation: (generation.backup_at, generation.backup_file.name),
        reverse=True,
    )
    retained_reasons = _retained_reasons(generations=generations, command=command)
    for generation in generations:
        retained_reason = retained_reasons.get(generation.backup_file)
        if retained_reason is not None:
            summaries.append(
                _generation_summary(
                    generation=generation,
                    action="retained",
                    reason=retained_reason,
                )
            )
            continue
        if not _has_newer_generation(generation=generation, generations=generations):
            summaries.append(
                _generation_summary(
                    generation=generation,
                    action="retained",
                    reason="no newer verified generation exists",
                )
            )
            continue
        if not command.apply:
            summaries.append(
                _generation_summary(
                    generation=generation,
                    action="delete_candidate",
                    reason="verified generation is outside retention policy; dry-run only",
                )
            )
            continue
        remote_deleted_object_count = _delete_remote_generation(
            generation=generation,
            command=command,
            remote_store=remote_store,
        )
        local_deleted_sidecar_count = _delete_local_sidecars(generation.backup_file)
        summaries.append(
            _generation_summary(
                generation=generation,
                action="deleted",
                reason="verified generation deleted by applied retention policy",
                remote_deleted_object_count=remote_deleted_object_count,
                local_deleted_sidecar_count=local_deleted_sidecar_count,
            )
        )

    return CleanupBackupOffsiteResult(
        status=BACKUP_OFFSITE_CLEANUP_STATUS_SUCCEEDED,
        triggered_by=command.triggered_by,
        evaluated_at=command.evaluated_at or datetime.now(UTC),
        backup_dir=backup_root,
        offsite_url=command.offsite_url,
        offsite_root=command.offsite_root,
        keep_latest=command.keep_latest,
        keep_weekly=command.keep_weekly,
        apply=command.apply,
        summaries=tuple(sorted(summaries, key=_summary_sort_key, reverse=True)),
    )


def _list_upload_receipt_files(backup_root: Path) -> list[Path]:
    if not backup_root.exists():
        return []
    return sorted(
        path.resolve()
        for path in backup_root.rglob("*.dump.offsite.json")
        if path.is_file()
    )


def _backup_file_for_upload_receipt(receipt_file: Path) -> Path:
    suffix = ".offsite.json"
    return Path(str(receipt_file)[: -len(suffix)])


def _verification_matches_upload(
    receipt: BackupOffsiteVerificationReceipt | None,
    *,
    upload_receipt: BackupOffsiteUploadReceipt,
) -> bool:
    if receipt is None:
        return False
    return (
        receipt.offsite_url == upload_receipt.offsite_url
        and receipt.offsite_root == upload_receipt.offsite_root
        and receipt.backup_size_bytes == upload_receipt.backup_size_bytes
        and receipt.backup_sha256 == upload_receipt.backup_sha256
        and receipt.manifest_sha256 == upload_receipt.manifest_sha256
        and receipt.chunk_size_bytes == upload_receipt.chunk_size_bytes
        and receipt.part_count == upload_receipt.part_count
        and receipt.remote_backup_path == upload_receipt.remote_backup_path
        and receipt.remote_manifest_path == upload_receipt.remote_manifest_path
        and receipt.verified_object_count == upload_receipt.part_count + 1
    )


def _backup_timestamp(*, backup_file: Path, fallback: datetime) -> datetime:
    match = BACKUP_TIMESTAMP_PATTERN.search(backup_file.name)
    if match is None:
        return fallback.astimezone(UTC)
    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


def _retained_reasons(
    *,
    generations: list[_BackupGeneration],
    command: CleanupBackupOffsiteCommand,
) -> dict[Path, str]:
    reasons: dict[Path, str] = {}
    for generation in generations:
        if generation.protected:
            reasons[generation.backup_file] = "protected milestone backup"
    for generation in generations[: command.keep_latest]:
        reasons.setdefault(generation.backup_file, "latest verified generation")

    retained_week_count = 0
    retained_weeks: set[tuple[int, int]] = set()
    for generation in generations:
        iso_calendar = generation.backup_at.isocalendar()
        week = (iso_calendar.year, iso_calendar.week)
        if week in retained_weeks:
            continue
        retained_weeks.add(week)
        if retained_week_count >= command.keep_weekly:
            continue
        reasons.setdefault(generation.backup_file, "weekly verified checkpoint")
        retained_week_count += 1
    return reasons


def _has_newer_generation(
    *,
    generation: _BackupGeneration,
    generations: list[_BackupGeneration],
) -> bool:
    return any(other.backup_at > generation.backup_at for other in generations)


def _delete_remote_generation(
    *,
    generation: _BackupGeneration,
    command: CleanupBackupOffsiteCommand,
    remote_store: BackupOffsiteCleanupRemoteStore,
) -> int:
    remote_parts_path = _relative_remote_path(
        generation.upload_receipt.remote_backup_path,
        offsite_root=command.offsite_root,
    )
    for part_index in range(1, generation.upload_receipt.part_count + 1):
        remote_store.delete_file(
            remote_path=f"{remote_parts_path}/{part_index:06d}.part"
        )
    remote_store.delete_file(
        remote_path=_relative_remote_path(
            generation.upload_receipt.remote_manifest_path,
            offsite_root=command.offsite_root,
        )
    )
    return generation.upload_receipt.part_count + 1


def _relative_remote_path(remote_path: str, *, offsite_root: str) -> str:
    normalized_remote_path = "/" + remote_path.strip("/")
    normalized_offsite_root = _normalize_offsite_root(offsite_root)
    prefix = normalized_offsite_root.rstrip("/") + "/"
    if not normalized_remote_path.startswith(prefix):
        raise RuntimeError(
            "refusing to delete remote path outside configured offsite root: "
            f"{remote_path}"
        )
    return normalized_remote_path[len(prefix) :]


def _delete_local_sidecars(backup_file: Path) -> int:
    deleted_count = 0
    for sidecar in (
        Path(f"{backup_file}.manifest.json"),
        Path(f"{backup_file}.offsite.json"),
        Path(f"{backup_file}.offsite.parts.json"),
        Path(f"{backup_file}.offsite.verified.json"),
    ):
        if sidecar.exists():
            sidecar.unlink()
            deleted_count += 1
    return deleted_count


def _milestone_marker_path(backup_file: Path) -> Path:
    return Path(f"{backup_file}.offsite.keep")


def _generation_summary(
    *,
    generation: _BackupGeneration,
    action: str,
    reason: str,
    remote_deleted_object_count: int = 0,
    local_deleted_sidecar_count: int = 0,
) -> BackupOffsiteCleanupSummary:
    return BackupOffsiteCleanupSummary(
        backup_file=generation.backup_file,
        backup_at=generation.backup_at,
        action=action,
        reason=reason,
        remote_backup_path=generation.upload_receipt.remote_backup_path,
        remote_manifest_path=generation.upload_receipt.remote_manifest_path,
        remote_deleted_object_count=remote_deleted_object_count,
        local_deleted_sidecar_count=local_deleted_sidecar_count,
    )


def _skip_summary(
    *,
    backup_file: Path,
    action: str,
    reason: str,
    upload_receipt: BackupOffsiteUploadReceipt | None = None,
) -> BackupOffsiteCleanupSummary:
    return BackupOffsiteCleanupSummary(
        backup_file=backup_file,
        backup_at=None,
        action=action,
        reason=reason,
        remote_backup_path=(
            upload_receipt.remote_backup_path if upload_receipt is not None else None
        ),
        remote_manifest_path=(
            upload_receipt.remote_manifest_path if upload_receipt is not None else None
        ),
    )


def _summary_sort_key(summary: BackupOffsiteCleanupSummary) -> tuple[datetime, str]:
    return (
        summary.backup_at or datetime.min.replace(tzinfo=UTC),
        summary.backup_file.name,
    )


def _normalize_offsite_root(offsite_root: str) -> str:
    parts = tuple(part for part in offsite_root.strip().split("/") if part)
    if not parts:
        return "/"
    return "/" + "/".join(parts)

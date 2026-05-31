from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(slots=True, frozen=True)
class BackupOffsiteVerificationReceipt:
    verified_at: datetime
    offsite_url: str
    offsite_root: str
    backup_size_bytes: int
    backup_sha256: str
    manifest_sha256: str
    chunk_size_bytes: int
    part_count: int
    remote_backup_path: str
    remote_manifest_path: str
    verified_object_count: int


class LocalBackupOffsiteVerificationReceiptStore:
    def load_receipt(
        self,
        *,
        backup_file: Path,
    ) -> BackupOffsiteVerificationReceipt | None:
        receipt_file = self.receipt_path_for_backup_file(backup_file)
        if not receipt_file.exists():
            return None
        payload = json.loads(receipt_file.read_text(encoding="utf-8"))
        return BackupOffsiteVerificationReceipt(
            verified_at=datetime.fromisoformat(str(payload["verified_at"])).astimezone(UTC),
            offsite_url=str(payload["offsite_url"]),
            offsite_root=str(payload["offsite_root"]),
            backup_size_bytes=int(payload["backup_size_bytes"]),
            backup_sha256=str(payload["backup_sha256"]),
            manifest_sha256=str(payload["manifest_sha256"]),
            chunk_size_bytes=int(payload["chunk_size_bytes"]),
            part_count=int(payload["part_count"]),
            remote_backup_path=str(payload["remote_backup_path"]),
            remote_manifest_path=str(payload["remote_manifest_path"]),
            verified_object_count=int(payload["verified_object_count"]),
        )

    def write_receipt(
        self,
        *,
        backup_file: Path,
        receipt: BackupOffsiteVerificationReceipt,
    ) -> Path:
        receipt_file = self.receipt_path_for_backup_file(backup_file)
        receipt_file.write_text(
            json.dumps(
                {
                    "verified_at": receipt.verified_at.astimezone(UTC).isoformat(),
                    "offsite_url": receipt.offsite_url,
                    "offsite_root": receipt.offsite_root,
                    "backup_size_bytes": receipt.backup_size_bytes,
                    "backup_sha256": receipt.backup_sha256,
                    "manifest_sha256": receipt.manifest_sha256,
                    "chunk_size_bytes": receipt.chunk_size_bytes,
                    "part_count": receipt.part_count,
                    "remote_backup_path": receipt.remote_backup_path,
                    "remote_manifest_path": receipt.remote_manifest_path,
                    "verified_object_count": receipt.verified_object_count,
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return receipt_file

    def receipt_path_for_backup_file(self, backup_file: Path) -> Path:
        return Path(f"{backup_file}.offsite.verified.json")

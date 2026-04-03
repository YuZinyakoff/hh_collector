from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(slots=True, frozen=True)
class RetentionArchiveUploadReceipt:
    uploaded_at: datetime
    offsite_url: str
    offsite_root: str
    manifest_sha256: str
    archive_sha256: str
    remote_archive_path: str
    remote_manifest_path: str


class LocalRetentionArchiveUploadReceiptStore:
    def load_receipt(self, *, manifest_file: Path) -> RetentionArchiveUploadReceipt | None:
        receipt_file = self.receipt_path_for_manifest(manifest_file)
        if not receipt_file.exists():
            return None
        payload = json.loads(receipt_file.read_text(encoding="utf-8"))
        return RetentionArchiveUploadReceipt(
            uploaded_at=datetime.fromisoformat(str(payload["uploaded_at"])).astimezone(UTC),
            offsite_url=str(payload["offsite_url"]),
            offsite_root=str(payload["offsite_root"]),
            manifest_sha256=str(payload["manifest_sha256"]),
            archive_sha256=str(payload["archive_sha256"]),
            remote_archive_path=str(payload["remote_archive_path"]),
            remote_manifest_path=str(payload["remote_manifest_path"]),
        )

    def write_receipt(
        self,
        *,
        manifest_file: Path,
        receipt: RetentionArchiveUploadReceipt,
    ) -> Path:
        receipt_file = self.receipt_path_for_manifest(manifest_file)
        receipt_file.write_text(
            json.dumps(
                {
                    "uploaded_at": receipt.uploaded_at.astimezone(UTC).isoformat(),
                    "offsite_url": receipt.offsite_url,
                    "offsite_root": receipt.offsite_root,
                    "manifest_sha256": receipt.manifest_sha256,
                    "archive_sha256": receipt.archive_sha256,
                    "remote_archive_path": receipt.remote_archive_path,
                    "remote_manifest_path": receipt.remote_manifest_path,
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return receipt_file

    def receipt_path_for_manifest(self, manifest_file: Path) -> Path:
        return Path(f"{manifest_file}.uploaded.json")

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(slots=True, frozen=True)
class ResearchArchiveOffsiteUploadReceipt:
    uploaded_at: datetime
    offsite_url: str
    offsite_root: str
    data_size_bytes: int
    data_sha256: str
    manifest_sha256: str
    remote_data_path: str
    remote_manifest_path: str


class LocalResearchArchiveOffsiteUploadReceiptStore:
    def load_receipt(
        self,
        *,
        manifest_file: Path,
    ) -> ResearchArchiveOffsiteUploadReceipt | None:
        receipt_file = self.receipt_path_for_manifest_file(manifest_file)
        if not receipt_file.exists():
            return None
        payload = json.loads(receipt_file.read_text(encoding="utf-8"))
        return ResearchArchiveOffsiteUploadReceipt(
            uploaded_at=datetime.fromisoformat(str(payload["uploaded_at"])).astimezone(UTC),
            offsite_url=str(payload["offsite_url"]),
            offsite_root=str(payload["offsite_root"]),
            data_size_bytes=int(payload["data_size_bytes"]),
            data_sha256=str(payload["data_sha256"]),
            manifest_sha256=str(payload["manifest_sha256"]),
            remote_data_path=str(payload["remote_data_path"]),
            remote_manifest_path=str(payload["remote_manifest_path"]),
        )

    def write_receipt(
        self,
        *,
        manifest_file: Path,
        receipt: ResearchArchiveOffsiteUploadReceipt,
    ) -> Path:
        receipt_file = self.receipt_path_for_manifest_file(manifest_file)
        receipt_file.write_text(
            json.dumps(
                {
                    "uploaded_at": receipt.uploaded_at.astimezone(UTC).isoformat(),
                    "offsite_url": receipt.offsite_url,
                    "offsite_root": receipt.offsite_root,
                    "data_size_bytes": receipt.data_size_bytes,
                    "data_sha256": receipt.data_sha256,
                    "manifest_sha256": receipt.manifest_sha256,
                    "remote_data_path": receipt.remote_data_path,
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

    def receipt_path_for_manifest_file(self, manifest_file: Path) -> Path:
        return Path(f"{manifest_file}.offsite.json")

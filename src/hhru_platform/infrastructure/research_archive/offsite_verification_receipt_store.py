from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(slots=True, frozen=True)
class ResearchArchiveOffsiteVerificationReceipt:
    verified_at: datetime
    offsite_url: str
    offsite_root: str
    dataset: str
    layer: str
    row_count: int
    data_size_bytes: int
    data_sha256: str
    manifest_sha256: str
    remote_data_path: str
    remote_manifest_path: str
    verified_object_count: int
    readback_verified: bool


class LocalResearchArchiveOffsiteVerificationReceiptStore:
    def load_receipt(
        self,
        *,
        manifest_file: Path,
    ) -> ResearchArchiveOffsiteVerificationReceipt | None:
        receipt_file = self.receipt_path_for_manifest_file(manifest_file)
        if not receipt_file.exists():
            return None
        payload = json.loads(receipt_file.read_text(encoding="utf-8"))
        return ResearchArchiveOffsiteVerificationReceipt(
            verified_at=datetime.fromisoformat(str(payload["verified_at"])).astimezone(UTC),
            offsite_url=str(payload["offsite_url"]),
            offsite_root=str(payload["offsite_root"]),
            dataset=str(payload["dataset"]),
            layer=str(payload["layer"]),
            row_count=int(payload["row_count"]),
            data_size_bytes=int(payload["data_size_bytes"]),
            data_sha256=str(payload["data_sha256"]),
            manifest_sha256=str(payload["manifest_sha256"]),
            remote_data_path=str(payload["remote_data_path"]),
            remote_manifest_path=str(payload["remote_manifest_path"]),
            verified_object_count=int(payload["verified_object_count"]),
            readback_verified=bool(payload["readback_verified"]),
        )

    def write_receipt(
        self,
        *,
        manifest_file: Path,
        receipt: ResearchArchiveOffsiteVerificationReceipt,
    ) -> Path:
        receipt_file = self.receipt_path_for_manifest_file(manifest_file)
        receipt_file.write_text(
            json.dumps(
                {
                    "verified_at": receipt.verified_at.astimezone(UTC).isoformat(),
                    "offsite_url": receipt.offsite_url,
                    "offsite_root": receipt.offsite_root,
                    "dataset": receipt.dataset,
                    "layer": receipt.layer,
                    "row_count": receipt.row_count,
                    "data_size_bytes": receipt.data_size_bytes,
                    "data_sha256": receipt.data_sha256,
                    "manifest_sha256": receipt.manifest_sha256,
                    "remote_data_path": receipt.remote_data_path,
                    "remote_manifest_path": receipt.remote_manifest_path,
                    "verified_object_count": receipt.verified_object_count,
                    "readback_verified": receipt.readback_verified,
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
        return Path(f"{manifest_file}.offsite.verified.json")

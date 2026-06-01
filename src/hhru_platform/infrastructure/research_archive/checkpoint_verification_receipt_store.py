from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(slots=True, frozen=True)
class ResearchArchiveCheckpointVerificationReceipt:
    verified_at: datetime
    offsite_url: str
    offsite_root: str
    checkpoint_size_bytes: int
    checkpoint_sha256: str
    remote_checkpoint_path: str


class LocalResearchArchiveCheckpointVerificationReceiptStore:
    def load_receipt(
        self,
        *,
        checkpoint_file: Path,
    ) -> ResearchArchiveCheckpointVerificationReceipt | None:
        receipt_file = self.receipt_path_for_checkpoint_file(checkpoint_file)
        if not receipt_file.exists():
            return None
        payload = json.loads(receipt_file.read_text(encoding="utf-8"))
        return ResearchArchiveCheckpointVerificationReceipt(
            verified_at=datetime.fromisoformat(str(payload["verified_at"])).astimezone(UTC),
            offsite_url=str(payload["offsite_url"]),
            offsite_root=str(payload["offsite_root"]),
            checkpoint_size_bytes=int(payload["checkpoint_size_bytes"]),
            checkpoint_sha256=str(payload["checkpoint_sha256"]),
            remote_checkpoint_path=str(payload["remote_checkpoint_path"]),
        )

    def write_receipt(
        self,
        *,
        checkpoint_file: Path,
        receipt: ResearchArchiveCheckpointVerificationReceipt,
    ) -> Path:
        receipt_file = self.receipt_path_for_checkpoint_file(checkpoint_file)
        receipt_file.write_text(
            json.dumps(
                {
                    "verified_at": receipt.verified_at.astimezone(UTC).isoformat(),
                    "offsite_url": receipt.offsite_url,
                    "offsite_root": receipt.offsite_root,
                    "checkpoint_size_bytes": receipt.checkpoint_size_bytes,
                    "checkpoint_sha256": receipt.checkpoint_sha256,
                    "remote_checkpoint_path": receipt.remote_checkpoint_path,
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return receipt_file

    def receipt_path_for_checkpoint_file(self, checkpoint_file: Path) -> Path:
        return Path(f"{checkpoint_file}.offsite.verified.json")

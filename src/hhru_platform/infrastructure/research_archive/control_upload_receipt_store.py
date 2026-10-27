from __future__ import annotations

import json
import logging
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

LOGGER = logging.getLogger(__name__)


class LocalControlUploadReceiptStore:
    """Upload cache only; these receipts never authorize retention or local prune."""

    def matches(self, *, local_file: Path, fingerprint: Mapping[str, object]) -> bool:
        receipt_file = self.receipt_path(local_file)
        try:
            payload = json.loads(receipt_file.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return False
        except (ValueError, UnicodeError):
            LOGGER.warning("ignoring malformed control upload receipt: %s", receipt_file)
            return False
        return isinstance(payload, dict) and all(
            payload.get(key) == value for key, value in fingerprint.items()
        )

    def write_receipt(self, *, local_file: Path, fingerprint: Mapping[str, object]) -> Path:
        receipt_file = self.receipt_path(local_file)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                prefix=f".{receipt_file.name}.",
                dir=receipt_file.parent,
                delete=False,
            ) as output:
                temporary = Path(output.name)
                json.dump(dict(fingerprint), output, sort_keys=True)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            temporary.replace(receipt_file)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return receipt_file

    @staticmethod
    def receipt_path(local_file: Path) -> Path:
        return Path(f"{local_file}.upload-cache.json")

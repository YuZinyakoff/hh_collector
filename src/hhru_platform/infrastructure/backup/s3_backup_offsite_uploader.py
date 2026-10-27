from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol, TypedDict, cast


class S3HeadObjectResponse(TypedDict):
    ContentLength: int


class S3UploadClient(Protocol):
    def upload_file(self, Filename: str, Bucket: str, Key: str, Config: Any = None) -> None:
        """Upload one local file to an S3-compatible object key."""

    def download_file(self, Bucket: str, Key: str, Filename: str, Config: Any = None) -> None:
        """Download one S3-compatible object key into a local file."""

    def head_object(self, *, Bucket: str, Key: str) -> S3HeadObjectResponse:
        """Return metadata for one S3-compatible object."""

    def delete_object(self, *, Bucket: str, Key: str) -> object:
        """Delete one S3-compatible object."""


@dataclass(slots=True, frozen=True)
class S3BackupOffsiteUploader:
    endpoint_url: str
    bucket: str
    key_prefix: str
    client: S3UploadClient
    serial_transfers: bool = False

    def __post_init__(self) -> None:
        normalized_endpoint_url = self.endpoint_url.strip().rstrip("/")
        normalized_bucket = self.bucket.strip()
        normalized_key_prefix = _normalize_key_prefix(self.key_prefix)
        if not normalized_endpoint_url:
            raise ValueError("endpoint_url must not be empty")
        if not normalized_bucket:
            raise ValueError("bucket must not be empty")
        object.__setattr__(self, "endpoint_url", normalized_endpoint_url)
        object.__setattr__(self, "bucket", normalized_bucket)
        object.__setattr__(self, "key_prefix", normalized_key_prefix)

    @classmethod
    def with_credentials(
        cls,
        *,
        endpoint_url: str,
        bucket: str,
        key_prefix: str,
        region_name: str,
        access_key_id: str,
        secret_access_key: str,
        serial_transfers: bool = False,
    ) -> S3BackupOffsiteUploader:
        return cls(
            endpoint_url=endpoint_url,
            bucket=bucket,
            key_prefix=key_prefix,
            client=_create_s3_client(
                endpoint_url=endpoint_url,
                region_name=region_name,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                bounded_requests=serial_transfers,
            ),
            serial_transfers=serial_transfers,
        )

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        key = _join_object_key(self.key_prefix, remote_path)
        if self.serial_transfers:
            self.client.upload_file(
                str(local_file), self.bucket, key, Config=_serial_transfer_config()
            )
        else:
            self.client.upload_file(str(local_file), self.bucket, key)

    def download_file(self, *, local_file: Path, remote_path: str) -> None:
        key = _join_object_key(self.key_prefix, remote_path)
        local_file.parent.mkdir(parents=True, exist_ok=True)
        if self.serial_transfers:
            self.client.download_file(
                self.bucket, key, str(local_file), Config=_serial_transfer_config()
            )
        else:
            self.client.download_file(self.bucket, key, str(local_file))

    def get_file_size(self, *, remote_path: str) -> int:
        key = _join_object_key(self.key_prefix, remote_path)
        response = self.client.head_object(Bucket=self.bucket, Key=key)
        return int(response["ContentLength"])

    def delete_file(self, *, remote_path: str) -> None:
        key = _join_object_key(self.key_prefix, remote_path)
        self.client.delete_object(Bucket=self.bucket, Key=key)


def _create_s3_client(
    *,
    endpoint_url: str,
    region_name: str,
    access_key_id: str,
    secret_access_key: str,
    bounded_requests: bool = False,
) -> S3UploadClient:
    boto3_module = import_module("boto3")
    botocore_config_module = import_module("botocore.config")
    client_factory = cast(Any, boto3_module.__dict__["client"])
    config_cls = cast(Any, botocore_config_module.__dict__["Config"])
    config_options: dict[str, Any] = {
        "signature_version": "s3v4",
        "s3": {"addressing_style": "path"},
    }
    if bounded_requests:
        config_options.update(
            connect_timeout=10,
            read_timeout=60,
            retries={"mode": "standard", "total_max_attempts": 3},
        )
    return cast(
        S3UploadClient,
        client_factory(
            "s3",
            endpoint_url=endpoint_url.strip().rstrip("/"),
            region_name=region_name.strip() or "ru-1",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            config=config_cls(**config_options),
        ),
    )


def _serial_transfer_config() -> Any:
    transfer_module = import_module("boto3.s3.transfer")
    config_cls = cast(Any, transfer_module.__dict__["TransferConfig"])
    return config_cls(use_threads=False, max_concurrency=1, num_download_attempts=3)


def _normalize_key_prefix(key_prefix: str) -> str:
    return "/".join(part for part in key_prefix.strip().split("/") if part)


def _join_object_key(key_prefix: str, remote_path: str) -> str:
    normalized_remote_path = _normalize_key_prefix(remote_path)
    if not normalized_remote_path:
        raise ValueError("remote_path must not be empty")
    if not key_prefix:
        return normalized_remote_path
    return f"{key_prefix}/{normalized_remote_path}"

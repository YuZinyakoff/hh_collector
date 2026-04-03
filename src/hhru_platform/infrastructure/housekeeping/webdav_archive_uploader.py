from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class WebDavTransport(Protocol):
    def request(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None = None,
    ) -> int:
        """Perform one WebDAV request and return the resulting HTTP status code."""


class UrlLibWebDavTransport:
    def __init__(self, *, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds

    def request(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None = None,
    ) -> int:
        request = Request(
            url=url,
            data=body,
            headers=dict(headers),
            method=method,
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                return int(response.status)
        except HTTPError as error:
            return int(error.code)
        except URLError as error:
            raise RuntimeError(f"webdav request failed for {url}: {error.reason}") from error


@dataclass(slots=True, frozen=True)
class WebDavArchiveUploader:
    base_url: str
    remote_root: str
    auth_header: str
    transport: WebDavTransport

    def __post_init__(self) -> None:
        normalized_base_url = self.base_url.rstrip("/")
        if not normalized_base_url:
            raise ValueError("base_url must not be empty")
        if not self.auth_header.strip():
            raise ValueError("auth_header must not be empty")
        object.__setattr__(self, "base_url", normalized_base_url)
        object.__setattr__(self, "remote_root", _normalize_remote_root(self.remote_root))
        object.__setattr__(self, "auth_header", self.auth_header.strip())

    @classmethod
    def with_basic_auth(
        cls,
        *,
        base_url: str,
        remote_root: str,
        username: str,
        password: str,
        timeout_seconds: float,
        transport: WebDavTransport | None = None,
    ) -> WebDavArchiveUploader:
        token = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
        return cls(
            base_url=base_url,
            remote_root=remote_root,
            auth_header=f"Basic {token}",
            transport=transport or UrlLibWebDavTransport(timeout_seconds=timeout_seconds),
        )

    @classmethod
    def with_bearer_token(
        cls,
        *,
        base_url: str,
        remote_root: str,
        bearer_token: str,
        timeout_seconds: float,
        transport: WebDavTransport | None = None,
    ) -> WebDavArchiveUploader:
        return cls(
            base_url=base_url,
            remote_root=remote_root,
            auth_header=f"Bearer {bearer_token.strip()}",
            transport=transport or UrlLibWebDavTransport(timeout_seconds=timeout_seconds),
        )

    def upload_file(self, *, local_file: Path, remote_path: str) -> None:
        parent_parts, file_parts = _split_remote_file_path(remote_path)
        self._ensure_remote_directory(parent_parts)
        full_file_parts = self._with_remote_root(file_parts)
        body = local_file.read_bytes()
        status = self.transport.request(
            method="PUT",
            url=self._url_for_parts(full_file_parts),
            headers=self._headers(
                {
                    "Content-Length": str(len(body)),
                    "Content-Type": "application/octet-stream",
                }
            ),
            body=body,
        )
        if status not in (200, 201, 204):
            raise RuntimeError(
                f"webdav PUT failed for {local_file} -> {self._joined_path(full_file_parts)}: "
                f"status={status}"
            )

    def _ensure_remote_directory(self, relative_dir_parts: tuple[str, ...]) -> None:
        full_parts = self._with_remote_root(())
        for part in relative_dir_parts:
            full_parts = (*full_parts, part)
            status = self.transport.request(
                method="MKCOL",
                url=self._url_for_parts(full_parts),
                headers=self._headers(),
                body=None,
            )
            if status not in (201, 405):
                raise RuntimeError(
                    f"webdav MKCOL failed for {self._joined_path(full_parts)}: status={status}"
                )

    def _url_for_parts(self, parts: tuple[str, ...]) -> str:
        encoded_path = "/".join(quote(part, safe="") for part in parts)
        if not encoded_path:
            return self.base_url
        return f"{self.base_url}/{encoded_path}"

    def _headers(self, extra_headers: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": self.auth_header}
        if extra_headers is not None:
            headers.update(dict(extra_headers))
        return headers

    def _joined_path(self, parts: tuple[str, ...]) -> str:
        return "/" + "/".join(parts)

    def _with_remote_root(self, parts: tuple[str, ...]) -> tuple[str, ...]:
        if not self.remote_root:
            return parts
        return (*tuple(part for part in self.remote_root.split("/") if part), *parts)


def _normalize_remote_root(remote_root: str) -> str:
    parts = tuple(part for part in remote_root.strip().split("/") if part)
    return "/".join(parts)


def _split_remote_file_path(remote_path: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    path_parts = tuple(part for part in remote_path.strip().split("/") if part)
    if not path_parts:
        raise ValueError("remote_path must not be empty")
    return path_parts[:-1], path_parts

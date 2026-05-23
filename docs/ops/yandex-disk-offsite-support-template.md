м# Yandex Disk offsite backup support template

Use this template when contacting Yandex support about WebDAV / REST API upload behavior for
large PostgreSQL backup files.

## Context

We use Yandex Disk as offsite storage for PostgreSQL backups from a VPS-based data collection
service. The target account is an organization/education account with paid Yandex Disk storage.
The expected backup size is currently about 2-3 GB and may grow over time.

## Problem statement

WebDAV upload formally works, but is not usable for automated multi-GB backups.

Observed with `https://webdav.yandex.ru` and target root `/hhru-platform/backups`:

- VPS to WebDAV: 1 MiB upload completes in about 65 seconds.
- VPS to WebDAV: 4 MiB upload completes in about 257 seconds.
- VPS to WebDAV: 8 MiB upload completes in about 513 seconds.
- VPS to WebDAV: 16 MiB upload fails after about 600 seconds with `curl: (52) Empty reply from server`.
- Local Windows machine to WebDAV: 16 MiB upload also fails after about 600 seconds with `curl: (52) Empty reply from server`.
- During failed uploads curl receives interim HTTP `100 Continue`, then the server closes the connection without a final status.
- A full backup upload of about 2.2 GB via WebDAV either times out or hangs for many hours without completing.

This suggests the issue is not isolated to the VPS network path.

## Additional blocker

The `rclone` Yandex backend cannot be authorized for the intended organization/education account.
Yandex authorization page shows:

```text
Выберите другой аккаунт
Этот сервис не работает с аккаунтами вашей организации
```

## Questions for support

1. Are there WebDAV upload throttling limits or timeout limits for Yandex 360 / organization accounts?
2. What is the officially supported way to upload 2-10 GB backup files automatically to paid Yandex Disk storage without the desktop GUI client?
3. Can REST API / OAuth access be enabled for this organization account?
4. Why does WebDAV close the connection on a 16 MiB upload after about 600 seconds with `Empty reply from server`?
5. Is there a recommended API, app permission, or account setting for server-side backup uploads to Yandex Disk?

## Evidence to attach

```text
curl -T 1MiB  -> HTTP 201, ~65s, ~16 KiB/s
curl -T 4MiB  -> HTTP 201, ~257s, ~16 KiB/s
curl -T 8MiB  -> HTTP 201, ~513s, ~16 KiB/s
curl -T 16MiB -> curl 52, HTTP 100 only, ~600s
```

Mention that the same 16 MiB failure reproduces from both VPS and local Windows machine.

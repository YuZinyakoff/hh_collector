from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
from io import BytesIO
from pathlib import Path
from time import monotonic
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/analysis/prepare_search_audit_sample.py"


@pytest.fixture
def sampler():
    spec = importlib.util.spec_from_file_location("prepare_search_audit_sample", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def manifest(root: Path, month: str = "2026-05", token: str = "one", size: int | None = None):
    folder = root / f"v1/bronze/raw_api_payload/request_type=vacancy_search/month={month}"
    folder.mkdir(parents=True, exist_ok=True)
    data = gzip.compress(
        (
            json.dumps(
                {
                    "request_type": "vacancy_search",
                    "raw_api_payload_id": 1,
                    "payload_json": {"items": [], "found": 0},
                }
            )
            + "\n"
        ).encode()
    )
    data_key = (folder / f"{token}.jsonl.gz").relative_to(root).as_posix()
    document = {
        "dataset_key": "bronze/raw_api_payload",
        "partition": {"request_type": "vacancy_search"},
        "source_min_observed_at": f"{month}-01T00:00:00+00:00",
        "row_count": 1,
        "data_file": data_key,
        "data_size_bytes": len(data) if size is None else size,
        "data_sha256": hashlib.sha256(data).hexdigest(),
    }
    path = folder / f"{token}.manifest.json"
    path.write_text(json.dumps(document))
    return path, data_key, data


class Remote:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects
        self.calls = []

    def head_object(self, *, Bucket, Key):
        self.calls.append(("HEAD", Key))
        return {"ContentLength": len(self.objects[Key])}

    def get_object(self, *, Bucket, Key):
        self.calls.append(("GET", Key))
        return {"ContentLength": len(self.objects[Key]), "Body": BytesIO(self.objects[Key])}


def make_plan(sampler, root: Path, **kwargs):
    return sampler.make_plan(
        root,
        per_month=kwargs.get("per_month", 2),
        max_bytes=kwargs.get("max_bytes", 100_000),
        max_chunk_bytes=kwargs.get("max_chunk_bytes", 1000),
        seed="test",
    )


def test_plan_spreads_six_months_deterministically_without_network(sampler, tmp_path, monkeypatch):
    source = tmp_path / "source"
    for month in range(5, 11):
        for token in ("one", "two", "three"):
            manifest(source, f"2026-{month:02}", token)
    monkeypatch.setattr(sampler, "build_remote", lambda: pytest.fail("plan contacted S3"))
    plan = make_plan(sampler, source)
    assert len(plan["entries"]) == 12
    assert all(month["selected_chunks"] == 2 for month in plan["months"])
    assert plan == {**make_plan(sampler, source), "created_at": plan["created_at"]}
    assert plan["planned_download_bytes"] <= plan["max_total_bytes"]
    assert not list(source.rglob("*.jsonl.gz"))  # Metadata-only source is sufficient.
    output = tmp_path / "report"
    assert sampler.main(["plan", "--archive-dir", str(source), "--output-dir", str(output)]) == 0
    assert json.loads((output / "plan.json").read_text())["live_network_requests"] == 0
    assert output.stat().st_mode & 0o077 == 0


def test_size_caps_and_unrepresented_months_are_reported(sampler, tmp_path):
    source = tmp_path / "source"
    manifest(source, "2026-05", size=50_000)
    manifest(source, "2026-06")
    plan = make_plan(sampler, source, max_chunk_bytes=1000, max_bytes=100_000)
    assert plan["months"][0]["selected_chunks"] == 0
    assert plan["months"][0]["over_chunk_size_limit"] == 1
    assert plan["months"][1]["selected_chunks"] == 1
    tiny = make_plan(sampler, source, max_bytes=1)
    assert not tiny["entries"]
    with pytest.raises(sampler.SampleError, match="empty_plan"):
        sampler.validate_plan(tiny, max_bytes=100_000)


@pytest.mark.parametrize(
    "bad",
    [
        "/etc/passwd",
        "../private",
        "v1/../private",
        "v1\\private",
        "v1/bronze/raw_api_payload/request_type=vacancy_search/../private",
    ],
)
def test_path_escape_is_rejected(sampler, tmp_path, bad):
    with pytest.raises(sampler.SampleError):
        sampler.safe_path(tmp_path, bad)


def test_plan_tampering_and_duplicate_keys_fail_before_network(sampler, tmp_path, monkeypatch):
    source = tmp_path / "source"
    manifest(source)
    plan = make_plan(sampler, source)
    monkeypatch.setattr(sampler, "build_remote", lambda: pytest.fail("invalid plan contacted S3"))
    with pytest.raises(sampler.SampleError, match="budget"):
        sampler.download_plan(plan, tmp_path / "sample", max_bytes=1, seconds=5)
    assert not (tmp_path / "sample").exists()
    plan["entries"] *= 2
    with pytest.raises(sampler.SampleError, match="duplicate"):
        sampler.validate_plan(plan, max_bytes=100_000)


def test_download_reads_only_planned_keys_checks_hashes_and_preserves_source(
    sampler, tmp_path, monkeypatch
):
    source = tmp_path / "source"
    path, data_key, data = manifest(source)
    original = path.read_bytes()
    plan = make_plan(sampler, source)
    remote = Remote(
        {f"archive/{path.relative_to(source).as_posix()}": original, f"archive/{data_key}": data}
    )
    monkeypatch.setattr(sampler, "build_remote", lambda: (remote, "bucket", "archive"))
    root = tmp_path / "output" / "archive"
    result = sampler.download_plan(plan, root, max_bytes=100_000, seconds=5)
    assert result["status"] == "download_verified"
    assert result["verified_chunks"] == 1
    assert result["verified_bytes"] == len(original) + len(data)
    assert len(remote.calls) == 4
    assert {method for method, key in remote.calls} == {"GET", "HEAD"}
    assert (root / data_key).read_bytes() == data
    assert path.read_bytes() == original
    assert not (source / data_key).exists()
    with pytest.raises(FileExistsError):
        sampler.download_plan(plan, root, max_bytes=100_000, seconds=5)


@pytest.mark.parametrize("failure", ["head", "get", "stream", "hash", "short"])
def test_download_rejects_size_races_overruns_and_checksum_failures(sampler, tmp_path, failure):
    data = b"expected-data"
    body = BytesIO(data)
    remote = MagicMock()
    remote.head_object.return_value = {"ContentLength": len(data) + (failure == "head")}
    remote.get_object.return_value = {
        "ContentLength": len(data) + (failure == "get"),
        "Body": body,
    }
    if failure == "stream":
        body = BytesIO(data + b"extra")
        remote.get_object.return_value["Body"] = body
    if failure == "short":
        body = BytesIO(data[:-1])
        remote.get_object.return_value["Body"] = body
    destination = tmp_path / "data"
    with pytest.raises(sampler.SampleError):
        sampler.download_object(
            remote,
            bucket="bucket",
            key="key",
            destination=destination,
            expected_bytes=len(data),
            expected_sha256="0" * 64 if failure == "hash" else hashlib.sha256(data).hexdigest(),
            deadline=monotonic() + 5,
        )
    assert not destination.exists()
    assert not destination.with_name("data.partial").exists()
    if failure == "head":
        remote.get_object.assert_not_called()
    else:
        assert body.closed


def test_existing_partial_is_not_deleted_and_deadline_prevents_network(sampler, tmp_path):
    remote = Remote({"key": b"data"})
    destination = tmp_path / "data"
    partial = tmp_path / "data.partial"
    partial.write_bytes(b"do-not-touch")
    with pytest.raises(FileExistsError):
        sampler.download_object(
            remote,
            bucket="b",
            key="key",
            destination=destination,
            expected_bytes=4,
            expected_sha256=hashlib.sha256(b"data").hexdigest(),
            deadline=monotonic() + 5,
        )
    assert partial.read_bytes() == b"do-not-touch"
    remote.calls.clear()
    with pytest.raises(sampler.SampleError, match="time_budget"):
        sampler.download_object(
            remote,
            bucket="b",
            key="key",
            destination=destination,
            expected_bytes=4,
            expected_sha256="0" * 64,
            deadline=0,
        )
    assert not remote.calls


def test_remote_manifest_cannot_retarget_plan(sampler, tmp_path, monkeypatch):
    source = tmp_path / "source"
    path, _, _ = manifest(source)
    document = json.loads(path.read_bytes())
    document["data_file"] = "../escape"
    raw = json.dumps(document).encode()
    plan = make_plan(sampler, source)
    plan["entries"][0].update(
        manifest_bytes=len(raw), manifest_sha256=hashlib.sha256(raw).hexdigest()
    )
    plan["planned_download_bytes"] = sum(
        e["manifest_bytes"] + e["data_bytes"] for e in plan["entries"]
    )
    remote = Remote({f"archive/{path.relative_to(source).as_posix()}": raw})
    monkeypatch.setattr(sampler, "build_remote", lambda: (remote, "b", "archive"))
    result = sampler.download_plan(plan, tmp_path / "output/archive", max_bytes=100_000, seconds=5)
    assert result["status"] == "incomplete"
    assert result["error_code"] == "remote_manifest_differs_from_plan"
    assert len(remote.calls) == 2


def test_exception_credentials_are_not_disclosed(sampler, tmp_path, monkeypatch, capsys):
    source, output = tmp_path / "source", tmp_path / "output"
    manifest(source)
    plan = make_plan(sampler, source)
    output.mkdir()
    plan_file = output / "plan.json"
    plan_file.write_text(json.dumps(plan))

    def fail():
        raise RuntimeError("https://USER:SECRET@example.com")

    monkeypatch.setattr(sampler, "build_remote", fail)
    assert sampler.main(["download", "--plan-file", str(plan_file)]) == 2
    assert "SECRET" not in capsys.readouterr().out
    assert "SECRET" not in (output / "download-report.json").read_text()


def test_missing_remote_object_is_recorded_without_s3_exception_body(
    sampler, tmp_path, monkeypatch
):
    source = tmp_path / "source"
    manifest(source)
    plan = make_plan(sampler, source)

    class Missing(Exception):
        response = {"Error": {"Code": "NoSuchKey", "Message": "SECRET"}}

    remote = MagicMock()
    remote.head_object.side_effect = Missing("SECRET")
    monkeypatch.setattr(sampler, "build_remote", lambda: (remote, "bucket", "archive"))
    result = sampler.download_plan(plan, tmp_path / "output/archive", max_bytes=100_000, seconds=5)
    assert result["status"] == "incomplete"
    assert result["error_code"] == "NoSuchKey"
    assert "SECRET" not in json.dumps(result)
    remote.get_object.assert_not_called()


def test_downloaded_sample_is_readable_by_existing_archive_auditor(sampler, tmp_path, monkeypatch):
    source = tmp_path / "source"
    path, data_key, data = manifest(source)
    plan = make_plan(sampler, source)
    remote = Remote(
        {
            f"archive/{path.relative_to(source).as_posix()}": path.read_bytes(),
            f"archive/{data_key}": data,
        }
    )
    monkeypatch.setattr(sampler, "build_remote", lambda: (remote, "bucket", "archive"))
    root = tmp_path / "sample/archive"
    assert (
        sampler.download_plan(plan, root, max_bytes=100_000, seconds=5)["status"]
        == "download_verified"
    )
    script = SCRIPT.with_name("audit_search_quality.py")
    spec = importlib.util.spec_from_file_location("sample_roundtrip_audit", script)
    auditor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(auditor)
    reference = tmp_path / "area-reference.json"
    reference.write_text(json.dumps({"captured_at": "2026-11-07T00:00:00Z", "areas": []}))
    output = tmp_path / "audit-report"
    assert (
        auditor.main(
            [
                "--archive-dir",
                str(root),
                "--area-reference",
                str(reference),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    report = json.loads((output / "summary.json").read_text())
    assert report["archive_counts"]["chunks_verified"] == 1
    assert report["archive_counts"]["rows_read"] == 1
    assert report["counts"]["responses"] == 1


def test_remote_client_has_bounded_timeouts_credentials_fallback_and_normalized_prefix(
    sampler, monkeypatch
):
    import boto3

    import hhru_platform.config.settings as settings

    values = {
        "research_archive_offsite_backend": "s3",
        "research_archive_offsite_root": " /root//archive/ ",
    }
    for suffix, value in {
        "endpoint_url": " https://s3.example/ ",
        "bucket": "bucket",
        "region": "ru-1",
        "access_key_id": "test-access",
        "secret_access_key": "test-secret",
    }.items():
        values[f"research_archive_offsite_s3_{suffix}"] = ""
        values[f"backup_offsite_s3_{suffix}"] = value
    monkeypatch.setattr(settings, "get_settings", lambda: SimpleNamespace(**values))
    factory = MagicMock()
    monkeypatch.setattr(boto3, "client", factory)
    client, bucket, prefix = sampler.build_remote()
    assert client is factory.return_value
    assert bucket == "bucket" and prefix == "root/archive"
    kwargs = factory.call_args.kwargs
    assert kwargs["endpoint_url"] == "https://s3.example"
    assert kwargs["aws_access_key_id"] == "test-access"
    assert kwargs["aws_secret_access_key"] == "test-secret"
    assert kwargs["config"].connect_timeout == 10
    assert kwargs["config"].read_timeout == 60
    assert kwargs["config"].retries["total_max_attempts"] == 3
    values["backup_offsite_s3_endpoint_url"] = "http://insecure.example"
    factory.reset_mock()
    with pytest.raises(sampler.SampleError, match="requires_https"):
        sampler.build_remote()
    factory.assert_not_called()

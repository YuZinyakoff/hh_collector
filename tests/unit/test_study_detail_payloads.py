from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailResult,
)
from hhru_platform.application.commands.study_detail_payloads import (
    DetailStudyCandidate,
    StoredRawPayload,
    StudyDetailPayloadsCommand,
    study_detail_payloads,
)


def test_study_detail_payloads_writes_report_and_recommends_selective_detail(
    tmp_path: Path,
) -> None:
    run_id = uuid4()
    vacancy_id = uuid4()
    seen_at = datetime(2026, 3, 19, 10, 0, tzinfo=UTC)

    candidates = [
        DetailStudyCandidate(
            vacancy_id=vacancy_id,
            hh_vacancy_id="123",
            crawl_run_id=run_id,
            seen_at=seen_at,
            short_payload_ref_id=11,
        )
    ]
    payloads = {
        11: StoredRawPayload(
            id=11,
            endpoint_type="vacancies.search",
            entity_hh_id=None,
            payload_hash="search-hash",
            received_at=seen_at,
            payload_json={
                "items": [
                    {
                        "id": "123",
                        "name": "Data Engineer",
                        "alternate_url": "https://hh.example/123",
                        "employment": {"id": "full"},
                    }
                ]
            },
        ),
        101: StoredRawPayload(
            id=101,
            endpoint_type="vacancies.detail",
            entity_hh_id="123",
            payload_hash="detail-hash-1",
            received_at=datetime(2026, 3, 19, 10, 5, tzinfo=UTC),
            payload_json={
                "id": "123",
                "name": "Data Engineer",
                "description": "Build pipelines and batch jobs.",
                "alternate_url": "https://hh.example/123",
                "area": {"id": "1", "name": "Moscow"},
                "employer": {"id": "77", "name": "ACME"},
                "employment": {"id": "full"},
                "schedule": {"id": "fullDay"},
                "experience": {"id": "between1And3"},
                "professional_roles": [{"id": "96"}],
                "key_skills": [{"name": "Python"}, {"name": "SQL"}],
                "salary": {"from": 200000, "to": 260000, "currency": "RUR"},
                "published_at": "2026-03-19T09:00:00+0300",
                "initial_created_at": "2026-03-18T09:00:00+0300",
            },
        ),
        102: StoredRawPayload(
            id=102,
            endpoint_type="vacancies.detail",
            entity_hh_id="123",
            payload_hash="detail-hash-2",
            received_at=datetime(2026, 3, 19, 10, 6, tzinfo=UTC),
            payload_json={
                "id": "123",
                "name": "Data Engineer",
                "description": "Build pipelines and batch jobs.",
                "alternate_url": "https://hh.example/123",
                "area": {"id": "1", "name": "Moscow"},
                "employer": {"id": "77", "name": "ACME"},
                "employment": {"id": "full"},
                "schedule": {"id": "fullDay"},
                "experience": {"id": "between1And3"},
                "professional_roles": [{"id": "96"}],
                "key_skills": [{"name": "Python"}, {"name": "SQL"}],
                "salary": {"from": 200000, "to": 260000, "currency": "RUR"},
                "published_at": "2026-03-19T09:00:00+0300",
                "initial_created_at": "2026-03-18T09:00:00+0300",
            },
        ),
    }
    raw_payload_ids = iter((101, 102))

    def fake_resolve_latest_crawl_run_id() -> object:
        return run_id

    def fake_load_candidates(resolved_run_id, sample_size):
        assert resolved_run_id == run_id
        assert sample_size == 1
        return candidates

    def fake_load_raw_payload(payload_id):
        return payloads.get(payload_id)

    def fake_fetch_detail(command) -> FetchVacancyDetailResult:
        payload_id = next(raw_payload_ids)
        return FetchVacancyDetailResult(
            vacancy_id=vacancy_id,
            hh_vacancy_id="123",
            detail_fetch_status="succeeded",
            snapshot_id=payload_id,
            request_log_id=payload_id,
            raw_payload_id=payload_id,
            detail_fetch_attempt_id=payload_id,
            error_message=None,
        )

    result = study_detail_payloads(
        StudyDetailPayloadsCommand(
            sample_size=1,
            repeat_fetches=1,
            pause_seconds=0.0,
            crawl_run_id=None,
            output_dir=tmp_path,
        ),
        resolve_latest_crawl_run_id_step=fake_resolve_latest_crawl_run_id,
        load_candidates_step=fake_load_candidates,
        load_raw_payload_step=fake_load_raw_payload,
        fetch_detail_step=fake_fetch_detail,
    )

    report_payload = json.loads(result.report_json_path.read_text(encoding="utf-8"))
    summary_markdown = result.summary_markdown_path.read_text(encoding="utf-8")

    assert result.crawl_run_id == run_id
    assert result.sample_size_selected == 1
    assert result.vacancies_with_search_sample == 1
    assert result.vacancies_with_successful_detail == 1
    assert result.raw_comparable_pairs == 1
    assert result.raw_changed_pairs == 0
    assert result.normalized_changed_pairs == 0
    assert "description" in result.detail_only_research_fields
    assert "key_skills[].name" in result.detail_only_research_fields
    assert result.report_directory.parent == tmp_path
    assert report_payload["summary"]["conclusion"]["recommended_policy"].startswith(
        "Prefer exhaustive list coverage plus selective detail fetches"
    )
    assert "Detail-only research fields" in summary_markdown
    assert "description" in summary_markdown

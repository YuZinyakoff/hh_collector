from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import sleep
from typing import Any
from uuid import UUID

from hhru_platform.application.commands.fetch_vacancy_detail import (
    FetchVacancyDetailCommand,
    FetchVacancyDetailResult,
)
from hhru_platform.infrastructure.normalization.vacancy_detail_normalizer import (
    VacancyDetailNormalizationError,
    normalize_vacancy_detail,
)
from hhru_platform.infrastructure.observability.operations import (
    log_operation_started,
    record_operation_failed,
    record_operation_succeeded,
)

LOGGER = logging.getLogger(__name__)

IMPORTANT_DETAIL_PATH_PREFIXES = (
    "description",
    "branded_description",
    "key_skills[]",
    "salary.",
    "salary_range.",
    "address.",
    "contacts.",
    "languages[]",
    "working_days[]",
    "working_time_intervals[]",
    "work_format[]",
    "work_schedule_by_days[]",
)
TOP_PATH_LIMIT = 20


class DetailPayloadStudyError(RuntimeError):
    """Raised when the detail payload study cannot proceed."""


@dataclass(slots=True, frozen=True)
class StudyDetailPayloadsCommand:
    sample_size: int = 5
    repeat_fetches: int = 2
    pause_seconds: float = 0.0
    crawl_run_id: UUID | None = None
    output_dir: Path = Path(".state/reports/detail-payload-study")

    def __post_init__(self) -> None:
        if self.sample_size < 1:
            raise ValueError("sample_size must be greater than or equal to one")
        if self.repeat_fetches < 1:
            raise ValueError("repeat_fetches must be greater than or equal to one")
        if self.pause_seconds < 0:
            raise ValueError("pause_seconds must be greater than or equal to zero")

        object.__setattr__(self, "output_dir", Path(self.output_dir))


@dataclass(slots=True, frozen=True)
class DetailStudyCandidate:
    vacancy_id: UUID
    hh_vacancy_id: str
    crawl_run_id: UUID
    seen_at: datetime
    short_payload_ref_id: int


@dataclass(slots=True, frozen=True)
class StoredRawPayload:
    id: int
    endpoint_type: str
    entity_hh_id: str | None
    payload_hash: str
    received_at: datetime
    payload_json: object


@dataclass(slots=True, frozen=True)
class StudyDetailPayloadsResult:
    crawl_run_id: UUID
    sample_size_requested: int
    sample_size_selected: int
    vacancies_with_search_sample: int
    vacancies_with_successful_detail: int
    raw_comparable_pairs: int
    raw_changed_pairs: int
    normalized_comparable_pairs: int
    normalized_changed_pairs: int
    report_directory: Path
    report_json_path: Path
    summary_markdown_path: Path
    recommendation: str
    detail_only_research_fields: tuple[str, ...]


ResolveLatestRunStep = Callable[[], UUID | None]
LoadCandidatesStep = Callable[[UUID, int], list[DetailStudyCandidate]]
LoadRawPayloadStep = Callable[[int], StoredRawPayload | None]
FetchDetailStep = Callable[[FetchVacancyDetailCommand], FetchVacancyDetailResult]


@dataclass(slots=True)
class _DetailFetchObservation:
    round_index: int
    reason: str
    request_log_id: int | None
    raw_payload_id: int | None
    snapshot_id: int | None
    detail_fetch_attempt_id: int | None
    payload_hash: str | None
    received_at: datetime | None
    error_message: str | None
    raw_leaf_values: dict[str, tuple[str, ...]] | None
    normalized_leaf_values: dict[str, tuple[str, ...]] | None
    normalized_detail_hash: str | None


def study_detail_payloads(
    command: StudyDetailPayloadsCommand,
    *,
    resolve_latest_crawl_run_id_step: ResolveLatestRunStep,
    load_candidates_step: LoadCandidatesStep,
    load_raw_payload_step: LoadRawPayloadStep,
    fetch_detail_step: FetchDetailStep,
) -> StudyDetailPayloadsResult:
    started_at = log_operation_started(
        LOGGER,
        operation="study_detail_payloads",
        sample_size=command.sample_size,
        repeat_fetches=command.repeat_fetches,
        pause_seconds=command.pause_seconds,
        crawl_run_id=command.crawl_run_id,
    )
    try:
        resolved_run_id = command.crawl_run_id or resolve_latest_crawl_run_id_step()
        if resolved_run_id is None:
            raise DetailPayloadStudyError("no crawl_run with search payload samples found")

        candidates = load_candidates_step(resolved_run_id, command.sample_size)
        if not candidates:
            raise DetailPayloadStudyError(
                f"no vacancy samples found for crawl_run {resolved_run_id}"
            )

        report_directory = _build_report_directory(command.output_dir)
        search_samples = _load_search_samples(candidates, load_raw_payload_step)
        observations_by_vacancy: dict[UUID, list[_DetailFetchObservation]] = defaultdict(list)
        study_token = report_directory.name

        for round_index in range(command.repeat_fetches + 1):
            if round_index > 0 and command.pause_seconds > 0:
                sleep(command.pause_seconds)

            for candidate in candidates:
                observation = _fetch_detail_observation(
                    candidate=candidate,
                    round_index=round_index,
                    study_token=study_token,
                    load_raw_payload_step=load_raw_payload_step,
                    fetch_detail_step=fetch_detail_step,
                )
                observations_by_vacancy[candidate.vacancy_id].append(observation)

        report_payload = _build_report_payload(
            crawl_run_id=resolved_run_id,
            command=command,
            candidates=candidates,
            search_samples=search_samples,
            observations_by_vacancy=observations_by_vacancy,
        )
        _write_report_files(report_directory, report_payload)
    except Exception as error:
        record_operation_failed(
            LOGGER,
            operation="study_detail_payloads",
            started_at=started_at,
            error_type=error.__class__.__name__,
            error_message=str(error),
            sample_size=command.sample_size,
            repeat_fetches=command.repeat_fetches,
            crawl_run_id=command.crawl_run_id,
        )
        raise

    summary = report_payload["summary"]
    result = StudyDetailPayloadsResult(
        crawl_run_id=resolved_run_id,
        sample_size_requested=command.sample_size,
        sample_size_selected=len(candidates),
        vacancies_with_search_sample=summary["vacancies_with_search_sample"],
        vacancies_with_successful_detail=summary["vacancies_with_successful_detail"],
        raw_comparable_pairs=summary["raw_comparable_pairs"],
        raw_changed_pairs=summary["raw_changed_pairs"],
        normalized_comparable_pairs=summary["normalized_comparable_pairs"],
        normalized_changed_pairs=summary["normalized_changed_pairs"],
        report_directory=report_directory,
        report_json_path=report_directory / "report.json",
        summary_markdown_path=report_directory / "summary.md",
        recommendation=summary["conclusion"]["recommended_policy"],
        detail_only_research_fields=tuple(summary["detail_only_research_fields"]),
    )
    record_operation_succeeded(
        LOGGER,
        operation="study_detail_payloads",
        started_at=started_at,
        records_written=None,
        crawl_run_id=str(result.crawl_run_id),
        sample_size_selected=result.sample_size_selected,
        vacancies_with_successful_detail=result.vacancies_with_successful_detail,
        raw_changed_pairs=result.raw_changed_pairs,
        normalized_changed_pairs=result.normalized_changed_pairs,
        report_directory=str(result.report_directory),
    )
    return result


def _build_report_directory(base_directory: Path) -> Path:
    report_directory = base_directory / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_directory.mkdir(parents=True, exist_ok=False)
    return report_directory


def _load_search_samples(
    candidates: list[DetailStudyCandidate],
    load_raw_payload_step: LoadRawPayloadStep,
) -> dict[UUID, dict[str, Any]]:
    samples: dict[UUID, dict[str, Any]] = {}
    for candidate in candidates:
        raw_payload = load_raw_payload_step(candidate.short_payload_ref_id)
        if raw_payload is None:
            samples[candidate.vacancy_id] = {
                "payload_ref_id": candidate.short_payload_ref_id,
                "item": None,
                "leaf_values": None,
                "error": "search raw payload not found",
            }
            continue

        try:
            item_payload = _extract_search_item(
                raw_payload.payload_json,
                hh_vacancy_id=candidate.hh_vacancy_id,
            )
        except DetailPayloadStudyError as error:
            samples[candidate.vacancy_id] = {
                "payload_ref_id": raw_payload.id,
                "item": None,
                "leaf_values": None,
                "error": str(error),
            }
            continue

        samples[candidate.vacancy_id] = {
            "payload_ref_id": raw_payload.id,
            "item": item_payload,
            "leaf_values": _collect_leaf_values(item_payload),
            "error": None,
        }
    return samples


def _extract_search_item(payload_json: object, *, hh_vacancy_id: str) -> dict[str, Any]:
    if not isinstance(payload_json, dict):
        raise DetailPayloadStudyError("search payload must be a JSON object")

    items = payload_json.get("items")
    if not isinstance(items, list):
        raise DetailPayloadStudyError("search payload.items must be a list")

    for item in items:
        if isinstance(item, dict) and item.get("id") == hh_vacancy_id:
            return item

    raise DetailPayloadStudyError(
        f"vacancy {hh_vacancy_id} not found inside referenced search payload"
    )


def _fetch_detail_observation(
    *,
    candidate: DetailStudyCandidate,
    round_index: int,
    study_token: str,
    load_raw_payload_step: LoadRawPayloadStep,
    fetch_detail_step: FetchDetailStep,
) -> _DetailFetchObservation:
    reason = f"detail_policy_study:{study_token}:r{round_index}"
    try:
        fetch_result = fetch_detail_step(
            FetchVacancyDetailCommand(
                vacancy_id=candidate.vacancy_id,
                reason=reason,
                attempt=1,
                crawl_run_id=candidate.crawl_run_id,
            )
        )
    except Exception as error:
        return _DetailFetchObservation(
            round_index=round_index,
            reason=reason,
            request_log_id=None,
            raw_payload_id=None,
            snapshot_id=None,
            detail_fetch_attempt_id=None,
            payload_hash=None,
            received_at=None,
            error_message=str(error),
            raw_leaf_values=None,
            normalized_leaf_values=None,
            normalized_detail_hash=None,
        )

    raw_payload = None
    if fetch_result.raw_payload_id is not None:
        raw_payload = load_raw_payload_step(fetch_result.raw_payload_id)

    normalized_leaf_values: dict[str, tuple[str, ...]] | None = None
    normalized_detail_hash: str | None = None
    error_message = fetch_result.error_message
    if raw_payload is not None:
        try:
            normalized_detail = normalize_vacancy_detail(raw_payload.payload_json)
        except VacancyDetailNormalizationError as error:
            error_message = error_message or str(error)
        else:
            normalized_leaf_values = _collect_leaf_values(normalized_detail.normalized_json)
            normalized_detail_hash = normalized_detail.normalized_hash

    return _DetailFetchObservation(
        round_index=round_index,
        reason=reason,
        request_log_id=fetch_result.request_log_id,
        raw_payload_id=fetch_result.raw_payload_id,
        snapshot_id=fetch_result.snapshot_id,
        detail_fetch_attempt_id=fetch_result.detail_fetch_attempt_id,
        payload_hash=raw_payload.payload_hash if raw_payload is not None else None,
        received_at=raw_payload.received_at if raw_payload is not None else None,
        error_message=error_message,
        raw_leaf_values=(
            _collect_leaf_values(raw_payload.payload_json) if raw_payload is not None else None
        ),
        normalized_leaf_values=normalized_leaf_values,
        normalized_detail_hash=normalized_detail_hash,
    )


def _build_report_payload(
    *,
    crawl_run_id: UUID,
    command: StudyDetailPayloadsCommand,
    candidates: list[DetailStudyCandidate],
    search_samples: dict[UUID, dict[str, Any]],
    observations_by_vacancy: dict[UUID, list[_DetailFetchObservation]],
) -> dict[str, Any]:
    detail_only_counter: Counter[str] = Counter()
    detail_non_null_counter: Counter[str] = Counter()
    important_detail_only_counter: Counter[str] = Counter()
    raw_changed_path_counter: Counter[str] = Counter()
    normalized_changed_path_counter: Counter[str] = Counter()
    raw_changed_pairs = 0
    raw_comparable_pairs = 0
    normalized_changed_pairs = 0
    normalized_comparable_pairs = 0

    vacancy_reports: list[dict[str, Any]] = []
    vacancies_with_search_sample = 0
    vacancies_with_successful_detail = 0

    for candidate in candidates:
        search_sample = search_samples[candidate.vacancy_id]
        search_leaf_values = search_sample["leaf_values"]
        if search_leaf_values is not None:
            vacancies_with_search_sample += 1

        observations = observations_by_vacancy[candidate.vacancy_id]
        first_successful_detail = next(
            (
                observation
                for observation in observations
                if observation.raw_leaf_values is not None
            ),
            None,
        )
        if first_successful_detail is not None:
            vacancies_with_successful_detail += 1

        detail_only_paths: list[str] = []
        detail_non_null_paths: list[str] = []
        important_detail_only_paths: list[str] = []
        if search_leaf_values is not None and first_successful_detail is not None:
            detail_only_paths = sorted(
                set(first_successful_detail.raw_leaf_values or ()) - set(search_leaf_values)
            )
            detail_non_null_paths = sorted(
                path
                for path, values in (first_successful_detail.raw_leaf_values or {}).items()
                if not _values_are_effectively_null(values)
                and (
                    path not in search_leaf_values
                    or _values_are_effectively_null(search_leaf_values[path])
                )
            )
            important_detail_only_paths = [
                path for path in detail_non_null_paths if _is_research_important_detail_path(path)
            ]
            detail_only_counter.update(detail_only_paths)
            detail_non_null_counter.update(detail_non_null_paths)
            important_detail_only_counter.update(important_detail_only_paths)

        repeated_pairs: list[dict[str, Any]] = []
        for previous_observation, current_observation in zip(
            observations,
            observations[1:],
            strict=False,
        ):
            raw_changed_paths: list[str] = []
            normalized_changed_paths: list[str] = []
            raw_payload_hash_changed = False
            normalized_detail_hash_changed = False

            if (
                previous_observation.raw_leaf_values is not None
                and current_observation.raw_leaf_values is not None
            ):
                raw_comparable_pairs += 1
                raw_changed_paths = _changed_paths(
                    previous_observation.raw_leaf_values,
                    current_observation.raw_leaf_values,
                )
                raw_payload_hash_changed = (
                    previous_observation.payload_hash != current_observation.payload_hash
                )
                if raw_changed_paths:
                    raw_changed_pairs += 1
                    raw_changed_path_counter.update(raw_changed_paths)

            if (
                previous_observation.normalized_leaf_values is not None
                and current_observation.normalized_leaf_values is not None
            ):
                normalized_comparable_pairs += 1
                normalized_changed_paths = _changed_paths(
                    previous_observation.normalized_leaf_values,
                    current_observation.normalized_leaf_values,
                )
                normalized_detail_hash_changed = (
                    previous_observation.normalized_detail_hash
                    != current_observation.normalized_detail_hash
                )
                if normalized_changed_paths:
                    normalized_changed_pairs += 1
                    normalized_changed_path_counter.update(normalized_changed_paths)

            repeated_pairs.append(
                {
                    "from_round": previous_observation.round_index,
                    "to_round": current_observation.round_index,
                    "raw_payload_hash_changed": raw_payload_hash_changed,
                    "normalized_detail_hash_changed": normalized_detail_hash_changed,
                    "raw_changed_paths": raw_changed_paths,
                    "normalized_changed_paths": normalized_changed_paths,
                }
            )

        vacancy_reports.append(
            {
                "vacancy_id": str(candidate.vacancy_id),
                "hh_vacancy_id": candidate.hh_vacancy_id,
                "crawl_run_id": str(candidate.crawl_run_id),
                "seen_at": candidate.seen_at.isoformat(),
                "search_payload_ref_id": candidate.short_payload_ref_id,
                "search_sample_error": search_sample["error"],
                "search_vs_first_detail": {
                    "detail_only_paths": detail_only_paths,
                    "detail_non_null_search_missing_or_null_paths": detail_non_null_paths,
                    "detail_only_research_paths": important_detail_only_paths,
                },
                "detail_fetches": [
                    {
                        "round_index": observation.round_index,
                        "reason": observation.reason,
                        "request_log_id": observation.request_log_id,
                        "raw_payload_id": observation.raw_payload_id,
                        "snapshot_id": observation.snapshot_id,
                        "detail_fetch_attempt_id": observation.detail_fetch_attempt_id,
                        "payload_hash": observation.payload_hash,
                        "received_at": (
                            observation.received_at.isoformat()
                            if observation.received_at is not None
                            else None
                        ),
                        "normalized_detail_hash": observation.normalized_detail_hash,
                        "error_message": observation.error_message,
                    }
                    for observation in observations
                ],
                "repeated_detail_pairs": repeated_pairs,
            }
        )

    raw_changed_pair_rate = _safe_ratio(raw_changed_pairs, raw_comparable_pairs)
    normalized_changed_pair_rate = _safe_ratio(
        normalized_changed_pairs,
        normalized_comparable_pairs,
    )
    detail_only_research_fields = sorted(important_detail_only_counter)
    summary: dict[str, object] = {
        "vacancies_with_search_sample": vacancies_with_search_sample,
        "vacancies_with_successful_detail": vacancies_with_successful_detail,
        "raw_comparable_pairs": raw_comparable_pairs,
        "raw_changed_pairs": raw_changed_pairs,
        "raw_changed_pair_rate": raw_changed_pair_rate,
        "normalized_comparable_pairs": normalized_comparable_pairs,
        "normalized_changed_pairs": normalized_changed_pairs,
        "normalized_changed_pair_rate": normalized_changed_pair_rate,
        "detail_only_research_fields": detail_only_research_fields,
    }
    summary["conclusion"] = _build_conclusion(
        detail_only_research_fields=detail_only_research_fields,
        raw_changed_pair_rate=raw_changed_pair_rate,
        normalized_changed_pair_rate=normalized_changed_pair_rate,
    )

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "crawl_run_id": str(crawl_run_id),
        "input": {
            "sample_size_requested": command.sample_size,
            "repeat_fetches": command.repeat_fetches,
            "pause_seconds": command.pause_seconds,
            "output_dir": str(command.output_dir),
        },
        "summary": summary,
        "aggregates": {
            "detail_only_path_counts": _counter_rows(detail_only_counter),
            "detail_non_null_search_missing_or_null_counts": _counter_rows(
                detail_non_null_counter
            ),
            "detail_only_research_field_counts": _counter_rows(important_detail_only_counter),
            "repeated_raw_changed_path_counts": _counter_rows(raw_changed_path_counter),
            "repeated_normalized_changed_path_counts": _counter_rows(
                normalized_changed_path_counter
            ),
        },
        "vacancies": vacancy_reports,
    }


def _build_conclusion(
    *,
    detail_only_research_fields: list[str],
    raw_changed_pair_rate: float,
    normalized_changed_pair_rate: float,
) -> dict[str, object]:
    if not detail_only_research_fields:
        recommended_policy = (
            "Search payload already covers most observed useful fields. Exhaustive list "
            "coverage should remain the primary strategy, and detail can stay selective."
        )
        stability_summary = (
            "No strong detail-only research fields were observed in the sample."
        )
        rationale = [
            (
                "The sampled detail payloads did not add a stable set of clearly unique "
                "research fields."
            ),
            (
                "A first-seen or manual detail policy is likely enough unless later "
                "samples show otherwise."
            ),
        ]
        return {
            "detail_value_summary": stability_summary,
            "stability_summary": stability_summary,
            "recommended_policy": recommended_policy,
            "rationale": rationale,
        }

    if normalized_changed_pair_rate <= 0.2:
        stability_summary = (
            "Repeated detail fetches are mostly stable on normalized fields; any raw drift is "
            "likely limited or noisy."
        )
        recommended_policy = (
            "Prefer exhaustive list coverage plus selective detail fetches on first_seen, "
            "short_changed, and a TTL refresh. Exhaustive detail for every observed vacancy "
            "does not look justified."
        )
        rationale = [
            "Detail adds fields that search does not reliably provide.",
            (
                "Repeated normalized detail changes are infrequent in the sampled "
                "pairwise refetches."
            ),
            (
                "The selective policy keeps broad market coverage while controlling "
                "API and storage cost."
            ),
        ]
    elif normalized_changed_pair_rate <= 0.5:
        stability_summary = (
            "Repeated detail fetches show moderate normalized drift, so detail remains useful but "
            "does not yet justify always-on exhaustive refetches."
        )
        recommended_policy = (
            "Prefer exhaustive list coverage plus selective detail with a shorter TTL refresh "
            "window for active vacancies."
        )
        rationale = [
            "Detail still adds fields unavailable in search.",
            "Some normalized fields change across refetches, so periodic refresh is warranted.",
            "A selective TTL policy is a better trade-off than fetching detail on every sighting.",
        ]
    else:
        stability_summary = (
            "Repeated detail fetches show material normalized drift in the sample."
        )
        recommended_policy = (
            "Detail appears unstable enough that a more aggressive detail refresh policy is "
            "warranted. If full exhaustive detail is too expensive, use exhaustive list "
            "coverage plus a very short TTL detail policy for active vacancies."
        )
        rationale = [
            "Detail adds unique fields beyond search.",
            "Normalized detail changes are frequent enough to challenge a sparse refresh policy.",
            "A near-exhaustive or aggressive TTL strategy is safer for research fidelity.",
        ]

    detail_value_summary = (
        "Detail-only research fields observed: " + ", ".join(detail_only_research_fields)
    )
    return {
        "detail_value_summary": detail_value_summary,
        "stability_summary": stability_summary,
        "recommended_policy": recommended_policy,
        "rationale": rationale,
    }


def _write_report_files(report_directory: Path, report_payload: dict[str, Any]) -> None:
    report_json_path = report_directory / "report.json"
    summary_markdown_path = report_directory / "summary.md"
    report_json_path.write_text(
        json.dumps(report_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(
        _build_markdown_summary(report_payload),
        encoding="utf-8",
    )


def _build_markdown_summary(report_payload: dict[str, Any]) -> str:
    summary = report_payload["summary"]
    conclusion = summary["conclusion"]
    aggregate_paths = report_payload["aggregates"]
    lines = [
        "# Detail Payload Study",
        "",
        f"- generated_at: {report_payload['generated_at']}",
        f"- crawl_run_id: {report_payload['crawl_run_id']}",
        (
            "- sample: "
            f"{summary['vacancies_with_successful_detail']}/"
            f"{report_payload['input']['sample_size_requested']} vacancies with successful detail"
        ),
        (
            "- repeated raw changes: "
            f"{summary['raw_changed_pairs']}/{summary['raw_comparable_pairs']} comparable pairs "
            f"({summary['raw_changed_pair_rate']:.1%})"
        ),
        (
            "- repeated normalized changes: "
            f"{summary['normalized_changed_pairs']}/"
            f"{summary['normalized_comparable_pairs']} comparable pairs "
            f"({summary['normalized_changed_pair_rate']:.1%})"
        ),
        "",
        "## Detail vs Search",
        "",
    ]
    lines.extend(_markdown_counter_block(
        "Detail-only research fields",
        aggregate_paths["detail_only_research_field_counts"],
    ))
    lines.extend(_markdown_counter_block(
        "Detail non-null while search is missing/null",
        aggregate_paths["detail_non_null_search_missing_or_null_counts"],
    ))
    lines.extend(
        [
            "",
            "## Repeated Detail Stability",
            "",
        ]
    )
    lines.extend(_markdown_counter_block(
        "Top raw changed paths",
        aggregate_paths["repeated_raw_changed_path_counts"],
    ))
    lines.extend(_markdown_counter_block(
        "Top normalized changed paths",
        aggregate_paths["repeated_normalized_changed_path_counts"],
    ))
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            f"- detail_value_summary: {conclusion['detail_value_summary']}",
            f"- stability_summary: {conclusion['stability_summary']}",
            f"- recommended_policy: {conclusion['recommended_policy']}",
        ]
    )
    for item in conclusion["rationale"]:
        lines.append(f"- rationale: {item}")
    lines.append("")
    return "\n".join(lines)


def _markdown_counter_block(title: str, rows: list[dict[str, Any]]) -> list[str]:
    lines = [f"### {title}", ""]
    if not rows:
        lines.append("- none observed in this sample")
        lines.append("")
        return lines

    for row in rows[:TOP_PATH_LIMIT]:
        lines.append(f"- {row['path']}: {row['count']}")
    lines.append("")
    return lines


def _counter_rows(counter: Counter[str]) -> list[dict[str, Any]]:
    return [
        {"path": path, "count": count}
        for path, count in counter.most_common(TOP_PATH_LIMIT)
    ]


def _changed_paths(
    previous_values: dict[str, tuple[str, ...]],
    current_values: dict[str, tuple[str, ...]],
) -> list[str]:
    changed_paths = []
    for path in sorted(set(previous_values) | set(current_values)):
        if previous_values.get(path) != current_values.get(path):
            changed_paths.append(path)
    return changed_paths


def _collect_leaf_values(payload_json: object) -> dict[str, tuple[str, ...]]:
    values_by_path: dict[str, set[str]] = defaultdict(set)
    _collect_leaf_values_into(payload_json, prefix="", values_by_path=values_by_path)
    return {
        path: tuple(sorted(values))
        for path, values in sorted(values_by_path.items())
        if path
    }


def _collect_leaf_values_into(
    payload_json: object,
    *,
    prefix: str,
    values_by_path: dict[str, set[str]],
) -> None:
    if isinstance(payload_json, dict):
        for key, value in payload_json.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            _collect_leaf_values_into(value, prefix=next_prefix, values_by_path=values_by_path)
        return

    if isinstance(payload_json, list):
        next_prefix = f"{prefix}[]" if prefix else "[]"
        for item in payload_json:
            _collect_leaf_values_into(item, prefix=next_prefix, values_by_path=values_by_path)
        return

    values_by_path[prefix].add(json.dumps(payload_json, ensure_ascii=False, sort_keys=True))


def _values_are_effectively_null(values: tuple[str, ...]) -> bool:
    return all(value == "null" for value in values)


def _is_research_important_detail_path(path: str) -> bool:
    return any(
        path == prefix or path.startswith(prefix)
        for prefix in IMPORTANT_DETAIL_PATH_PREFIXES
    )


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator

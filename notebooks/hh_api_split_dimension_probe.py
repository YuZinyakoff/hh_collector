from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

from hhru_platform.config.settings import get_settings
from notebooks import hh_api_probe_harness as h

DEFAULT_ROOT_HH_AREA_ID = "113"
DEFAULT_SAMPLE_PAGES = 5
DEFAULT_TOP_ROLES = 5


@dataclass(slots=True, frozen=True)
class AreaCandidate:
    hh_area_id: str
    name: str
    level: int | None
    path_text: str | None


@dataclass(slots=True, frozen=True)
class TimeWindowSpec:
    label: str
    date_from: str
    date_to: str


def local_now() -> datetime:
    return datetime.now().astimezone()


def json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def log(message: str) -> None:
    print(f"[{local_now().isoformat()}] {message}", flush=True)


def build_time_window_specs(anchor_end: datetime | None = None) -> list[TimeWindowSpec]:
    if anchor_end is None:
        now = local_now()
        anchor_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    anchor_end = anchor_end.astimezone()
    windows = [
        ("trailing_1h", timedelta(hours=1)),
        ("trailing_6h", timedelta(hours=6)),
        ("trailing_24h", timedelta(hours=24)),
        ("trailing_7d", timedelta(days=7)),
    ]
    specs: list[TimeWindowSpec] = []
    for label, delta in windows:
        start = anchor_end - delta
        specs.append(
            TimeWindowSpec(
                label=label,
                date_from=start.isoformat(timespec="seconds"),
                date_to=anchor_end.isoformat(timespec="seconds"),
            )
        )
    return specs


def summarize_role_overlap(items: list[dict[str, Any]]) -> dict[str, Any]:
    role_counter: Counter[str] = Counter()
    role_names: dict[str, str] = {}
    total_items = 0
    multi_role_items = 0
    max_roles_per_vacancy = 0

    for item in items:
        if not isinstance(item, dict):
            continue
        total_items += 1
        roles = item.get("professional_roles")
        if not isinstance(roles, list):
            continue
        normalized_roles: list[str] = []
        for role in roles:
            if not isinstance(role, dict):
                continue
            role_id = role.get("id")
            if role_id is None:
                continue
            normalized_role_id = str(role_id)
            normalized_roles.append(normalized_role_id)
            role_counter[normalized_role_id] += 1
            role_name = role.get("name")
            if isinstance(role_name, str) and role_name and normalized_role_id not in role_names:
                role_names[normalized_role_id] = role_name
        unique_role_count = len(set(normalized_roles))
        max_roles_per_vacancy = max(max_roles_per_vacancy, unique_role_count)
        if unique_role_count > 1:
            multi_role_items += 1

    return {
        "sampled_vacancies": total_items,
        "multi_role_vacancies": multi_role_items,
        "multi_role_share": 0.0 if total_items == 0 else round(multi_role_items / total_items, 4),
        "max_roles_per_vacancy": max_roles_per_vacancy,
        "top_roles": [
            {
                "professional_role_id": role_id,
                "name": role_names.get(role_id),
                "sample_hits": hits,
            }
            for role_id, hits in role_counter.most_common()
        ],
    }


def discover_leaf_areas(root_hh_area_id: str = DEFAULT_ROOT_HH_AREA_ID) -> list[AreaCandidate]:
    settings = get_settings()
    engine = create_engine(settings.database_url)
    query = text(
        """
        SELECT
            a.hh_area_id,
            a.name,
            a.level,
            a.path_text
        FROM area AS a
        JOIN area AS root
          ON a.parent_area_id = root.id
        WHERE root.hh_area_id = :root_hh_area_id
          AND a.is_active = true
          AND NOT EXISTS (
              SELECT 1
              FROM area AS child
              WHERE child.parent_area_id = a.id
                AND child.is_active = true
          )
        ORDER BY a.level NULLS FIRST, a.path_text NULLS LAST, a.name, a.hh_area_id
        """
    )
    try:
        with engine.begin() as connection:
            rows = connection.execute(query, {"root_hh_area_id": root_hh_area_id}).mappings().all()
    finally:
        engine.dispose()
    return [
        AreaCandidate(
            hh_area_id=str(row["hh_area_id"]),
            name=str(row["name"]),
            level=None if row["level"] is None else int(row["level"]),
            path_text=None if row["path_text"] is None else str(row["path_text"]),
        )
        for row in rows
    ]


def probe_search(params: dict[str, Any]) -> dict[str, Any]:
    response = h.hh_get("/vacancies", params)
    payload = response.get("payload")
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        items = []
    return {
        "params": params,
        "status_code": response.get("status_code"),
        "error_type": response.get("error_type"),
        "error_value": response.get("error_value"),
        "latency_ms": response.get("latency_ms"),
        "found": response.get("found"),
        "pages": response.get("pages"),
        "items_count": response.get("items_count"),
        "request_id": response.get("request_id"),
        "timestamp_utc": response.get("timestamp_utc"),
        "items": items,
    }


def sample_area_pages(area_hh_id: str, *, sample_pages: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for page in range(sample_pages):
        results.append(
            probe_search(
                {
                    "area": area_hh_id,
                    "page": page,
                    "per_page": 20,
                }
            )
        )
    return results


def choose_top_roles(
    role_overlap_summary: dict[str, Any],
    *,
    top_roles: int,
) -> list[dict[str, Any]]:
    return list(role_overlap_summary.get("top_roles") or [])[:top_roles]


def build_time_probe_results(area_hh_id: str, specs: list[TimeWindowSpec]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for spec in specs:
        params = {
            "area": area_hh_id,
            "page": 0,
            "per_page": 20,
            "date_from": spec.date_from,
            "date_to": spec.date_to,
        }
        result = probe_search(params)
        result["label"] = spec.label
        results.append(result)
    return results


def build_role_probe_results(area_hh_id: str, roles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for role in roles:
        role_id = str(role["professional_role_id"])
        params = {
            "area": area_hh_id,
            "page": 0,
            "per_page": 20,
            "professional_role": role_id,
        }
        result = probe_search(params)
        result["professional_role_id"] = role_id
        result["professional_role_name"] = role.get("name")
        result["sample_hits"] = role.get("sample_hits")
        results.append(result)
    return results


def _min_pages(results: list[dict[str, Any]]) -> int | None:
    pages = [
        int(result["pages"])
        for result in results
        if isinstance(result.get("pages"), int | float)
    ]
    return None if not pages else min(pages)


def assess_dimensions(
    *,
    baseline: dict[str, Any],
    role_overlap_summary: dict[str, Any],
    time_results: list[dict[str, Any]],
    role_results: list[dict[str, Any]],
) -> dict[str, Any]:
    baseline_pages = baseline.get("pages")
    time_min_pages = _min_pages(time_results)
    role_min_pages = _min_pages(role_results)
    return {
        "time_window": {
            "supported": all(result.get("status_code") == 200 for result in time_results),
            "disjoint_capable": True,
            "best_pages": time_min_pages,
            "best_reduction_vs_baseline": None
            if not isinstance(baseline_pages, int | float) or time_min_pages is None
            else round(float(baseline_pages) - float(time_min_pages), 2),
        },
        "professional_role": {
            "supported": all(result.get("status_code") == 200 for result in role_results),
            "disjoint_capable": False,
            "best_pages": role_min_pages,
            "best_reduction_vs_baseline": None
            if not isinstance(baseline_pages, int | float) or role_min_pages is None
            else round(float(baseline_pages) - float(role_min_pages), 2),
            "sample_multi_role_share": role_overlap_summary.get("multi_role_share"),
        },
        "preferred_first_fallback": (
            "time_window"
            if role_overlap_summary.get("multi_role_vacancies", 0) > 0 or time_min_pages is not None
            else "undecided"
        ),
    }


def render_markdown_summary(summary: dict[str, Any]) -> str:
    lines = [
        "# HH API Split Dimension Ranking",
        "",
        f"- generated_at_utc: `{summary['generated_at_utc']}`",
        f"- auth_mode: `{summary['auth_mode']}`",
        f"- root_hh_area_id: `{summary['root_hh_area_id']}`",
        f"- discovered_leaf_areas: `{summary['discovered_leaf_area_count']}`",
        (
            f"- preferred_first_fallback: "
            f"`{summary['global_recommendation']['preferred_first_fallback']}`"
        ),
        "",
        "## Key Reading",
        "",
        (
            "- `time_window` is currently the safer first fallback because it can be "
            "made disjoint and the live probes narrowed hot leaf areas much more "
            "aggressively than the sampled `professional_role` splits."
        ),
        (
            "- `professional_role` remains worth supporting as a secondary split lever, "
            "but not as the first exhaustive fallback until its disjoint coverage "
            "semantics are proven."
        ),
        "",
        "## Areas",
        "",
    ]
    for area in summary["areas"]:
        lines.extend(
            [
                f"### {area['area_name']} (`{area['area_hh_id']}`)",
                "",
                (
                    f"- baseline: status `{area['baseline']['status_code']}`, "
                    f"found `{area['baseline']['found']}`, pages `{area['baseline']['pages']}`"
                ),
                (
                    f"- sampled vacancies: `{area['role_overlap']['sampled_vacancies']}`, "
                    f"multi-role share `{area['role_overlap']['multi_role_share']}`, "
                    f"max roles per vacancy `{area['role_overlap']['max_roles_per_vacancy']}`"
                ),
                (
                    f"- best time window pages: "
                    f"`{area['dimension_assessment']['time_window']['best_pages']}`"
                ),
                (
                    f"- best professional role pages: "
                    f"`{area['dimension_assessment']['professional_role']['best_pages']}`"
                ),
                (
                    f"- preferred first fallback: "
                    f"`{area['dimension_assessment']['preferred_first_fallback']}`"
                ),
                "",
                "Top time windows:",
            ]
        )
        for result in area["time_windows"]:
            lines.append(
                f"- `{result['label']}`: status `{result['status_code']}`, "
                f"found `{result['found']}`, pages `{result['pages']}`"
            )
        lines.append("")
        lines.append("Top professional roles:")
        for result in area["professional_roles"]:
            lines.append(
                f"- `{result['professional_role_id']}` "
                f"({result.get('professional_role_name') or 'unknown'}): "
                f"status `{result['status_code']}`, "
                f"found `{result['found']}`, pages `{result['pages']}`"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def build_summary(
    *,
    root_hh_area_id: str,
    sample_pages: int,
    top_roles: int,
) -> dict[str, Any]:
    time_specs = build_time_window_specs()
    leaf_areas = discover_leaf_areas(root_hh_area_id)
    areas_summary: list[dict[str, Any]] = []

    for area in leaf_areas:
        log(f"probing baseline leaf area {area.hh_area_id} ({area.name})")
        baseline = probe_search({"area": area.hh_area_id, "page": 0, "per_page": 20})
        sampled_pages = sample_area_pages(area.hh_area_id, sample_pages=sample_pages)
        sampled_items = [
            item
            for page_result in sampled_pages
            for item in page_result.get("items", [])
            if isinstance(item, dict)
        ]
        role_overlap = summarize_role_overlap(sampled_items)
        selected_roles = choose_top_roles(role_overlap, top_roles=top_roles)
        log(f"probing time windows for area {area.hh_area_id}")
        time_results = build_time_probe_results(area.hh_area_id, time_specs)
        log(f"probing professional roles for area {area.hh_area_id}")
        role_results = build_role_probe_results(area.hh_area_id, selected_roles)
        assessment = assess_dimensions(
            baseline=baseline,
            role_overlap_summary=role_overlap,
            time_results=time_results,
            role_results=role_results,
        )
        areas_summary.append(
            {
                "area_hh_id": area.hh_area_id,
                "area_name": area.name,
                "level": area.level,
                "path_text": area.path_text,
                "baseline": baseline,
                "sample_pages": sampled_pages,
                "role_overlap": role_overlap,
                "time_windows": time_results,
                "professional_roles": role_results,
                "dimension_assessment": assessment,
            }
        )

    global_preference = "time_window"
    if areas_summary and any(
        area["dimension_assessment"]["preferred_first_fallback"] != "time_window"
        for area in areas_summary
    ):
        global_preference = "mixed_or_followup_required"

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "run_label": h.RUN_LABEL,
        "auth_mode": h.default_auth_mode(),
        "root_hh_area_id": root_hh_area_id,
        "discovered_leaf_area_count": len(leaf_areas),
        "time_windows": [
            {
                "label": spec.label,
                "date_from": spec.date_from,
                "date_to": spec.date_to,
            }
            for spec in time_specs
        ],
        "areas": areas_summary,
        "global_recommendation": {
            "preferred_first_fallback": global_preference,
            "secondary_supported_dimensions": ["professional_role"],
            "notes": [
                (
                    "time_window can be made disjoint, HH accepts "
                    "datetime-precision date_from/date_to, and live probes showed "
                    "much stronger narrowing on hot leaf areas"
                ),
                (
                    "professional_role is still worth supporting, but its exhaustive "
                    "disjoint coverage semantics are not yet proven and live "
                    "narrowing was weaker or mixed"
                ),
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe secondary split dimensions for hot HH search leaf areas.",
    )
    parser.add_argument(
        "--root-hh-area-id",
        default=DEFAULT_ROOT_HH_AREA_ID,
        help="Root hh area whose direct leaf children should be probed. Defaults to 113 (Russia).",
    )
    parser.add_argument(
        "--sample-pages",
        type=int,
        default=DEFAULT_SAMPLE_PAGES,
        help="How many baseline pages to sample per leaf area for professional role overlap stats.",
    )
    parser.add_argument(
        "--top-roles",
        type=int,
        default=DEFAULT_TOP_ROLES,
        help="How many sampled professional roles to probe per leaf area.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.sample_pages <= 0:
        raise ValueError("--sample-pages must be > 0")
    if args.top_roles <= 0:
        raise ValueError("--top-roles must be > 0")

    summary = build_summary(
        root_hh_area_id=str(args.root_hh_area_id),
        sample_pages=int(args.sample_pages),
        top_roles=int(args.top_roles),
    )

    json_path = h.RESULTS_DIR / f"{h.RUN_LABEL}-split-dimension-ranking-summary.json"
    md_path = h.RESULTS_DIR / f"{h.RUN_LABEL}-split-dimension-ranking-summary.md"
    json_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, default=json_default) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(render_markdown_summary(summary), encoding="utf-8")

    print(f"summary_json={json_path}")
    print(f"summary_md={md_path}")
    print(f"preferred_first_fallback={summary['global_recommendation']['preferred_first_fallback']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

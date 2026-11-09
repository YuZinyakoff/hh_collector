from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from time import monotonic
from typing import Any

from sqlalchemy import Engine, text


class SearchAuditReader:
    """Short, read-only transactions. Never hold a snapshot for the whole audit."""

    def __init__(
        self, engine: Engine, *, timeout_ms: int = 30_000, deadline: float | None = None
    ) -> None:
        if timeout_ms <= 0:
            raise ValueError("timeout_ms must be positive")
        self.engine = engine
        self.timeout_ms = timeout_ms
        self.deadline = deadline

    def query(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        if self.deadline is not None and monotonic() >= self.deadline:
            raise TimeoutError("audit time budget exhausted")
        with self.engine.connect() as connection, connection.begin():
            connection.execute(text("SET TRANSACTION READ ONLY"))
            connection.execute(text(f"SET LOCAL statement_timeout = {self.timeout_ms}"))
            connection.execute(text("SET LOCAL lock_timeout = '2s'"))
            return [dict(row) for row in connection.execute(text(sql), params or {}).mappings()]

    def runs(self, *, limit: int, run_ids: Sequence[str]) -> list[dict[str, Any]]:
        if run_ids:
            return self.query(
                "SELECT id, run_type, status, started_at, finished_at FROM crawl_run "
                "WHERE id = ANY(CAST(:ids AS uuid[])) ORDER BY started_at DESC",
                {"ids": list(run_ids)},
            )
        return self.query(
            "SELECT id, run_type, status, started_at, finished_at FROM crawl_run "
            "WHERE run_type IN ('production_weekly_sweep', 'weekly_sweep') "
            "ORDER BY started_at DESC LIMIT :limit",
            {"limit": limit},
        )

    def areas(self) -> list[dict[str, Any]]:
        return self.query(
            "SELECT a.hh_area_id, a.name, p.hh_area_id AS parent_hh_area_id "
            "FROM area a LEFT JOIN area p ON p.id = a.parent_area_id ORDER BY a.hh_area_id"
        )

    def partitions(self, run_id: str, *, limit: int) -> list[dict[str, Any]]:
        return self.query(
            "SELECT id, crawl_run_id, status, coverage_status, is_terminal, params_json, "
            "pages_processed, pages_total_expected "
            "FROM crawl_partition WHERE crawl_run_id = CAST(:id AS uuid) "
            "ORDER BY id LIMIT :limit",
            {"id": run_id, "limit": limit},
        )

    def responses(self, partition_id: str, *, max_bytes: int) -> list[dict[str, Any]]:
        # Both ends of each partition, without scanning global payload JSON columns.
        return self.query(
            """
            WITH selected AS (
              (SELECT id, crawl_run_id, crawl_partition_id, requested_at,
                      params_json, status_code, error_type FROM api_request_log
               WHERE crawl_partition_id = CAST(:id AS uuid) AND request_type = 'vacancy_search'
               ORDER BY id LIMIT 1)
              UNION
              (SELECT id, crawl_run_id, crawl_partition_id, requested_at,
                      params_json, status_code, error_type FROM api_request_log
               WHERE crawl_partition_id = CAST(:id AS uuid) AND request_type = 'vacancy_search'
               ORDER BY id DESC LIMIT 1)
            )
            SELECT q.id AS api_request_log_id, q.crawl_run_id, q.crawl_partition_id,
                   q.requested_at, q.params_json, q.status_code, q.error_type,
                   r.id AS raw_api_payload_id, r.received_at, r.payload_hash,
                   CASE WHEN octet_length(r.payload_json::text) <= :max_bytes
                        THEN r.payload_json END AS payload_json,
                   octet_length(r.payload_json::text) > :max_bytes AS payload_omitted
            FROM selected q LEFT JOIN raw_api_payload r ON r.api_request_log_id = q.id
            ORDER BY q.id, r.id
            """,
            {"id": partition_id, "max_bytes": max_bytes},
        )

    def request_inventory(self, run_id: str) -> dict[str, Any]:
        rows = self.query(
            """
            WITH scoped AS (
                SELECT q.id, q.status_code, q.crawl_run_id, p.crawl_run_id AS partition_run_id
                FROM api_request_log q LEFT JOIN crawl_partition p ON p.id = q.crawl_partition_id
                WHERE q.crawl_run_id = CAST(:id AS uuid) AND q.request_type = 'vacancy_search'
                UNION
                SELECT q.id, q.status_code, q.crawl_run_id, p.crawl_run_id AS partition_run_id
                FROM crawl_partition p JOIN api_request_log q ON q.crawl_partition_id = p.id
                WHERE p.crawl_run_id = CAST(:id AS uuid)
                  AND q.crawl_run_id IS DISTINCT FROM CAST(:id AS uuid)
                  AND q.request_type = 'vacancy_search'
            ), inventory AS (
                SELECT q.id, q.status_code, q.crawl_run_id, q.partition_run_id,
                       count(r.id) AS raw_count
                FROM scoped q LEFT JOIN raw_api_payload r ON r.api_request_log_id = q.id
                GROUP BY q.id, q.status_code, q.crawl_run_id, q.partition_run_id
            )
            SELECT count(*) AS requests, coalesce(max(id), 0) AS max_request_id,
                   count(*) FILTER (WHERE status_code = 200) AS http_200_requests,
                   count(*) FILTER (WHERE status_code = 200 AND raw_count = 0) AS missing_200_raw,
                   coalesce(sum(raw_count), 0)::bigint AS raw_rows,
                   count(*) FILTER (WHERE raw_count > 1) AS multiple_raw_requests,
                   count(*) FILTER (WHERE crawl_run_id IS DISTINCT FROM CAST(:id AS uuid))
                       AS request_run_mismatches,
                   count(*) FILTER (WHERE partition_run_id IS DISTINCT FROM CAST(:id AS uuid))
                       AS partition_run_mismatches
            FROM inventory
            """,
            {"id": run_id},
        )
        return rows[0]

    def all_responses(
        self, partition_id: str, *, max_bytes: int, through_request_id: int, batch_size: int
    ) -> Iterator[dict[str, Any]]:
        if not 1 <= batch_size <= 50:
            raise ValueError("response batch size must be between 1 and 50")
        after_id = 0
        while after_id < through_request_id:
            rows = self.query(
                """
                WITH selected AS (
                    SELECT id, crawl_run_id, crawl_partition_id, requested_at,
                           params_json, status_code, error_type
                    FROM api_request_log
                    WHERE crawl_partition_id = CAST(:id AS uuid)
                      AND request_type = 'vacancy_search'
                      AND id > :after_id AND id <= :through_id
                    ORDER BY id LIMIT :batch_size
                )
                SELECT q.id AS api_request_log_id, q.crawl_run_id, q.crawl_partition_id,
                       q.requested_at, q.params_json, q.status_code, q.error_type,
                       r.id AS raw_api_payload_id, r.received_at, r.payload_hash,
                       CASE WHEN octet_length(r.payload_json::text) <= :max_bytes
                            THEN r.payload_json END AS payload_json,
                       octet_length(r.payload_json::text) > :max_bytes AS payload_omitted
                FROM selected q LEFT JOIN raw_api_payload r ON r.api_request_log_id = q.id
                ORDER BY q.id, r.id
                """,
                {
                    "id": partition_id,
                    "after_id": after_id,
                    "through_id": through_request_id,
                    "batch_size": batch_size,
                    "max_bytes": max_bytes,
                },
            )
            if not rows:
                return
            next_id = max(int(row["api_request_log_id"]) for row in rows)
            if next_id <= after_id:
                raise ValueError("request cursor did not advance")
            yield from rows
            after_id = next_id
            if len(rows) < batch_size:
                return

    def seen_inventory(self, run_id: str, *, max_groups: int) -> list[dict[str, Any]]:
        return self.query(
            """
            SELECT short_payload_ref_id, crawl_partition_id, count(*) AS seen_rows
            FROM vacancy_seen_event WHERE crawl_run_id = CAST(:id AS uuid)
            GROUP BY short_payload_ref_id, crawl_partition_id
            ORDER BY short_payload_ref_id NULLS LAST, crawl_partition_id
            LIMIT :limit
            """,
            {"id": run_id, "limit": max_groups + 1},
        )

    def derivatives(self, raw_id: int) -> dict[str, list[dict[str, Any]]]:
        return {
            "seen": self.query(
                "SELECT s.short_hash, s.crawl_run_id, s.crawl_partition_id, "
                "v.hh_vacancy_id FROM vacancy_seen_event s "
                "JOIN vacancy v ON v.id = s.vacancy_id "
                "WHERE s.short_payload_ref_id = :id ORDER BY s.id LIMIT 1001",
                {"id": raw_id},
            ),
            "snapshots": self.query(
                "SELECT s.short_hash, v.hh_vacancy_id, s.normalized_json->'payload' AS payload "
                "FROM vacancy_snapshot s JOIN vacancy v ON v.id = s.vacancy_id "
                "WHERE s.short_payload_ref_id = :id AND s.snapshot_type = 'short' "
                "ORDER BY s.id LIMIT 1001",
                {"id": raw_id},
            ),
        }

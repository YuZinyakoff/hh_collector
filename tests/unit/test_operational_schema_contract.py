from __future__ import annotations

import re
from pathlib import Path

from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from hhru_platform.infrastructure.db import models  # noqa: F401
from hhru_platform.infrastructure.db.base import Base

EXPECTED_TABLES = {
    "api_request_log",
    "area",
    "crawl_partition",
    "crawl_run",
    "detail_fetch_attempt",
    "dictionary_sync_run",
    "employer",
    "professional_role",
    "raw_api_payload",
    "vacancy",
    "vacancy_current_state",
    "vacancy_professional_role",
    "vacancy_seen_event",
    "vacancy_snapshot",
}


def _server_default_sql(table_name: str, column_name: str) -> str:
    default = Base.metadata.tables[table_name].c[column_name].server_default
    assert default is not None
    return str(default.arg.compile(dialect=postgresql.dialect()))


def _foreign_key_ondelete(table_name: str, column_name: str) -> str | None:
    foreign_keys = list(Base.metadata.tables[table_name].c[column_name].foreign_keys)
    assert len(foreign_keys) == 1
    return foreign_keys[0].ondelete


def _index_sql(table_name: str, index_name: str) -> str:
    table = Base.metadata.tables[table_name]
    index = next(index for index in table.indexes if index.name == index_name)
    return str(CreateIndex(index).compile(dialect=postgresql.dialect()))


def _unique_constraint_names(table_name: str) -> set[str]:
    table = Base.metadata.tables[table_name]
    return {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint) and constraint.name is not None
    }


def _metadata_index_names() -> set[str]:
    return {
        index.name
        for table in Base.metadata.tables.values()
        for index in table.indexes
        if index.name is not None
    }


def test_operational_metadata_table_set_matches_mvp_contract() -> None:
    assert set(Base.metadata.tables) == EXPECTED_TABLES


def test_operational_metadata_constraints_defaults_and_indexes_match_contract() -> None:
    assert _server_default_sql("crawl_run", "id") == "gen_random_uuid()"
    assert _server_default_sql("crawl_run", "triggered_by") == "'system'"
    assert _server_default_sql("crawl_run", "config_snapshot_json") == "'{}'::jsonb"
    assert "DESC" in _index_sql("crawl_run", "idx_crawl_run_started_at")

    assert "uq_crawl_partition_run_key" in _unique_constraint_names("crawl_partition")
    assert _foreign_key_ondelete("crawl_partition", "crawl_run_id") == "CASCADE"
    assert _server_default_sql("crawl_partition", "params_json") == "'{}'::jsonb"
    assert "created_at" in Base.metadata.tables["crawl_partition"].c

    assert _foreign_key_ondelete("api_request_log", "crawl_run_id") == "SET NULL"
    assert _foreign_key_ondelete("api_request_log", "crawl_partition_id") == "SET NULL"
    assert _server_default_sql("api_request_log", "method") == "'GET'"
    assert _server_default_sql("api_request_log", "params_json") == "'{}'::jsonb"
    assert "DESC" in _index_sql("api_request_log", "idx_api_request_log_requested_at")

    assert "uq_area_hh_area_id" in _unique_constraint_names("area")
    assert "idx_area_parent_area_id" in _metadata_index_names()
    assert _foreign_key_ondelete("area", "parent_area_id") == "SET NULL"

    assert "uq_professional_role_hh_professional_role_id" in _unique_constraint_names(
        "professional_role"
    )

    assert "uq_employer_hh_employer_id" in _unique_constraint_names("employer")
    assert "idx_employer_area_id" in _metadata_index_names()
    assert _foreign_key_ondelete("employer", "area_id") == "SET NULL"

    assert "uq_vacancy_hh_vacancy_id" in _unique_constraint_names("vacancy")
    assert _foreign_key_ondelete("vacancy", "employer_id") == "SET NULL"
    assert _foreign_key_ondelete("vacancy", "area_id") == "SET NULL"
    assert _server_default_sql("vacancy", "source_type") == "'hh_api'"
    assert "DESC" in _index_sql("vacancy", "idx_vacancy_published_at")

    assert list(Base.metadata.tables["vacancy_professional_role"].primary_key.columns.keys()) == [
        "vacancy_id",
        "professional_role_id",
    ]
    assert _foreign_key_ondelete("vacancy_professional_role", "vacancy_id") == "CASCADE"
    assert _foreign_key_ondelete("vacancy_professional_role", "professional_role_id") == "CASCADE"

    assert "uq_vse_seen" in _unique_constraint_names("vacancy_seen_event")
    assert _foreign_key_ondelete("vacancy_seen_event", "crawl_partition_id") == "CASCADE"
    assert _foreign_key_ondelete("vacancy_seen_event", "short_payload_ref_id") == "SET NULL"
    assert "idx_vacancy_seen_event_partition_id" in _metadata_index_names()
    assert "DESC" in _index_sql("vacancy_seen_event", "idx_vacancy_seen_event_seen_at")

    assert _server_default_sql("vacancy_current_state", "seen_count") == "1"
    assert _server_default_sql("vacancy_current_state", "detail_fetch_status") == "'not_requested'"
    assert _foreign_key_ondelete("vacancy_current_state", "vacancy_id") == "CASCADE"
    assert _foreign_key_ondelete("vacancy_current_state", "last_seen_run_id") == "SET NULL"
    assert "DESC" in _index_sql("vacancy_current_state", "idx_vacancy_current_state_last_seen_at")

    assert _server_default_sql("dictionary_sync_run", "started_at") == "now()"
    assert "DESC" in _index_sql("dictionary_sync_run", "idx_dictionary_sync_run_started_at")

    assert _server_default_sql("vacancy_snapshot", "captured_at") == "now()"
    assert _foreign_key_ondelete("vacancy_snapshot", "vacancy_id") == "CASCADE"
    assert _foreign_key_ondelete("vacancy_snapshot", "detail_payload_ref_id") == "SET NULL"
    assert "DESC" in _index_sql("vacancy_snapshot", "idx_vacancy_snapshot_captured_at")

    assert _server_default_sql("detail_fetch_attempt", "attempt") == "1"
    assert _server_default_sql("detail_fetch_attempt", "requested_at") == "now()"
    assert _foreign_key_ondelete("detail_fetch_attempt", "crawl_run_id") == "SET NULL"
    assert "DESC" in _index_sql("detail_fetch_attempt", "idx_detail_fetch_attempt_requested_at")


def test_schema_sources_keep_same_table_and_index_sets() -> None:
    schema_sql = Path("schema.sql").read_text(encoding="utf-8")
    migration_sql = Path("migrations/versions/0001_initial_schema.py").read_text(encoding="utf-8")

    schema_tables = set(re.findall(r"^CREATE TABLE ([a-z_]+) \(", schema_sql, re.MULTILINE))
    migration_tables = set(re.findall(r'op\.create_table\(\s*"([a-z_]+)"', migration_sql))

    schema_indexes = set(re.findall(r"^CREATE INDEX ([a-z0-9_]+)", schema_sql, re.MULTILINE))
    migration_indexes = set(re.findall(r'op\.create_index\(\s*"([a-z0-9_]+)"', migration_sql))

    metadata_indexes = _metadata_index_names()

    assert schema_tables == EXPECTED_TABLES
    assert migration_tables == EXPECTED_TABLES
    assert schema_indexes == metadata_indexes
    assert migration_indexes == metadata_indexes

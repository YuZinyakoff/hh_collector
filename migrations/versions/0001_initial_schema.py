"""Initial operational schema."""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "crawl_run",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("run_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("triggered_by", sa.Text(), nullable=False),
        sa.Column("config_snapshot_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("partitions_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("partitions_done", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("partitions_failed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("notes", sa.Text(), nullable=True),
    )
    op.create_index("idx_crawl_run_started_at", "crawl_run", ["started_at"])
    op.create_index("idx_crawl_run_status", "crawl_run", ["status"])

    op.create_table(
        "area",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("hh_area_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("parent_area_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("area.id"), nullable=True),
        sa.Column("level", sa.Integer(), nullable=True),
        sa.Column("path_text", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("hh_area_id", name="uq_area_hh_area_id"),
    )

    op.create_table(
        "employer",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("hh_employer_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("alternate_url", sa.Text(), nullable=True),
        sa.Column("site_url", sa.Text(), nullable=True),
        sa.Column("area_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("area.id"), nullable=True),
        sa.Column("is_trusted", sa.Boolean(), nullable=True),
        sa.Column("raw_first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("hh_employer_id", name="uq_employer_hh_employer_id"),
    )
    op.create_index("idx_employer_name", "employer", ["name"])

    op.create_table(
        "professional_role",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("hh_professional_role_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("category_name", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("hh_professional_role_id", name="uq_professional_role_hh_professional_role_id"),
    )

    op.create_table(
        "crawl_partition",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("crawl_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("crawl_run.id"), nullable=False),
        sa.Column("partition_key", sa.Text(), nullable=False),
        sa.Column("params_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("pages_total_expected", sa.Integer(), nullable=True),
        sa.Column("pages_processed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("items_seen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.UniqueConstraint("crawl_run_id", "partition_key", name="uq_crawl_partition_run_key"),
    )
    op.create_index("idx_crawl_partition_run_id", "crawl_partition", ["crawl_run_id"])
    op.create_index("idx_crawl_partition_status", "crawl_partition", ["status"])

    op.create_table(
        "api_request_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("crawl_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("crawl_run.id"), nullable=True),
        sa.Column(
            "crawl_partition_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("crawl_partition.id"),
            nullable=True,
        ),
        sa.Column("request_type", sa.Text(), nullable=False),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("method", sa.Text(), nullable=False),
        sa.Column("params_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("request_headers_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("response_received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index("idx_api_request_log_partition_id", "api_request_log", ["crawl_partition_id"])
    op.create_index("idx_api_request_log_requested_at", "api_request_log", ["requested_at"])
    op.create_index("idx_api_request_log_run_id", "api_request_log", ["crawl_run_id"])
    op.create_index("idx_api_request_log_status_code", "api_request_log", ["status_code"])

    op.create_table(
        "raw_api_payload",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("api_request_log_id", sa.BigInteger(), sa.ForeignKey("api_request_log.id"), nullable=False),
        sa.Column("endpoint_type", sa.Text(), nullable=False),
        sa.Column("entity_hh_id", sa.Text(), nullable=True),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("payload_hash", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_raw_api_payload_entity_hh_id", "raw_api_payload", ["entity_hh_id"])
    op.create_index("idx_raw_api_payload_received_at", "raw_api_payload", ["received_at"])
    op.create_index("idx_raw_api_payload_request_log_id", "raw_api_payload", ["api_request_log_id"])

    op.create_table(
        "vacancy",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("hh_vacancy_id", sa.Text(), nullable=False),
        sa.Column("employer_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("employer.id"), nullable=True),
        sa.Column("area_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("area.id"), nullable=True),
        sa.Column("name_current", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at_hh", sa.DateTime(timezone=True), nullable=True),
        sa.Column("archived_at_hh", sa.DateTime(timezone=True), nullable=True),
        sa.Column("alternate_url", sa.Text(), nullable=True),
        sa.Column("employment_type_code", sa.Text(), nullable=True),
        sa.Column("schedule_type_code", sa.Text(), nullable=True),
        sa.Column("experience_code", sa.Text(), nullable=True),
        sa.Column("source_type", sa.Text(), nullable=False, server_default="hh_api"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("hh_vacancy_id", name="uq_vacancy_hh_vacancy_id"),
    )
    op.create_index("idx_vacancy_area_id", "vacancy", ["area_id"])
    op.create_index("idx_vacancy_employer_id", "vacancy", ["employer_id"])
    op.create_index("idx_vacancy_published_at", "vacancy", ["published_at"])

    op.create_table(
        "vacancy_seen_event",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("vacancy_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("vacancy.id"), nullable=False),
        sa.Column("crawl_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("crawl_run.id"), nullable=False),
        sa.Column(
            "crawl_partition_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("crawl_partition.id"),
            nullable=False,
        ),
        sa.Column("seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("list_position", sa.Integer(), nullable=True),
        sa.Column("short_hash", sa.Text(), nullable=False),
        sa.Column("short_payload_ref_id", sa.BigInteger(), sa.ForeignKey("raw_api_payload.id"), nullable=True),
        sa.UniqueConstraint("vacancy_id", "crawl_partition_id", "seen_at", name="uq_vse_seen"),
    )
    op.create_index("idx_vacancy_seen_event_run_id", "vacancy_seen_event", ["crawl_run_id"])
    op.create_index("idx_vacancy_seen_event_seen_at", "vacancy_seen_event", ["seen_at"])
    op.create_index("idx_vacancy_seen_event_vacancy_id", "vacancy_seen_event", ["vacancy_id"])

    op.create_table(
        "vacancy_current_state",
        sa.Column("vacancy_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("vacancy.id"), primary_key=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("seen_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("consecutive_missing_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_probably_inactive", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("last_seen_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("crawl_run.id"), nullable=True),
        sa.Column("last_short_hash", sa.Text(), nullable=True),
        sa.Column("last_detail_hash", sa.Text(), nullable=True),
        sa.Column("last_detail_fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("detail_fetch_status", sa.Text(), nullable=False, server_default="not_requested"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_vacancy_current_state_detail_status", "vacancy_current_state", ["detail_fetch_status"])
    op.create_index("idx_vacancy_current_state_inactive", "vacancy_current_state", ["is_probably_inactive"])
    op.create_index("idx_vacancy_current_state_last_seen_at", "vacancy_current_state", ["last_seen_at"])

    op.create_table(
        "vacancy_snapshot",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("vacancy_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("vacancy.id"), nullable=False),
        sa.Column("snapshot_type", sa.Text(), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("crawl_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("crawl_run.id"), nullable=True),
        sa.Column("short_hash", sa.Text(), nullable=True),
        sa.Column("detail_hash", sa.Text(), nullable=True),
        sa.Column("short_payload_ref_id", sa.BigInteger(), sa.ForeignKey("raw_api_payload.id"), nullable=True),
        sa.Column("detail_payload_ref_id", sa.BigInteger(), sa.ForeignKey("raw_api_payload.id"), nullable=True),
        sa.Column("normalized_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("change_reason", sa.Text(), nullable=True),
    )
    op.create_index("idx_vacancy_snapshot_captured_at", "vacancy_snapshot", ["captured_at"])
    op.create_index("idx_vacancy_snapshot_detail_hash", "vacancy_snapshot", ["detail_hash"])
    op.create_index("idx_vacancy_snapshot_vacancy_id", "vacancy_snapshot", ["vacancy_id"])

    op.create_table(
        "detail_fetch_attempt",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("vacancy_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("vacancy.id"), nullable=False),
        sa.Column("crawl_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("crawl_run.id"), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index("idx_detail_fetch_attempt_requested_at", "detail_fetch_attempt", ["requested_at"])
    op.create_index("idx_detail_fetch_attempt_status", "detail_fetch_attempt", ["status"])
    op.create_index("idx_detail_fetch_attempt_vacancy_id", "detail_fetch_attempt", ["vacancy_id"])


def downgrade() -> None:
    op.drop_index("idx_detail_fetch_attempt_vacancy_id", table_name="detail_fetch_attempt")
    op.drop_index("idx_detail_fetch_attempt_status", table_name="detail_fetch_attempt")
    op.drop_index("idx_detail_fetch_attempt_requested_at", table_name="detail_fetch_attempt")
    op.drop_table("detail_fetch_attempt")

    op.drop_index("idx_vacancy_snapshot_vacancy_id", table_name="vacancy_snapshot")
    op.drop_index("idx_vacancy_snapshot_detail_hash", table_name="vacancy_snapshot")
    op.drop_index("idx_vacancy_snapshot_captured_at", table_name="vacancy_snapshot")
    op.drop_table("vacancy_snapshot")

    op.drop_index("idx_vacancy_current_state_last_seen_at", table_name="vacancy_current_state")
    op.drop_index("idx_vacancy_current_state_inactive", table_name="vacancy_current_state")
    op.drop_index("idx_vacancy_current_state_detail_status", table_name="vacancy_current_state")
    op.drop_table("vacancy_current_state")

    op.drop_index("idx_vacancy_seen_event_vacancy_id", table_name="vacancy_seen_event")
    op.drop_index("idx_vacancy_seen_event_seen_at", table_name="vacancy_seen_event")
    op.drop_index("idx_vacancy_seen_event_run_id", table_name="vacancy_seen_event")
    op.drop_table("vacancy_seen_event")

    op.drop_index("idx_vacancy_published_at", table_name="vacancy")
    op.drop_index("idx_vacancy_employer_id", table_name="vacancy")
    op.drop_index("idx_vacancy_area_id", table_name="vacancy")
    op.drop_table("vacancy")

    op.drop_index("idx_raw_api_payload_request_log_id", table_name="raw_api_payload")
    op.drop_index("idx_raw_api_payload_received_at", table_name="raw_api_payload")
    op.drop_index("idx_raw_api_payload_entity_hh_id", table_name="raw_api_payload")
    op.drop_table("raw_api_payload")

    op.drop_index("idx_api_request_log_status_code", table_name="api_request_log")
    op.drop_index("idx_api_request_log_run_id", table_name="api_request_log")
    op.drop_index("idx_api_request_log_requested_at", table_name="api_request_log")
    op.drop_index("idx_api_request_log_partition_id", table_name="api_request_log")
    op.drop_table("api_request_log")

    op.drop_index("idx_crawl_partition_status", table_name="crawl_partition")
    op.drop_index("idx_crawl_partition_run_id", table_name="crawl_partition")
    op.drop_table("crawl_partition")

    op.drop_table("professional_role")
    op.drop_index("idx_employer_name", table_name="employer")
    op.drop_table("employer")
    op.drop_table("area")

    op.drop_index("idx_crawl_run_status", table_name="crawl_run")
    op.drop_index("idx_crawl_run_started_at", table_name="crawl_run")
    op.drop_table("crawl_run")

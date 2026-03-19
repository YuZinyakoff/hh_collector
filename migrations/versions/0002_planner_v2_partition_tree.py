"""Add planner v2 tree fields to crawl_partition."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_planner_v2_partition_tree"
down_revision: str | None = "0001_initial_schema"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "crawl_partition",
        sa.Column(
            "parent_partition_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.create_foreign_key(
        "fk_crawl_partition_parent_partition_id",
        "crawl_partition",
        "crawl_partition",
        ["parent_partition_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.add_column(
        "crawl_partition",
        sa.Column(
            "scope_key",
            sa.Text(),
            nullable=True,
        ),
    )
    op.execute("UPDATE crawl_partition SET scope_key = partition_key WHERE scope_key IS NULL")
    op.alter_column("crawl_partition", "scope_key", nullable=False)
    op.add_column(
        "crawl_partition",
        sa.Column(
            "depth",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "crawl_partition",
        sa.Column(
            "split_dimension",
            sa.Text(),
            nullable=True,
        ),
    )
    op.add_column(
        "crawl_partition",
        sa.Column(
            "split_value",
            sa.Text(),
            nullable=True,
        ),
    )
    op.add_column(
        "crawl_partition",
        sa.Column(
            "planner_policy_version",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'v1'"),
        ),
    )
    op.add_column(
        "crawl_partition",
        sa.Column(
            "is_terminal",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
    )
    op.add_column(
        "crawl_partition",
        sa.Column(
            "is_saturated",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "crawl_partition",
        sa.Column(
            "coverage_status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'unassessed'"),
        ),
    )
    op.create_unique_constraint(
        "uq_crawl_partition_run_scope_key",
        "crawl_partition",
        ["crawl_run_id", "scope_key"],
    )
    op.create_index(
        "idx_crawl_partition_parent_partition_id",
        "crawl_partition",
        ["parent_partition_id"],
    )
    op.create_index(
        "idx_crawl_partition_coverage_status",
        "crawl_partition",
        ["coverage_status"],
    )


def downgrade() -> None:
    op.drop_index("idx_crawl_partition_coverage_status", table_name="crawl_partition")
    op.drop_index("idx_crawl_partition_parent_partition_id", table_name="crawl_partition")
    op.drop_constraint(
        "uq_crawl_partition_run_scope_key",
        "crawl_partition",
        type_="unique",
    )
    op.drop_column("crawl_partition", "coverage_status")
    op.drop_column("crawl_partition", "is_saturated")
    op.drop_column("crawl_partition", "is_terminal")
    op.drop_column("crawl_partition", "planner_policy_version")
    op.drop_column("crawl_partition", "split_value")
    op.drop_column("crawl_partition", "split_dimension")
    op.drop_column("crawl_partition", "depth")
    op.drop_column("crawl_partition", "scope_key")
    op.drop_constraint(
        "fk_crawl_partition_parent_partition_id",
        "crawl_partition",
        type_="foreignkey",
    )
    op.drop_column("crawl_partition", "parent_partition_id")

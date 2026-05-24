"""Add first-detail backlog lease columns."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_first_detail_backlog_lease"
down_revision: str | None = "0003_snapshot_retention_idx"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "vacancy_current_state",
        sa.Column("first_detail_lease_owner", sa.Text(), nullable=True),
    )
    op.add_column(
        "vacancy_current_state",
        sa.Column("first_detail_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "idx_vacancy_current_state_first_detail_lease_expires",
        "vacancy_current_state",
        ["first_detail_lease_expires_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_vacancy_current_state_first_detail_lease_expires",
        table_name="vacancy_current_state",
    )
    op.drop_column("vacancy_current_state", "first_detail_lease_expires_at")
    op.drop_column("vacancy_current_state", "first_detail_lease_owner")

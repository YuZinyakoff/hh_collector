"""Add vacancy_snapshot retention index."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_snapshot_retention_idx"
down_revision: str | None = "0002_planner_v2_partition_tree"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "idx_vacancy_snapshot_vacancy_type_captured_at",
        "vacancy_snapshot",
        ["vacancy_id", "snapshot_type", sa.text("captured_at DESC")],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_vacancy_snapshot_vacancy_type_captured_at",
        table_name="vacancy_snapshot",
    )

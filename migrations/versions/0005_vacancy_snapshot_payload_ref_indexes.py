"""Add vacancy_snapshot payload reference indexes."""

from collections.abc import Sequence

from alembic import op

revision: str = "0005_snapshot_payload_ref_idx"
down_revision: str | None = "0004_first_detail_backlog_lease"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "idx_vacancy_snapshot_short_payload_ref_id",
        "vacancy_snapshot",
        ["short_payload_ref_id"],
    )
    op.create_index(
        "idx_vacancy_snapshot_detail_payload_ref_id",
        "vacancy_snapshot",
        ["detail_payload_ref_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_vacancy_snapshot_detail_payload_ref_id",
        table_name="vacancy_snapshot",
    )
    op.drop_index(
        "idx_vacancy_snapshot_short_payload_ref_id",
        table_name="vacancy_snapshot",
    )

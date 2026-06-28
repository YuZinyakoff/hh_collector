"""Add vacancy_seen_event payload reference index."""

from collections.abc import Sequence

from alembic import op

revision: str = "0006_seen_event_payload_ref_idx"
down_revision: str | None = "0005_snapshot_payload_ref_idx"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            "idx_vacancy_seen_event_short_payload_ref_id",
            "vacancy_seen_event",
            ["short_payload_ref_id"],
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "idx_vacancy_seen_event_short_payload_ref_id",
            table_name="vacancy_seen_event",
            postgresql_concurrently=True,
        )

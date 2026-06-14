"""saved_jobs id -> uuid

Revision ID: c5d6e7f8a9b0
Revises: b2c3d4e5f6a7
Create Date: 2026-06-14 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "c5d6e7f8a9b0"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # No FK references saved_jobs.id, so we can swap the PK type outright.
    op.drop_column("saved_jobs", "id")
    op.add_column(
        "saved_jobs",
        sa.Column(
            "id",
            sa.UUID(),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
    )
    op.create_primary_key("saved_jobs_pkey", "saved_jobs", ["id"])


def downgrade() -> None:
    op.drop_constraint("saved_jobs_pkey", "saved_jobs", type_="primary")
    op.drop_column("saved_jobs", "id")
    op.add_column(
        "saved_jobs",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
    )
    op.create_primary_key("saved_jobs_pkey", "saved_jobs", ["id"])

"""create_saved_jobs_table

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-06-12 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# saved -> applied -> interviewing -> offer -> rejected
SAVED_JOB_STATUSES = ("saved", "applied", "interviewing", "offer", "rejected")


def upgrade() -> None:
    op.create_table(
        "saved_jobs",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            server_default=sa.text("'saved'"),
            nullable=False,
        ),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("applied_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("user_id", "job_id", name="uq_saved_jobs_user_job"),
        sa.CheckConstraint(
            "status IN ('saved', 'applied', 'interviewing', 'offer', 'rejected')",
            name="ck_saved_jobs_status",
        ),
    )

    op.create_index(
        "idx_saved_jobs_user",
        "saved_jobs",
        ["user_id", "updated_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_saved_jobs_user", table_name="saved_jobs")
    op.drop_table("saved_jobs")

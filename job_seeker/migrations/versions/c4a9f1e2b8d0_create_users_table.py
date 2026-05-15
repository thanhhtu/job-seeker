"""create_users_table

Revision ID: c4a9f1e2b8d0
Revises: 880d0d5146f3
Create Date: 2026-05-13

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "c4a9f1e2b8d0"
down_revision: Union[str, Sequence[str], None] = "880d0d5146f3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index("uq_users_email", "users", ["email"], unique=True)
    op.create_index("idx_chat_sessions_user_id", "chat_sessions", ["user_id"])


def downgrade() -> None:
    op.drop_index("idx_chat_sessions_user_id", table_name="chat_sessions")
    op.drop_index("uq_users_email", table_name="users")
    op.drop_table("users")

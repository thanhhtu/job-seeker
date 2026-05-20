"""add_chat_sessions_is_guest

Revision ID: d7e8f9a0b1c2
Revises: c4a9f1e2b8d0
Create Date: 2026-05-19

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "d7e8f9a0b1c2"
down_revision: Union[str, Sequence[str], None] = "c4a9f1e2b8d0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "chat_sessions",
        sa.Column("is_guest", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.execute(
        """
        UPDATE chat_sessions s
        SET is_guest = true
        WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = s.user_id)
        """
    )
    op.create_index("idx_chat_sessions_is_guest", "chat_sessions", ["is_guest"])


def downgrade() -> None:
    op.drop_index("idx_chat_sessions_is_guest", table_name="chat_sessions")
    op.drop_column("chat_sessions", "is_guest")

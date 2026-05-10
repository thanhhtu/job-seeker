"""create_extensions_and_search_config

Revision ID: 6945b11df349
Revises:
Create Date: 2026-05-07 23:27:17.731663

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "6945b11df349"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_textsearch")
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("CREATE EXTENSION IF NOT EXISTS unaccent")

    op.execute("DROP TEXT SEARCH CONFIGURATION IF EXISTS public.vietnamese_unaccent")
    op.execute("CREATE TEXT SEARCH CONFIGURATION public.vietnamese_unaccent (COPY = simple)")
    op.execute(
        """
        ALTER TEXT SEARCH CONFIGURATION public.vietnamese_unaccent
        ALTER MAPPING FOR asciiword WITH unaccent, simple
        """
    )


def downgrade() -> None:
    op.execute("DROP TEXT SEARCH CONFIGURATION IF EXISTS public.vietnamese_unaccent")

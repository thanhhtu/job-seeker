"""create_jobs_table

Revision ID: 55659f3fd5ff
Revises: 6945b11df349
Create Date: 2026-05-07 23:37:11.457061

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, NUMERIC, UUID

# revision identifiers, used by Alembic.
revision: str = "55659f3fd5ff"
down_revision: Union[str, Sequence[str], None] = "6945b11df349"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),

        sa.Column("job_id", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),

        sa.Column("company_name", sa.Text(), nullable=False),
        sa.Column("company_url", sa.Text()),
        sa.Column("company_id", sa.Text()),
        sa.Column("company_size", sa.Text()),
        sa.Column("company_industry", ARRAY(sa.Text()), nullable=False, server_default="{}"),
        sa.Column("country", sa.Text()),

        sa.Column("salary_raw", sa.Text()),
        sa.Column("salary_min", NUMERIC(15, 2)),
        sa.Column("salary_max", NUMERIC(15, 2)),
        sa.Column("salary_currency", sa.Text()),
        sa.Column("salary_negotiable", sa.Boolean(), nullable=False, server_default="false"),

        sa.Column("location_raw", sa.Text()),
        sa.Column("locations", ARRAY(sa.Text()), nullable=False, server_default="{}"),

        sa.Column("job_domains", ARRAY(sa.Text()), nullable=False, server_default="{}"),
        sa.Column("job_level", sa.Text()),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("requirements", sa.Text()),
        sa.Column("skills", ARRAY(sa.Text()), nullable=False, server_default="{}"),
        sa.Column("experience_years_min", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("education", sa.Text()),
        sa.Column("benefits", sa.Text()),
        sa.Column("work_mode", sa.Text(), nullable=False, server_default="unknown"),
        sa.Column("work_mode_days", sa.Text()),
        sa.Column("overtime_policy", sa.Text()),
        sa.Column("hiring_quantity", sa.Integer()),
        sa.Column("deadline", sa.Date()),

        sa.Column("posted_date", sa.TIMESTAMP(timezone=True)),
        sa.Column("crawled_date", sa.TIMESTAMP(timezone=True)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),

        # vector type SQLAlchemy không biết → dùng raw text
        sa.Column("embedding", sa.Text()),  # placeholder, sẽ ALTER bên dưới

        sa.UniqueConstraint("source", "job_id", name="uq_jobs_source_job_id"),
    )

    # Đổi embedding sang vector(1024) — SA không có type này
    op.execute("ALTER TABLE jobs ALTER COLUMN embedding TYPE vector(1024) USING NULL::vector(1024)")

    # HNSW index (pgvector)
    op.execute(
        """
        CREATE INDEX ON jobs USING HNSW (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
        """
    )

    # BM25 indexes (pg_textsearch)
    for col in ["title", "skills", "job_domains", "description",
                "requirements", "company_name", "job_level", "location_raw"]:
        op.execute(
            f"CREATE INDEX ON jobs USING bm25 ({col}) "
            f"WITH (text_config = 'public.vietnamese_unaccent')"
        )

    # GIN indexes (array containment)
    for col in ["skills", "locations", "job_domains"]:
        op.execute(f"CREATE INDEX ON jobs USING GIN ({col})")

    # Comments
    op.execute("COMMENT ON TABLE jobs IS 'Job listings from itviec and topcv with vector and BM25 search support'")
    op.execute("COMMENT ON COLUMN jobs.embedding IS 'Mistral mistral-embed (1024 dimensions) for semantic similarity'")


def downgrade() -> None:
    op.drop_table("jobs")

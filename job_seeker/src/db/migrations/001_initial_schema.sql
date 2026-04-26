SET search_path TO public;

-- ============================================================
-- Job Seeker Schema - Revised Migration
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_textsearch;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================================
-- Vietnamese Text Search Configuration
-- ============================================================
-- Custom text search config that strips diacritics
-- "lập trình" matches "lap trinh" queries
DROP TEXT SEARCH CONFIGURATION IF EXISTS vietnamese_unaccent;
CREATE TEXT SEARCH CONFIGURATION vietnamese_unaccent (COPY = simple);
ALTER TEXT SEARCH CONFIGURATION vietnamese_unaccent
    ALTER MAPPING FOR asciiword WITH unaccent, simple;

-- ============================================================
-- Main Jobs Table
-- ============================================================
CREATE TABLE jobs 
(
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    job_id               TEXT        NOT NULL,
    source               TEXT        NOT NULL,
    url                  TEXT        NOT NULL,
    title                TEXT        NOT NULL,

    company_name         TEXT        NOT NULL,
    company_url          TEXT,
    company_id           TEXT,
    company_size         TEXT,
    company_industry     TEXT[]      NOT NULL DEFAULT '{}', 
    country              TEXT,

    salary_raw           TEXT,
    salary_min           NUMERIC(15, 2), 
    salary_max           NUMERIC(15, 2),
    salary_currency      TEXT,
    salary_negotiable    BOOLEAN     NOT NULL DEFAULT false,

    location_raw         TEXT,
    locations            TEXT[]      NOT NULL DEFAULT '{}',

    job_domains          TEXT[]      NOT NULL DEFAULT '{}',
    job_level            TEXT,
    description          TEXT        NOT NULL,
    requirements         TEXT,
    skills               TEXT[]      NOT NULL DEFAULT '{}',
    experience_years_min INT         NOT NULL DEFAULT 0,
    education            TEXT,
    benefits             TEXT,
    work_mode            TEXT        NOT NULL DEFAULT 'unknown',
    work_mode_days       TEXT,
    overtime_policy      TEXT,
    hiring_quantity      INT,
    deadline             DATE,
    
    posted_date          TIMESTAMPTZ,
    crawled_date         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    embedding            vector(1024),

    UNIQUE (source, job_id)
);

-- ============================================================
-- Indexes
-- ============================================================

-- 1. pg_vector HNSW Index (Semantic Search)
CREATE INDEX ON jobs USING HNSW (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- 2. pg_textsearch BM25 Indexes (Full-Text Search) - 8 fields
CREATE INDEX ON jobs USING bm25 (title) WITH (text_config = 'public.vietnamese_unaccent');
CREATE INDEX ON jobs USING bm25 (skills) WITH (text_config = 'public.vietnamese_unaccent');
CREATE INDEX ON jobs USING bm25 (job_domains) WITH (text_config = 'public.vietnamese_unaccent');
CREATE INDEX ON jobs USING bm25 (description) WITH (text_config = 'public.vietnamese_unaccent');
CREATE INDEX ON jobs USING bm25 (requirements) WITH (text_config = 'public.vietnamese_unaccent');
CREATE INDEX ON jobs USING bm25 (company_name) WITH (text_config = 'public.vietnamese_unaccent');
CREATE INDEX ON jobs USING bm25 (job_level) WITH (text_config = 'public.vietnamese_unaccent');
CREATE INDEX ON jobs USING bm25 (location_raw) WITH (text_config = 'public.vietnamese_unaccent');

-- 3. GIN Indexes (Array Containment)
CREATE INDEX ON jobs USING GIN (skills);
CREATE INDEX ON jobs USING GIN (locations);
CREATE INDEX ON jobs USING GIN (job_domains);

-- ============================================================
-- Comments
-- ============================================================
COMMENT ON TABLE jobs IS 'Job listings from itviec and topcv with vector and BM25 search support';
COMMENT ON COLUMN jobs.embedding IS 'Mistral mistral-embed (1024 dimensions) for semantic similarity';
COMMENT ON COLUMN jobs.skills IS 'Normalized skill tags array, searchable via BM25 and GIN containment';
COMMENT ON COLUMN jobs.locations IS 'Normalized location codes: ha_noi, ho_chi_minh, da_nang, etc.';
COMMENT ON COLUMN jobs.location_raw IS 'Raw address text for FTS location search: "Hà Nội", "Lotte Mall", etc.';

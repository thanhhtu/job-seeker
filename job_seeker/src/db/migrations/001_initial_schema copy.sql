SET search_path TO public;

-- ============================================================
-- 1. Extensions
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_bm25;

-- ============================================================
-- 2. Vietnamese Text Search Configuration
-- ============================================================
DROP TEXT SEARCH CONFIGURATION IF EXISTS vietnamese_unaccent;
CREATE TEXT SEARCH CONFIGURATION vietnamese_unaccent (COPY = simple);
ALTER TEXT SEARCH CONFIGURATION vietnamese_unaccent
    ALTER MAPPING FOR asciiword WITH unaccent, simple;

-- ============================================================
-- 3. Main Jobs Table
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
-- 4. Triggers
-- ============================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW 
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- 5. Indexes (Hybrid: Vector + BM25 + Filters)
-- ============================================================

-- A. AI Vector Search (HNSW)
CREATE INDEX idx_jobs_embedding ON jobs
    USING HNSW (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 128);

-- B. BM25 Full-text Search
-- This is the most powerful index for keyword search
CREATE INDEX idx_jobs_bm25 ON jobs USING bm25 (
    title, 
    salary_min,
    salary_max,
    salary_currency,
    locations,
    job_level,
    description, 
    requirements, 
    skills
) WITH (
    text_config = 'public.vietnamese_unaccent'
);

-- C. GIN — Support filtering by array (Tags/Filters)
CREATE INDEX idx_jobs_gin_skills     ON jobs USING GIN (skills);
CREATE INDEX idx_jobs_gin_locations  ON jobs USING GIN (locations);
CREATE INDEX idx_jobs_gin_domains    ON jobs USING GIN (job_domains);

-- D. B-tree — Support fast filtering/sorting
CREATE INDEX idx_jobs_job_id         ON jobs (job_id);
CREATE INDEX idx_jobs_posted_date    ON jobs (posted_date DESC) WHERE posted_date IS NOT NULL;
CREATE INDEX idx_jobs_salary_min     ON jobs (salary_min) WHERE salary_min IS NOT NULL;
CREATE INDEX idx_jobs_work_mode      ON jobs (work_mode);
CREATE INDEX idx_jobs_deadline       ON jobs (deadline) WHERE deadline >= CURRENT_DATE;

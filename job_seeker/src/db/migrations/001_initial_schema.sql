-- SET search_path TO public;

-- CREATE EXTENSION IF NOT EXISTS pg_textsearch;
-- CREATE EXTENSION IF NOT EXISTS unaccent;
-- CREATE EXTENSION IF NOT EXISTS vector;
-- CREATE EXTENSION IF NOT EXISTS pgroonga;

-- DROP TEXT SEARCH CONFIGURATION IF EXISTS vietnamese_unaccent;
-- CREATE TEXT SEARCH CONFIGURATION vietnamese_unaccent (COPY = simple);
-- ALTER TEXT SEARCH CONFIGURATION vietnamese_unaccent
--     ALTER MAPPING FOR asciiword WITH unaccent, simple;

-- -- ============================================================
-- -- Main Jobs Table
-- -- ============================================================
-- CREATE TABLE jobs
-- (
--     -- Surrogate primary key
--     id                   UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),

--     -- Business key + source (unique per source, for deduplication)
--     source               TEXT        NOT NULL,
--     job_id               TEXT        NOT NULL,
--     url                  TEXT        NOT NULL,
--     company_url          TEXT,
--     company_id           TEXT,

--     -- Vector search
--     embedding            vector(1024),

--     -- BM25 / PGroonga Full-text search fields
--     title                TEXT        NOT NULL,
--     skills               TEXT[]      NOT NULL             DEFAULT '{}',
--     job_domains          TEXT[]      NOT NULL             DEFAULT '{}',
--     description          TEXT        NOT NULL,
--     requirements         TEXT,

--     -- Keyword filters (store only, no B-tree needed for pure FTS search)
--     work_mode            TEXT        NOT NULL             DEFAULT 'unknown',
--     country              TEXT,
--     job_level            TEXT,
--     education            TEXT,

--     -- Range filters (store only, no B-tree needed for pure FTS search)
--     salary_min           NUMERIC(12, 2),
--     salary_max           NUMERIC(12, 2),
--     salary_currency      TEXT,
--     salary_raw           TEXT,
--     experience_years_min INT                              DEFAULT 0,
--     posted_at            TIMESTAMPTZ,
--     posted_date          TIMESTAMPTZ,
--     crawled_date         TIMESTAMPTZ,

--     -- Array filters
--     locations            TEXT[]      NOT NULL             DEFAULT '{}',
--     location_raw         TEXT,                            -- Raw address for FTS search

--     -- Store only (no index)
--     company_name         TEXT        NOT NULL,
--     company_size         TEXT,
--     company_industry     TEXT[],
--     work_mode_days       TEXT,
--     overtime_policy      TEXT,
--     benefits             TEXT,
--     hiring_quantity      INT,
--     salary_negotiable    BOOLEAN                          DEFAULT false,
--     deadline             DATE,

--     -- Timestamps
--     created_at           TIMESTAMPTZ NOT NULL             DEFAULT now(),
--     updated_at           TIMESTAMPTZ NOT NULL             DEFAULT now(),

--     -- Deduplication constraint: one job_id per source
--     UNIQUE (source, job_id)
-- );

-- -- ============================================================
-- -- Indexes
-- -- ============================================================

-- -- 1. pg_vector HNSW Index (Semantic Search)
-- CREATE INDEX ON jobs USING HNSW (embedding vector_cosine_ops)
--     WITH (m = 16, ef_construction = 64);

-- -- 2. pg_textsearch BM25 Indexes (Full-Text Search)
-- -- Dùng cho ranking BM25 thuần túy, tốt với tokenizer Vietnamese unaccent
-- CREATE INDEX ON jobs USING bm25 (title)        WITH (text_config = 'public.vietnamese_unaccent');
-- CREATE INDEX ON jobs USING bm25 (skills)       WITH (text_config = 'public.vietnamese_unaccent');
-- CREATE INDEX ON jobs USING bm25 (job_domains)  WITH (text_config = 'public.vietnamese_unaccent');
-- CREATE INDEX ON jobs USING bm25 (description)  WITH (text_config = 'public.vietnamese_unaccent');
-- CREATE INDEX ON jobs USING bm25 (requirements) WITH (text_config = 'public.vietnamese_unaccent');
-- CREATE INDEX ON jobs USING bm25 (company_name) WITH (text_config = 'public.vietnamese_unaccent');
-- CREATE INDEX ON jobs USING bm25 (job_level)    WITH (text_config = 'public.vietnamese_unaccent');
-- CREATE INDEX ON jobs USING bm25 (location_raw) WITH (text_config = 'public.vietnamese_unaccent');

-- -- 3. PGroonga Indexes (Multilingual Full-Text Search)
-- -- Lợi thế so với BM25: hỗ trợ tiếng Việt có dấu native (không cần unaccent),
-- -- fuzzy search, prefix search, và LIKE/ILIKE acceleration.
-- --
-- -- Index riêng từng field để:
-- --   - Kiểm soát được tokenizer/normalizer per field
-- --   - Query planner chọn đúng index cho từng điều kiện
-- --   - Tránh index bloat khi chỉ search 1-2 field

-- -- 3a. Core search fields (TEXT) — dùng NormalizerNFKC150 để chuẩn hóa Unicode,
-- --     TokenMecab hoặc TokenNgram cho tiếng Việt
-- CREATE INDEX pgroonga_idx_title
--     ON jobs USING pgroonga (title)
--     WITH (normalizer = 'NormalizerNFKC150',
--           tokenizer  = 'TokenNgram("unify_symbol", true, "unify_digit", true)');

-- CREATE INDEX pgroonga_idx_description
--     ON jobs USING pgroonga (description)
--     WITH (normalizer = 'NormalizerNFKC150',
--           tokenizer  = 'TokenNgram("unify_symbol", true, "unify_digit", true)');

-- CREATE INDEX pgroonga_idx_requirements
--     ON jobs USING pgroonga (requirements)
--     WITH (normalizer = 'NormalizerNFKC150',
--           tokenizer  = 'TokenNgram("unify_symbol", true, "unify_digit", true)');

-- CREATE INDEX pgroonga_idx_company_name
--     ON jobs USING pgroonga (company_name)
--     WITH (normalizer = 'NormalizerNFKC150',
--           tokenizer  = 'TokenNgram("unify_symbol", true, "unify_digit", true)');

-- CREATE INDEX pgroonga_idx_location_raw
--     ON jobs USING pgroonga (location_raw)
--     WITH (normalizer = 'NormalizerNFKC150',
--           tokenizer  = 'TokenNgram("unify_symbol", true, "unify_digit", true)');

-- -- 3b. Array fields (TEXT[]) — PGroonga hỗ trợ array search native,
-- --     cho phép query: skills &@~ 'python' mà không cần unnest
-- CREATE INDEX pgroonga_idx_skills
--     ON jobs USING pgroonga (skills)
--     WITH (normalizer = 'NormalizerNFKC150',
--           tokenizer  = 'TokenNgram("unify_symbol", true, "unify_digit", true)');

-- CREATE INDEX pgroonga_idx_job_domains
--     ON jobs USING pgroonga (job_domains)
--     WITH (normalizer = 'NormalizerNFKC150',
--           tokenizer  = 'TokenNgram("unify_symbol", true, "unify_digit", true)');

-- -- 4. GIN Indexes (Array Containment — exact match)
-- -- Giữ lại cho các query containment chính xác: locations @> '{ha_noi}'
-- -- PGroonga không thay thế được GIN cho containment operator @>
-- CREATE INDEX ON jobs USING GIN (skills);
-- CREATE INDEX ON jobs USING GIN (locations);
-- CREATE INDEX ON jobs USING GIN (job_domains);

-- -- ============================================================
-- -- Comments
-- -- ============================================================
-- COMMENT ON TABLE jobs IS 'Job listings from itviec and topcv with vector, BM25, and PGroonga search support';
-- COMMENT ON COLUMN jobs.embedding    IS 'Mistral mistral-embed (1024 dimensions) for semantic similarity';
-- COMMENT ON COLUMN jobs.skills       IS 'Normalized skill tags array — BM25 (ranking), GIN (exact containment), PGroonga (fuzzy/prefix FTS)';
-- COMMENT ON COLUMN jobs.locations    IS 'Normalized location codes: ha_noi, ho_chi_minh, da_nang, etc. — GIN exact match only';
-- COMMENT ON COLUMN jobs.location_raw IS 'Raw address text — BM25 + PGroonga FTS: "Hà Nội", "Lotte Mall West Lake", etc.';



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
    -- Surrogate primary key
    id                   UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Business key + source (unique per source, for deduplication)
    source               TEXT        NOT NULL,
    job_id               TEXT        NOT NULL,
    url                  TEXT        NOT NULL,
    company_url          TEXT,
    company_id           TEXT,

    -- Vector search
    embedding            vector(1024),

    -- BM25 Full-text search fields
    title                TEXT        NOT NULL,
    skills               TEXT[]      NOT NULL             DEFAULT '{}',
    job_domains          TEXT[]      NOT NULL             DEFAULT '{}',
    description          TEXT        NOT NULL,
    requirements         TEXT,

    -- Keyword filters (store only, no B-tree needed for pure FTS search)
    work_mode            TEXT        NOT NULL             DEFAULT 'unknown',
    country              TEXT,
    job_level            TEXT,
    education            TEXT,

    -- Range filters (store only, no B-tree needed for pure FTS search)
    salary_min           NUMERIC(12, 2),
    salary_max           NUMERIC(12, 2),
    salary_currency      TEXT,
    salary_raw           TEXT,
    experience_years_min INT                              DEFAULT 0,
    posted_at            TIMESTAMPTZ,
    posted_date          TIMESTAMPTZ,
    crawled_date         TIMESTAMPTZ,

    -- Array filters
    locations            TEXT[]      NOT NULL             DEFAULT '{}',
    location_raw          TEXT,                           -- Raw address for FTS search

    -- Store only (no index)
    company_name         TEXT        NOT NULL,
    company_size         TEXT,
    company_industry     TEXT[],
    work_mode_days       TEXT,
    overtime_policy      TEXT,
    benefits             TEXT,
    hiring_quantity      INT,
    salary_negotiable    BOOLEAN                          DEFAULT false,
    deadline             DATE,

    -- Timestamps
    created_at           TIMESTAMPTZ NOT NULL             DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL             DEFAULT now(),

    -- Deduplication constraint: one job_id per source
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

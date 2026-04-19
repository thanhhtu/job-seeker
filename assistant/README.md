# Job Seeker AI Agent

Vietnamese-language job search assistant powered by LangGraph, Mistral AI, and PostgreSQL with hybrid BM25 + vector search.

## Prerequisites

- **Python 3.10+**
- **Docker** & **Docker Compose**
- **LangGraph CLI** (`pip install langgraph-cli[inmem]`)
- **Mistral API Key** ([Get one here](https://console.mistral.ai/))

## Setup

### 1. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:
```text
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5433/postgres
MISTRAL_API_KEY=your_mistral_api_key_here   # REQUIRED
LANGSMITH_API_KEY=                          # Optional, for tracing
```

### 2. Start database

```bash
docker compose up -d
```

### 3. Run migrations

```bash
# Using psql
psql "postgresql://postgres:postgres@localhost:5433/postgres" -f migrations/001_initial_schema.sql

# Or using Docker
docker exec -i job_seeker_db psql -U postgres -d postgres < migrations/001_initial_schema.sql
```

### 4. Install dependencies

```bash
pip install -e . "langgraph-cli[inmem]"
```

### 5. Ingest job data (optional)

```bash
python scripts/ingest.py
```

Loads jobs from `data/itviec_jobs_schema.json` and `data/topcv_jobs_schema.json`, generates Mistral embeddings, and upserts them into the database.

## Run

```bash
langgraph dev
```

Opens LangGraph Studio at `http://localhost:2024`.

For external access (webhooks, mobile testing, etc.), use `--tunnel`:

```bash
langgraph dev --tunnel
```

This exposes the server via a public URL through a tunnel.

### API example

```bash
curl -X POST http://localhost:2024/assistants/agent/invoke \
  -H "Content-Type: application/json" \
  -d '{"input": {"messages": [{"role": "user", "content": "tìm việc backend python hà nội"}]}}'
```

## Project Structure

```
src/agent/
  graph.py           LangGraph state machine (entry point)
  config.py          Database configuration
  db/
    pool.py          Asyncpg connection pool
    repository.py    Job search (BM25 + vector hybrid)
  ingest/
    ingest.py        Ingestion script
    json_loader.py   JSON job data loader
    embedder.py      Mistral embedding generation
  models/
    job.py           Job Pydantic models
migrations/
  001_initial_schema.sql   Database schema + indexes
compose.yaml         Docker Compose (PostgreSQL)
langgraph.json       LangGraph Server config
scripts/ingest.py    Seed job data into database
```

## Configuration

|      Variable       | Required |                             Default                              |             Description              |
|---------------------|----------|------------------------------------------------------------------|--------------------------------------|
| `DATABASE_URL`      |    Yes   | `postgresql+asyncpg://postgres:postgres@localhost:5433/postgres` | Async PostgreSQL connection string   |
| `MISTRAL_API_KEY`   | **Yes**  |                              -                                   | Mistral API key for LLM + embeddings |
| `LANGSMITH_API_KEY` |    No    |                              -                                   | LangSmith tracing                    |

## Database

PostgreSQL with `pg_textsearch` and `vector` extensions:
- **BM25** on 8 fields (title, skills, job_domains, description, requirements, company_name, job_level, location_raw)
- **HNSW vector** index (1024-dim Mistral embeddings)
- **GIN** indexes on array fields (skills, locations, job_domains)

Search uses **hybrid RRF (Reciprocal Rank Fusion)** to combine BM25 and vector results.

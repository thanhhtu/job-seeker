# Job Seeker Agent

An agent for job search and Q&A, built with **LangGraph**, **Mistral AI**, **Ollama** (bge-m3 embeddings), and **PostgreSQL hybrid search** (BM25 + vector).

---

## Table of Contents

- [Project Structure](#project-structure)
- [Workflow](#workflow)
- [Hybrid Search](#hybrid-search)
- [Setup & Run](#setup--run)
- [REST API](#rest-api)
- [Web Chatbot](#web-chatbot)
- [Docker](#docker)
- [Database Migration](#database-migration)
- [Tech Stack](#tech-stack)

---

## Project Structure

```text
job_seeker/
├── frontend/                      # React + Vite web chat UI
│   ├── public/
│   ├── src/
│   │   ├── api/                   
│   │   ├── components/            
│   │   ├── hooks/
│   │   ├── pages/                
│   │   ├── types/
│   │   └── utils/
│   ├── package.json
│   └── vite.config.ts
│
├── src/
│   ├── agent/                     # LangGraph state machine
│   │   ├── graph.py               # Graph definition and routing
│   │   ├── constants.py
│   │   ├── llm/                   # LLM retry helpers
│   │   ├── memory/                # Slots, keywords
│   │   ├── nodes/                 
│   │   └── states/                # AgentState
│   ├── api/                       # FastAPI backend
│   │   ├── app.py                 # App factory, lifespan, routers
│   │   ├── errors.py
│   │   ├── openapi_meta.py
│   │   ├── auth/                  # Register / login / JWT / passwords
│   │   │   ├── router.py
│   │   │   ├── deps.py
│   │   │   ├── jwt_tokens.py
│   │   │   ├── passwords.py
│   │   │   └── schemas.py
│   │   ├── chat/                  # Chat stream endpoint
│   │   │   ├── router.py
│   │   │   └── schemas.py
│   │   └── sessions/              # Chat sessions CRUD
│   │       ├── router.py
│   │       └── schemas.py
│   ├── core/
│   │   ├── config.py              # pydantic-settings
│   │   ├── logger.py
│   │   └── tracing.py             # LangSmith
│   ├── db/
│   │   ├── client.py              # asyncpg pool
│   │   ├── langgraph_checkpoint.py
│   │   └── repositories/
│   │       ├── chat_repository.py # chat_sessions / chat_messages
│   │       ├── job_repository.py  # jobs upsert
│   │       └── user_repository.py
│   ├── ingest/
│   │   ├── embed.py               # Ollama bge-m3 embeddings
│   │   ├── json_loader.py
│   │   └── pipeline.py
│   ├── models/
│   │   └── job_schema.py
│   └── retrieval/                 # Hybrid search + rerank
│       ├── search.py
│       ├── reranker.py
│       └── _filters.py
│
├── crawler/                       # Job crawlers
│   ├── data_job/                  # JSON output + schemas
│   ├── itviec/
│   └── topcv/
│
├── docker/
│   ├── postgres/
│   ├── embedding/                 
│   └── reranker/
│
├── migrations/                    # Alembic (versions/)
├── scripts/
│   └── ingest.py
│
├── alembic.ini
├── compose.yaml
├── langgraph.json
├── pyproject.toml
└── uv.lock
```

---

## Workflow

### Ingest (run once or by batch)

```text
crawler/ (itviec, topcv, data_job)
    → json_loader.py   — load & parse raw data
    → embed.py         — text → vector (bge-m3)
    → client.py        — save into PostgreSQL
```

### Agent (runs whenever the user asks a question)

```text
User input
    → graph.py         — LangGraph orchestration
    → nodes/           — rewrite query → search → rrf → rerank → generate
    → search.py        — PostgreSQL hybrid search
    → reranker.py      — refine results
    → Return response to user
```

---

## Hybrid Search

Search is performed entirely inside PostgreSQL, combining two approaches:

| Method | Mechanism | PostgreSQL feature |
|---|---|---|
| **BM25** | Keyword-based search, exact text matching | `tsvector` + `tsquery` |
| **Vector search** | Semantic search, understands query meaning | `pgvector` + cosine similarity |

Results from both methods are merged using **Reciprocal Rank Fusion (RRF)**, then passed through `reranker.py` for refinement before being returned by the agent.

---

## Setup & Run

### Requirements

- Python 3.12+ (see `.python-version`)
- [uv](https://github.com/astral-sh/uv)
- Docker

### Setup Steps

**1. Install dependencies**
```bash
uv sync
```

**2. Create `.env` file**
```bash
cp .env.example .env
```

Fill in the environment variables:

```env
DATABASE_URL=
DATABASE_DB=
DATABASE_PASSWORD=
DATABASE_USER=

MISTRAL_API_KEY=your_mistral_api_key
OLLAMA_BASE_URL=http://localhost:11434     # Ollama embedding service
RERANKER_URL=http://localhost:8001         # BGE Reranker service

LANGSMITH_API_KEY=                         # Optional
LANGSMITH_TRACING=false
LANGSMITH_PROJECT=job-seeker

TARGETARCH=                                # arm64 (Mac) | amd64 (Windows)

BACKEND_URL=http://localhost:8080

JWT_SECRET=                                # JWT: use a long random secret in production; if empty, a dev default is used (see config)

```

**3. Start PostgreSQL**
```bash
docker compose up -d
```

**4. Run migrations** — see detailed instructions in the [Database Migration](#database-migration) section
```bash
uv run alembic upgrade head
```

**5. Ingest data**
```bash
uv run scripts/ingest.py
```

**6. Start LangGraph server**
```bash
uv run langgraph dev
```

Open LangGraph Studio at `http://localhost:2024`.

> To expose it externally (webhook, mobile testing), use `--tunnel`:
> ```bash
> langgraph dev --tunnel
> ```

---

## REST API

### Run the API (FastAPI)

```bash
uv run uvicorn src.api.app:app --reload --port 8080
```

The examples below assume the API base URL is **`http://localhost:8080`**. Change the host/port if you run Uvicorn differently.

### API docs (Swagger, ReDoc, OpenAPI)

| What | URL (default local) |
|------|----------------------|
| **Swagger UI** — try endpoints, **Authorize** with JWT | **<http://localhost:8080/docs>** |
| **ReDoc** | <http://localhost:8080/redoc> |
| **OpenAPI JSON** — Postman import, codegen, etc. | <http://localhost:8080/openapi.json> |
| **Root `/`** | Redirects to **`/docs`** (307) |

---

## Web Chatbot

Full architecture with React FE + FastAPI BE + LangGraph:

| Component                | File                          |
|--------------------------|-------------------------------|
| LangGraph (brain)        | `src/agent/graph.py`          |
| Backend API (FastAPI)    | `src/api/app.py`              |
| Chat history (PostgreSQL)| `src/db/repositories/chat_repository.py` |
| Frontend (React + Vite)  | `frontend/src/App.tsx`         |

### Run backend

Same as [Run the API (FastAPI)](#rest-api-swagger-auth): Uvicorn on port `8080` (or your chosen port).

### Run frontend

```bash
cd frontend
cp .env.example .env
npm install
npm run dev
```

Frontend runs by default at **`http://localhost:5173`** and calls API at `VITE_API_URL` (default `http://localhost:8080`).

---

## Docker

### Common commands

```bash
# Start services in detached mode
docker compose up -d

# Stop and remove all containers + volumes (full DB reset)
docker compose down -v

# Rebuild images from scratch without cache
docker compose build --no-cache

# View container status
docker compose ps

# View logs of a specific service
docker logs <container_name>
```

> **Note:** `down -v` removes all database data. You will need to rerun migrations and ingest data afterward.

---

## Database Migration

The project uses **Alembic** for database migration management. Migration files are located in `migrations/versions/`.

### Initialize (run only once for a new project setup)

```bash
uv run alembic init migrations
```

### Create a new migration

Whenever the schema changes (new table, new column, etc.), create a new migration:

```bash
# Create migration file manually
uv run alembic revision -m "change_description"

# Or let Alembic auto-detect changes from models
uv run alembic revision --autogenerate -m "change_description"
```

> After creating the file, open the migration inside `migrations/versions/` and review the `upgrade()` and `downgrade()` functions before running it.

### Run migrations

```bash
# Apply all pending migrations
uv run alembic upgrade head

# Upgrade by exactly N steps
uv run alembic upgrade +1
```

### Rollback

```bash
# Rollback to previous migration
uv run alembic downgrade -1

# Rollback to a specific revision
uv run alembic downgrade <revision_id>

# Rollback everything to the initial state
uv run alembic downgrade base

# History migration
uv run alembic history

# Current migration version
uv run alembic current
```

### Check status

```bash
# Show current migration version
uv run alembic current

# Show full migration history
uv run alembic history --verbose
```

---

## Tech Stack

| Library / Service                    | Purpose                                  |
|--------------------------------------|------------------------------------------|
| Python 3.12                          | Runtime                                  |
| FastAPI + Uvicorn                    | Backend API                              |
| React + Vite                         | Web chat UI                              |
| LangGraph + LangChain Core           | Agent orchestration                      |
| `langchain-mistralai`                | Mistral LLM client                       |
| Ollama (bge-m3)                      | Embedding service                        |
| PostgreSQL + pgvector + FTS          | Hybrid search (BM25 + vector)            |
| `asyncpg`                            | Async PostgreSQL driver                  |
| `alembic`                            | Database migration                       |
| `httpx`                              | HTTP client for services                 |
| `PyJWT` + `bcrypt`                   | JWT auth & password hashing              |
| `curl-cffi`                          | Crawler TLS impersonation                |
| LangSmith                            | Tracing (optional)                       |
| `pydantic` / `pydantic-settings`     | Data modeling and configuration          |
| `black`                              | Code formatter                           |
| `uv`                                 | Package manager                          |

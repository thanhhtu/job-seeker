# Job Seeker Agent

An agent for job search and Q&A, built with **LangGraph**, **Mistral AI**, **Ollama** (bge-m3 embeddings), and **PostgreSQL hybrid search** (BM25 + vector).

---

## Table of Contents

- [Project Structure](#project-structure)
- [Workflow](#workflow)
- [Hybrid Search](#hybrid-search)
- [Setup & Run](#setup--run)
- [REST API, Swagger & Auth](#rest-api-swagger-auth)
- [Web Chatbot](#web-chatbot)
- [Docker](#docker)
- [Database Migration](#database-migration)
- [Tech Stack](#tech-stack)

---

## Project Structure

```text
job_seeker/
├── frontend/                 # React + Vite web chat UI
│   ├── src/
│   └── package.json
├── src/
│   ├── agent/               # LangGraph state machine
│   │   ├── graph.py         # Graph definition and routing
│   │   ├── nodes/           
│   │   └── state.py         # AgentState schema
│   ├── api/                 # FastAPI backend
│   │   ├── app.py
│   │   ├── auth_router.py   # Register / login / me / chat-sessions
│   │   ├── deps.py          # JWT Bearer dependencies
│   │   ├── openapi_meta.py  # OpenAPI / Swagger tag descriptions
│   │   └── schemas.py
│   ├── auth/                # JWT + bcrypt
│   ├── users/               # `users` table repository
│   ├── chat_history/         # Chat history store (PostgreSQL)
│   │   └── store.py
│   ├── core/
│   │   ├── config.py         # Environment config (pydantic-settings)
│   │   ├── logger.py         # Shared logger
│   │   └── tracing.py        # LangSmith tracing helper
│   ├── db/
│   │   ├── client.py         # Initialize and manage the asyncpg connection pool. Provides PostgreSQL connection interfaces.
│   │   ├── repository.py     # Job search query functions
│   ├── ingest/
│   │   ├── embed.py          # Generate vector embeddings via Ollama (bge-m3)
│   │   ├── json_loader.py    # Load and parse raw data from JSON
│   │   └── pipeline.py       # Orchestrate ingest flow
│   ├── models/
│   │   └── job_schema.py     # Standard schema for job data
│   └── retrieval/            # Hybrid search + rerank
│       ├── reranker.py
│       └── search.py
│
├── crawler/                 # Crawl data from multiple sources
│   ├── data_job/
│   ├── itviec/
│   └── topcv/
│
├── docker/                  # Dockerfiles for services
├── migrations/              # Alembic migrations
├── scripts/                 # Helper scripts
├── alembic.ini
├── compose.yaml             # Docker Compose (PostgreSQL + pgvector)
├── langgraph.json           # LangGraph Server config
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

## REST API, Swagger & Auth

### `users` table and chat data

| Table | Description |
|-------|-------------|
| **`users`** | Registered accounts: `id` (UUID as text), unique `email`, `password_hash`, `created_at`. |
| **`chat_sessions`** | Chat threads; `user_id` is either a registered user id or an opaque guest id (text). |
| **`chat_messages`** | Messages keyed by `session_id`. |

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

**Using Swagger with auth**

1. Open **<http://localhost:8080/docs>**.
2. Call **`POST /api/auth/register`** or **`POST /api/auth/login`** and copy `access_token` from the response.
3. Click **Authorize** (lock icon), choose **HTTPBearer**, paste **only** the token (Swagger adds the `Bearer ` prefix).
4. Call protected routes (e.g. **`GET /api/auth/me`**, **`GET /api/me/chat-sessions`**, **`GET /api/sessions/{session_id}/messages`**).

Swagger is configured with **persistAuthorization** so the token survives a page refresh while you stay on `/docs`.

### Main endpoints (summary)

| Method | Path | Notes |
|--------|------|--------|
| `POST` | `/api/auth/register` | Sign up; returns JWT + `user` |
| `POST` | `/api/auth/login` | Sign in; returns JWT |
| `GET` | `/api/auth/me` | Requires `Authorization: Bearer <JWT>` |
| `GET` | `/api/me/chat-sessions` | List chat sessions for the authenticated user |
| `POST` | `/api/chat` | Chat; with JWT, messages attach to that user (body `user_id` ignored); without JWT, uses body `user_id` (guest) |
| `GET` | `/api/sessions/{session_id}/messages` | Message history; **requires** JWT and session ownership |
| `GET` | `/health` | Health check |

JWT settings: `JWT_SECRET` and token lifetime (`jwt_expire_minutes`, etc.) in [`src/core/config.py`](src/core/config.py) (pydantic-settings reads matching env vars where applicable).

---

## Web Chatbot

Full architecture with React FE + FastAPI BE + LangGraph:

| Component                | File                          |
|--------------------------|-------------------------------|
| LangGraph (brain)        | `src/agent/graph.py`          |
| Backend API (FastAPI)    | `src/api/app.py`              |
| Chat history (PostgreSQL)| `src/chat_history/store.py`   |
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

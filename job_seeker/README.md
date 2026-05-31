# Job Seeker Agent

Job search agent with **LangGraph**, **Mistral AI**, **Ollama** (bge-m3), and **PostgreSQL hybrid search** (BM25 + vector).

---

## Table of Contents

- [Project Structure](#project-structure)
- [Setup & Run](#setup--run)
- [Crawler](#crawler)
- [REST API & Web Chatbot](#rest-api--web-chatbot)
- [Docker](#docker)
- [Database Migration](#database-migration)

---

## Project Structure

```text
job_seeker/
├── frontend/          # React + Vite chat UI
├── src/
│   ├── agent/         # LangGraph nodes & graph
│   ├── api/           # FastAPI (auth, chat, sessions)
│   ├── core/          # config, logger, tracing
│   ├── db/            # asyncpg, repositories, checkpoints
│   ├── ingest/        # JSON load, embed, upsert
│   ├── models/        # Job schema
│   └── retrieval/     # BM25 + vector search, reranker
├── crawler/           # DBOS workflow (ITviec, TopCV)
├── docker/            # postgres, embedding, reranker
├── migrations/        # Alembic
└── scripts/ingest.py
```

**Data flow:** `crawler/` → JSON → embed → PostgreSQL → agent search (BM25 + vector → RRF → rerank → LLM response)

---

## Setup & Run

**Requirements:** Python 3.12+, [uv](https://github.com/astral-sh/uv), Docker

```bash
uv sync
cp .env.example .env          
docker compose up -d
uv run alembic upgrade head
uv run scripts/ingest.py        # or use crawler (see below)
uv run uvicorn src.api.app:app --reload --port 8080
```

Optional — LangGraph Studio: `uv run langgraph dev` → http://localhost:2024

---

## Crawler

Crawls **ITviec** & **TopCV** via [DBOS](https://docs.dbos.dev/). Output: `crawler/data_job/YYYYMMDD/` → auto embed & upsert to PostgreSQL.

**Prerequisites:** `.env` + PostgreSQL running (same as Setup).

```bash
uv sync --group crawler         # project root

cd crawler

python crawler_workflow.py --ui               # scheduler (00:00 Asia/Ho_Chi_Minh) + UI at :8090
python crawler_workflow.py --trigger          # crawl now
```

State stored in `crawler/crawler_state.sqlite`. No need to run `scripts/ingest.py` after a successful crawl.

---

## REST API & Web Chatbot

**Backend**
```bash
uv run uvicorn src.api.app:app --reload --port 8080
```

| URL | Purpose |
|-----|---------|
| http://localhost:8080/docs | Swagger UI |
| http://localhost:8080/redoc | ReDoc |
| http://localhost:8080/openapi.json | OpenAPI JSON |

**Frontend**
```bash
cd frontend
cp .env.example .env
npm install
npm run dev
```

→ http://localhost:5173 (API default: http://localhost:8080)

---

## Docker

```bash
docker compose up -d                        # start services
docker compose down -v                      # stop + wipe volumes (rerun migrations & ingest)
docker compose build --no-cache             # rebuild images
docker compose ps                           # container status
docker logs <container_name>                # view logs
```

---

## Database Migration

```bash
uv run alembic upgrade head                              # apply all pending migrations
uv run alembic upgrade +1                                # apply one step
uv run alembic downgrade -1                              # rollback one step
uv run alembic revision -m "description"                 # create migration manually
uv run alembic revision --autogenerate -m "description"  # auto-detect schema changes
uv run alembic current                                   # show current version
uv run alembic history --verbose                         # full migration history
```

Migration files: `migrations/versions/`

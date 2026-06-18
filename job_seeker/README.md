# Job Seeker Agent

Job search agent with **LangGraph**, **Mistral AI**, **Ollama** (bge-m3), and **PostgreSQL hybrid search** (full-text search + vector).

---

## Table of Contents

- [Project Structure](#project-structure)
- [Setup & Run](#setup--run)
- [Crawler](#crawler)
- [REST API & Web Chatbot](#rest-api--web-chatbot)
- [Public Sharing with ngrok](#public-sharing-with-ngrok)
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
│   └── retrieval/     # full-text search + vector search, reranker
├── crawler/           # DBOS workflow (ITviec, TopCV)
├── docker/            # postgres, embedding, reranker
├── migrations/        # Alembic
└── scripts/ingest.py
```

**Data flow:** `crawler/` → JSON → embed → PostgreSQL → agent search (full-text search + vector → RRF → rerank → LLM response)

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
uv sync --group crawler                  # project root

cd crawler

python crawler_workflow.py --ui          # scheduler (00:00 Asia/Ho_Chi_Minh) + UI at :8090
python crawler_workflow.py --trigger     # crawl now
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

## Public Sharing with ngrok

Expose the locally-running app to the internet so **anyone can access it** — using a **single** [ngrok](https://ngrok.com) tunnel (works on the free plan).

**How it works:** Vite (port `5173`) serves the frontend **and** proxies `/api` → local backend (`8080`). So one ngrok tunnel on `:5173` exposes the whole app. This is already wired up in [`frontend/vite.config.ts`](frontend/vite.config.ts) (the `server.proxy` + `allowedHosts` settings) and `frontend/.env` (`VITE_API_URL=` empty → same-origin requests).

### 1. One-time setup

```bash
brew install ngrok                          # macOS (or see https://ngrok.com/download)
```

Sign up (free) at https://dashboard.ngrok.com/signup, copy your authtoken from
https://dashboard.ngrok.com/get-started/your-authtoken, then:

```bash
ngrok config add-authtoken <YOUR_TOKEN>
```

### 2. Run (3 terminals)

```bash
# Terminal 1 — backend
uv run uvicorn src.api.app:app --reload --port 8080

# Terminal 2 — frontend (restart so it picks up .env + vite proxy)
cd frontend && npm run dev

# Terminal 3 — public tunnel
./frontend/share-ngrok.sh                   # or: ngrok http 5173
```

ngrok prints a public URL like `https://xxxx.ngrok-free.app` — **share that link**. It serves the frontend, and API calls are auto-proxied to your local backend. No second tunnel needed.

> **Note:** Make sure `frontend/.env` has `VITE_API_URL=` (empty). If you set it to a fixed URL, requests bypass the proxy and other users won't be able to reach the API.

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

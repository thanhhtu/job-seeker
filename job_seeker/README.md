<!-- 
uv init
uv sync
uv add [library]
uv run python main.py
docker exec -it job_seeker_db psql -U postgres -d postgres   
docker compose down -v
docker compose build --no-cache     
-->

# Project Name

Job search agent hỗ trợ tìm kiếm và hỏi đáp về job, sử dụng LangGraph, Mistral AI embeddings, và PostgreSQL hybrid search (BM25 + vector).

---

## 1. Cấu trúc project

```
job_seeker/
├── src/
│   ├── main.py
│   ├── agent/             # Module chứa LangGraph state machine — agent tìm kiếm và hỏi đáp về job cho user.
│   │   ├── graph.py       # Định nghĩa LangGraph graph: kết nối các node, điều kiện chuyển trạng thái. Entry point của agent.
│   │   ├── nodes.py       # Implement từng node trong graph: nhận câu hỏi từ user, gọi search, rerank, sinh câu trả lời.
│   │   └── state.py       # Định nghĩa AgentState — schema trạng thái (messages, context, kết quả search,...) truyền xuyên suốt các node.
│   ├── retrieval/         # Module xử lý tìm kiếm và rerank kết quả — tách biệt hoàn toàn với DB layer.
│   │   ├── reranker.py    # Rerank kết quả search bằng cross-encoder hoặc LLM trước khi trả về cho agent.
│   │   └── search.py      # Thực hiện hybrid search (BM25 + vector) trong PostgreSQL. Kết hợp kết quả bằng Reciprocal Rank Fusion (RRF).
│   ├── core/
│   │   ├── config.py      # Load và validate environment variables (DATABASE_URL, MISTRAL_API_KEY,...). Dùng pydantic-settings.
│   │   └── logger.py      # Cấu hình logger dùng chung cho toàn bộ app.
│   ├── db/
│   │   ├── client.py      # Khởi tạo và quản lý asyncpg connection pool. Cung cấp interface kết nối tới PostgreSQL.
│   │   ├── repository.py  # Query functions tìm kiếm job. Không dùng ORM, viết SQL trực tiếp với asyncpg.
│   │   └── migrations/    # SQL migration files để khởi tạo và cập nhật schema DB theo từng version.
│   ├── ingest/
│   │   ├── embed.py       # Tạo vector embedding từ text dùng bge-m3. Quản lý singleton embedder, chạy bất đồng bộ.
│   │   ├── json_loader.py # Load và parse raw data từ file JSON trước khi đưa vào pipeline.
│   │   └── pipeline.py    # Orchestrate toàn bộ flow ingest: load -> embed -> lưu DB.
│   └── models/
│       └── job_schema.py  # Định nghĩa schema/model chuẩn cho dữ liệu job dùng xuyên suốt pipeline và DB layer.
│ 
├── crawler/               # Module crawl dữ liệu job từ nhiều nguồn. Mỗi nguồn là một sub-folder riêng.
│   ├── data_job/
│   ├── itviec/
│   └── topcv/ 
│ 
├── scripts/
│   ├── ingest.py          # Script chạy end-to-end pipeline ingest từ terminal.
│   └── main.py            # Script tiện ích, chạy các task thủ công khi cần.
│ 
├── .editorconfig
├── .env
├── .gitignore
├── .python-version        # Chỉ định Python version dùng cho project (dùng với pyenv).
├── Dockerfile             # Docker image cho application.
├── README.md
├── compose.yaml           # Docker Compose config để chạy PostgreSQL (với pgvector) local.
├── langgraph.json         # Config LangGraph Server: định nghĩa graph entry point, dependencies.
├── pyproject.toml         # Cấu hình project: dependencies, ruff (lint/format), uv.
└── uv.lock
```

---

## 2. Flow 
### 2.1. Flow ingest

Chạy một lần (hoặc theo batch) để nhập data vào hệ thống.

```
crawler/itviec, crawler/topcv, crawler/data_job
      ↓
json_loader.py (load + parse raw data)
      ↓
embed.py (text → vector qua bge-m3)
      ↓
clients.py (lưu vào PostgreSQL)
```

---

### 2.2. Flow agent (query)

Chạy mỗi khi user đặt câu hỏi hoặc tìm kiếm job.

```
User hỏi / tìm kiếm
      ↓
graph.py (LangGraph điều phối)
      ↓
nodes.py (embed câu hỏi → gọi search → rerank → sinh trả lời)
      ↓
retrieval/search.py (hybrid search PostgreSQL)
      ↓
retrieval/reranker.py (rerank kết quả)
      ↓
Trả kết quả / câu trả lời cho user
```

---

## 3. Hybrid search (BM25 + Vector)

Tìm kiếm chạy hoàn toàn trong PostgreSQL, kết hợp 2 phương pháp:

| Phương pháp       | Cơ chế                                   | PostgreSQL feature                         |
|-------------------|------------------------------------------|--------------------------------------------|
| **BM25**          | Tìm kiếm từ khóa, khớp text chính xác    | `tsvector` + `tsquery` (full-text search)  |
| **Vector search** | Tìm kiếm ngữ nghĩa, hiểu ý nghĩa câu hỏi | `pgvector` + cosine similarity             |

Kết quả từ 2 phương pháp được merge bằng **Reciprocal Rank Fusion (RRF)**, sau đó đưa qua `reranker.py` để tinh chỉnh lần cuối trước khi trả về agent.

---

## 4. Environment variables

Tạo file `.env` từ `.env.example`:

```bash
cp .env.example .env
```

```env
DATABASE_URL=
DATABASE_DB=
DATABASE_PASSWORD=
DATABASE_USER=
MISTRAL_API_KEY=your_mistral_api_key_here   # REQUIRED
LANGSMITH_API_KEY=                          # Optional, for tracing
TARGETARCH=                                 # Mac: arm64 | Win: amd64
```

---

## 5. Cài đặt và chạy

### 5.1. Yêu cầu

- Python 3.11+ (xem `.python-version`)
- [uv](https://github.com/astral-sh/uv)
- Docker

### 5.2. Setup

```bash
# 1. Cài dependencies
uv sync

# 2. Setup .env
cp .env.example .env

# 3. Chạy PostgreSQL
docker compose up

# 4. Chạy migrations
# Using psql
psql "postgresql://postgres:postgres@localhost:5433/postgres" -f src/db/migrations/001_initial_schema.sql

# Or using Docker
docker exec -i job_seeker_db psql -U postgres -d postgres < src/db/migrations/001_initial_schema.sql

# 5. Chạy ingest data
uv run scripts/ingest.py

# 6. Start LangGraph server
langgraph dev
```

---

## Tech stack

| Thư viện                          | Mục đích                                           |
|-----------------------------------|----------------------------------------------------|
| `langgraph`                       | State machine agent — tìm kiếm và hỏi đáp về job   |
| `asyncpg`                         | Kết nối PostgreSQL bất đồng bộ, không dùng ORM     |
| `pgvector`                        | Vector search trong PostgreSQL                     |
| `langchain-mistralai`             | Tạo embedding qua Mistral AI                       |
| `pydantic` / `pydantic-settings`  | Data modeling và config management                 |
| `ruff`                            | Linter + formatter                                 |
| `uv`                              | Package manager                                    |
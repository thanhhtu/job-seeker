from __future__ import annotations

# LangSmith đọc os.environ — pydantic chỉ load .env vào Settings, không set env.
from src.core.tracing import setup_langsmith_tracing

setup_langsmith_tracing()

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool

from src.agent.graph import build_graph
from src.api.schemas import ChatHistoryResponse, ChatMessage, ChatRequest, ChatResponse
from src.chat_history.store import ChatHistoryStore
from src.core.config import settings
from src.core.logger import get_logger
from src.db.client import close_pool
from src.db.langgraph_checkpoint import (
    ensure_langgraph_checkpoint_schema,
    postgres_conninfo_for_psycopg,
)

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    dsn = postgres_conninfo_for_psycopg(settings.database_url)
    await ensure_langgraph_checkpoint_schema(dsn)
    pool = AsyncConnectionPool(conninfo=dsn, max_size=10, open=False)
    await pool.open()
    checkpointer = AsyncPostgresSaver(pool)
    app.state.graph = build_graph(checkpointer)
    logger.info("LangGraph compiled with Postgres checkpointer (thread_id = session_id)")
    try:
        yield
    finally:
        await pool.close()
        await close_pool()


app = FastAPI(title="Job Seeker Chat API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

store = ChatHistoryStore()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/chat", response_model=ChatResponse)
async def chat(payload: ChatRequest) -> ChatResponse:
    message = payload.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message must not be empty.")

    try:
        session_id = await store.ensure_session(payload.user_id, payload.session_id)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    graph = app.state.graph
    config = {"configurable": {"thread_id": session_id}}

    # Chỉ gửi tin nhắn mới; checkpoint khôi phục messages + parsed_query + summary.
    result = await graph.ainvoke({"messages": [HumanMessage(content=message)]}, config)
    assistant_message = (result.get("output") or "").strip()
    if not assistant_message:
        assistant_message = "I could not generate a response. Please try again."

    await store.add_message(session_id=session_id, role="user", content=message)
    await store.add_message(session_id=session_id, role="assistant", content=assistant_message)

    return ChatResponse(
        session_id=session_id,
        user_message=message,
        assistant_message=assistant_message,
    )


@app.get("/api/sessions/{session_id}/messages", response_model=ChatHistoryResponse)
async def get_session_messages(session_id: str) -> ChatHistoryResponse:
    rows = await store.get_messages(session_id)
    messages = [
        ChatMessage(
            role=row["role"],
            content=row["content"],
            created_at=row["created_at"],
        )
        for row in rows
    ]
    return ChatHistoryResponse(session_id=session_id, messages=messages)

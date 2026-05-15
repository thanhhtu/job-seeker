from __future__ import annotations

from src.core.tracing import setup_langsmith_tracing

setup_langsmith_tracing()

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool

from src.agent.graph import build_graph
from src.api.auth_router import me_router, router as auth_router
from src.api.deps import get_current_user, optional_current_user
from src.api.openapi_meta import APP_DESCRIPTION, OPENAPI_TAGS
from src.api.schemas import ChatHistoryResponse, ChatMessage, ChatRequest, ChatResponse
from src.chat_history.store import ChatHistoryStore
from src.users.repository import UserRecord
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


app = FastAPI(
    title="Job Seeker Chat API",
    version="1.0.0",
    lifespan=lifespan,
    description=APP_DESCRIPTION,
    openapi_tags=OPENAPI_TAGS,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={
        "persistAuthorization": True,
        "displayRequestDuration": True,
        "filter": True,
        "tryItOutEnabled": True,
    },
)
app.include_router(auth_router)
app.include_router(me_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

store = ChatHistoryStore()


@app.get("/", include_in_schema=False)
async def root_redirect_to_docs() -> RedirectResponse:
    return RedirectResponse(url="/docs", status_code=307)


@app.get("/health", tags=["health"], summary="Health check")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/api/chat",
    response_model=ChatResponse,
    tags=["chat"],
    summary="Chat with the agent",
    description=(
        "If the request includes the header `Authorization: Bearer <JWT>`, "
        "the message is associated with the authenticated user and `user_id` "
        "in the request body is ignored; chat history is stored in `chat_messages`. "
        "Without a token: uses `user_id` + `session_id` as a guest flow — "
        "LangGraph checkpoints still use `session_id`, but **no** rows are written "
        "to `chat_messages`."
    ),
)
async def chat(
    payload: ChatRequest,
    auth_user: UserRecord | None = Depends(optional_current_user),
) -> ChatResponse:
    message = payload.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message must not be empty.")

    if auth_user is not None:
        effective_user_id = auth_user.id
    else:
        effective_user_id = (payload.user_id or "anonymous").strip() or "anonymous"

    try:
        session_id = await store.ensure_session(effective_user_id, payload.session_id)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    graph = app.state.graph
    config = {"configurable": {"thread_id": session_id}}

    # Only send the new message; checkpoint restores messages + parsed_query + summary.
    result = await graph.ainvoke({"messages": [HumanMessage(content=message)]}, config)
    assistant_message = (result.get("output") or "").strip()
    if not assistant_message:
        assistant_message = "I could not generate a response. Please try again."

    if auth_user is not None:
        await store.add_message(session_id=session_id, role="user", content=message)
        await store.add_message(
            session_id=session_id, role="assistant", content=assistant_message
        )

    return ChatResponse(
        session_id=session_id,
        user_message=message,
        assistant_message=assistant_message,
    )


@app.get(
    "/api/sessions/{session_id}/messages",
    response_model=ChatHistoryResponse,
    tags=["history"],
    summary="Session message history",
    description=(
        "JWT required; only the session owner "
        "(`session_id` belongs to the user in the token) can access it."
    ),
)
async def get_session_messages(
    session_id: str,
    user: UserRecord = Depends(get_current_user),
) -> ChatHistoryResponse:
    owner = await store.get_session_owner(session_id)
    if owner is None:
        raise HTTPException(status_code=404, detail="Session not found.")
    if owner != user.id:
        raise HTTPException(status_code=403, detail="You do not have access to this session.")

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

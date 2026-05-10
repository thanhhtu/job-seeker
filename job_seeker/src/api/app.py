from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from langchain_core.messages import AIMessage, HumanMessage

from src.agent.graph import build_graph
from src.api.schemas import ChatHistoryResponse, ChatMessage, ChatRequest, ChatResponse
from src.chat_history.store import ChatHistoryStore
from src.db.client import close_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    yield
    # shutdown
    await close_pool()


app = FastAPI(title="Job Seeker Chat API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

graph = build_graph()
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

    history_rows = await store.get_messages(session_id)
    history_messages = []
    for row in history_rows:
        if row["role"] == "user":
            history_messages.append(HumanMessage(content=row["content"]))
        else:
            history_messages.append(AIMessage(content=row["content"]))
    history_messages.append(HumanMessage(content=message))

    result = await graph.ainvoke({"messages": history_messages})
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

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from src.api.auth.deps import get_current_user
from src.api.sessions.schemas import (
    ChatHistoryResponse,
    ChatMessage,
    ChatSessionSummary,
    UpdateSessionTitleRequest,
)
from src.chat_history.store import ChatHistoryStore
from src.users.repository import UserRecord

router = APIRouter(prefix="/api/sessions", tags=["history"])
me_router = APIRouter(prefix="/api/me", tags=["me"])
_store = ChatHistoryStore()


@me_router.get(
    "/chat-sessions",
    response_model=list[ChatSessionSummary],
    summary="My chat sessions",
    description="JWT required. Returns sessions with message count and last message timestamp.",
)
async def list_my_chat_sessions(
    user: UserRecord = Depends(get_current_user),
) -> list[ChatSessionSummary]:
    rows = await _store.list_sessions_for_user(user.id)
    return [
        ChatSessionSummary(
            session_id=r["session_id"],
            title=r.get("title"),
            created_at=r["created_at"],
            last_message_at=r["last_message_at"],
            message_count=int(r["message_count"]),
        )
        for r in rows
    ]


@me_router.patch(
    "/chat-sessions/{session_id}",
    response_model=ChatSessionSummary,
    summary="Rename chat session",
    description="JWT required. Updates the display title for a session you own.",
)
async def update_my_chat_session_title(
    session_id: str,
    payload: UpdateSessionTitleRequest,
    user: UserRecord = Depends(get_current_user),
) -> ChatSessionSummary:
    owner = await _store.get_session_owner(session_id)
    if owner is None:
        raise HTTPException(status_code=404, detail="Session not found.")
    if owner != user.id:
        raise HTTPException(status_code=403, detail="You do not have access to this session.")

    updated = await _store.update_session_title(session_id, user.id, payload.title)
    if not updated:
        raise HTTPException(status_code=400, detail="Title must not be empty.")

    rows = await _store.list_sessions_for_user(user.id)
    row = next((r for r in rows if r["session_id"] == session_id), None)
    if row is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    return ChatSessionSummary(
        session_id=row["session_id"],
        title=row.get("title"),
        created_at=row["created_at"],
        last_message_at=row["last_message_at"],
        message_count=int(row["message_count"]),
    )


@router.get(
    "/{session_id}/messages",
    response_model=ChatHistoryResponse,
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
    owner = await _store.get_session_owner(session_id)
    if owner is None:
        raise HTTPException(status_code=404, detail="Session not found.")
    if owner != user.id:
        raise HTTPException(status_code=403, detail="You do not have access to this session.")

    rows = await _store.get_messages(session_id)
    messages = [
        ChatMessage(
            role=row["role"],
            content=row["content"],
            created_at=row["created_at"],
        )
        for row in rows
    ]
    return ChatHistoryResponse(session_id=session_id, messages=messages)

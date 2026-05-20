from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ChatSessionSummary(BaseModel):
    session_id: str
    title: str | None = None
    created_at: datetime
    last_message_at: datetime | None
    message_count: int


class UpdateSessionTitleRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)


class ChatMessage(BaseModel):
    role: str
    content: str
    created_at: datetime


class ChatHistoryResponse(BaseModel):
    session_id: str
    messages: list[ChatMessage]

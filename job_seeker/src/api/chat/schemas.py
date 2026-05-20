from __future__ import annotations

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    user_id: str = Field(default="anonymous")
    session_id: str | None = None


class ChatResponse(BaseModel):
    session_id: str
    user_message: str
    assistant_message: str

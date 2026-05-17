from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class UserRegister(BaseModel):
    email: str = Field(..., min_length=3, max_length=254)
    password: str = Field(..., min_length=8, max_length=128)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        e = v.strip().lower()
        if "@" not in e:
            raise ValueError("Invalid email.")
        return e


class UserLogin(BaseModel):
    email: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        return v.strip().lower()


class UserPublic(BaseModel):
    id: str
    email: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user: UserPublic


class RefreshRequest(BaseModel):
    refresh_token: str = Field(..., min_length=1)


class ChatSessionSummary(BaseModel):
    session_id: str
    created_at: datetime
    last_message_at: datetime | None
    message_count: int


class ChatRequest(BaseModel):
    """Khi gửi header Authorization: Bearer <JWT>, user_id trong body bị bỏ qua."""

    message: str = Field(..., min_length=1)
    user_id: str = Field(default="anonymous")
    session_id: str | None = None


class ChatResponse(BaseModel):
    session_id: str
    user_message: str
    assistant_message: str


class ChatMessage(BaseModel):
    role: str
    content: str
    created_at: datetime


class ChatHistoryResponse(BaseModel):
    session_id: str
    messages: list[ChatMessage]

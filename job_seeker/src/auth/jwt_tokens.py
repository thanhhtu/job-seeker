from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal

import jwt

from src.core.config import settings

TokenType = Literal["access", "refresh"]
TOKEN_TYPE_ACCESS: TokenType = "access"
TOKEN_TYPE_REFRESH: TokenType = "refresh"


def _encode(*, payload: dict[str, object]) -> str:
    return jwt.encode(
        payload,
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )


def create_access_token(*, user_id: str, email: str) -> tuple[str, int]:
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=settings.jwt_expire_minutes)
    payload = {
        "sub": user_id,
        "email": email,
        "type": TOKEN_TYPE_ACCESS,
        "iat": now,
        "exp": expire,
    }
    token = _encode(payload=payload)
    expires_in = int((expire - now).total_seconds())
    return token, expires_in


def create_refresh_token(*, user_id: str, email: str) -> str:
    now = datetime.now(timezone.utc)
    expire = now + timedelta(days=settings.jwt_refresh_expire_days)
    payload = {
        "sub": user_id,
        "email": email,
        "type": TOKEN_TYPE_REFRESH,
        "iat": now,
        "exp": expire,
    }
    return _encode(payload=payload)


def decode_access_token(token: str) -> dict[str, object]:
    return _decode_token(token, expected_type=TOKEN_TYPE_ACCESS)


def decode_refresh_token(token: str) -> dict[str, object]:
    return _decode_token(token, expected_type=TOKEN_TYPE_REFRESH)


def _decode_token(token: str, *, expected_type: TokenType) -> dict[str, object]:
    payload = jwt.decode(
        token,
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
    )
    token_type = payload.get("type")
    if token_type != expected_type:
        raise jwt.InvalidTokenError("Unexpected token type")
    return payload

from __future__ import annotations

import jwt
from fastapi import Depends, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.api.auth.jwt_tokens import decode_access_token
from src.api.errors import ErrorCode, api_error
from src.users.repository import UserRecord, get_user_by_id

bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user_from_credentials(
    credentials: HTTPAuthorizationCredentials,
) -> UserRecord:
    if credentials.scheme.lower() != "bearer":
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.NOT_AUTHENTICATED,
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = credentials.credentials
    try:
        payload = decode_access_token(token)
    except jwt.ExpiredSignatureError:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.TOKEN_EXPIRED,
            headers={"WWW-Authenticate": "Bearer"},
        ) from None
    except jwt.InvalidTokenError:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.INVALID_TOKEN,
            headers={"WWW-Authenticate": "Bearer"},
        ) from None

    sub = payload.get("sub")
    if not isinstance(sub, str) or not sub:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.INVALID_TOKEN_SUBJECT,
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = await get_user_by_id(sub)
    if user is None:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.USER_NOT_FOUND,
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> UserRecord:
    if credentials is None:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.NOT_AUTHENTICATED,
            headers={"WWW-Authenticate": "Bearer"},
        )
    return await get_current_user_from_credentials(credentials)


async def optional_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> UserRecord | None:
    if credentials is None:
        return None
    return await get_current_user_from_credentials(credentials)

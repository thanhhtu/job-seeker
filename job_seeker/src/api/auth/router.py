from __future__ import annotations

import asyncpg
import jwt as pyjwt
from fastapi import APIRouter, Depends, status

from src.api.auth.deps import get_current_user
from src.api.errors import ErrorCode, api_error
from src.api.auth.jwt_tokens import (
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
)
from src.api.auth.passwords import hash_password, verify_password
from src.api.auth.schemas import (
    RefreshRequest,
    TokenResponse,
    UserLogin,
    UserPublic,
    UserRegister,
)
from src.users.repository import UserRecord, create_user, get_user_by_email, get_user_by_id

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _token_bundle(user: UserRecord) -> TokenResponse:
    access_token, expires_in = create_access_token(user_id=user.id, email=user.email)
    refresh_token = create_refresh_token(user_id=user.id, email=user.email)
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=expires_in,
        user=UserPublic(id=user.id, email=user.email),
    )


@router.post(
    "/register",
    response_model=TokenResponse,
    summary="Register",
    description="Create an account and immediately return a JWT.",
)
async def register(payload: UserRegister) -> TokenResponse:
    email = payload.email.strip().lower()
    pwd_hash = hash_password(payload.password)
    try:
        user = await create_user(email=email, password_hash=pwd_hash)
    except asyncpg.UniqueViolationError as exc:
        raise api_error(
            status.HTTP_409_CONFLICT,
            ErrorCode.EMAIL_ALREADY_REGISTERED,
        ) from exc
    return _token_bundle(user)


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Login",
    description="Return a JWT if the email/password is correct.",
)
async def login(payload: UserLogin) -> TokenResponse:
    email = payload.email.strip().lower()
    row = await get_user_by_email(email)
    if row is None:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.INVALID_CREDENTIALS,
        )
    user, stored_hash = row
    if not verify_password(payload.password, stored_hash):
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.INVALID_CREDENTIALS,
        )
    return _token_bundle(user)


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Refresh access token",
    description="Exchange a valid refresh token for a new access token and refresh token.",
)
async def refresh_tokens(payload: RefreshRequest) -> TokenResponse:
    try:
        claims = decode_refresh_token(payload.refresh_token.strip())
    except pyjwt.ExpiredSignatureError:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.REFRESH_TOKEN_EXPIRED,
        ) from None
    except pyjwt.InvalidTokenError:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.INVALID_REFRESH_TOKEN,
        ) from None

    sub = claims.get("sub")
    if not isinstance(sub, str) or not sub:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.INVALID_REFRESH_TOKEN,
        )

    user = await get_user_by_id(sub)
    if user is None:
        raise api_error(
            status.HTTP_401_UNAUTHORIZED,
            ErrorCode.USER_NOT_FOUND,
        )

    return _token_bundle(user)


@router.get(
    "/me",
    response_model=UserPublic,
    summary="Current user information",
    description="Requires the header `Authorization: Bearer <JWT>`.",
)
async def auth_me(user: UserRecord = Depends(get_current_user)) -> UserPublic:
    return UserPublic(id=user.id, email=user.email)

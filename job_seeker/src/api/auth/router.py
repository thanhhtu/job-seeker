from __future__ import annotations

import asyncpg
import jwt as pyjwt
from fastapi import APIRouter, Depends, Response, status

from src.api.auth.deps import get_current_user
from src.api.errors import ErrorCode, api_error
from src.api.auth.jwt_tokens import (
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
)
from src.api.auth.passwords import hash_password, verify_password
from src.api.auth.schemas import (
    ChangePasswordRequest,
    RefreshRequest,
    TokenResponse,
    UpdateProfileRequest,
    UserLogin,
    UserPublic,
    UserRegister,
)
from src.db.repositories.user_repository import (
    UserRecord,
    create_user,
    get_password_hash,
    get_user_by_email,
    get_user_by_id,
    update_password_hash,
    update_user_profile,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _token_bundle(user: UserRecord) -> TokenResponse:
    access_token, expires_in = create_access_token(user_id=user.id, email=user.email)
    refresh_token = create_refresh_token(user_id=user.id, email=user.email)
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=expires_in,
        user=_user_public(user),
    )


def _user_public(user: UserRecord) -> UserPublic:
    return UserPublic(id=user.id, email=user.email, name=user.name, phone=user.phone)


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
    return _user_public(user)


@router.patch(
    "/me",
    response_model=UserPublic,
    summary="Update profile",
    description="Update the current user's name and phone. Email cannot be changed.",
)
async def update_profile(
    payload: UpdateProfileRequest,
    user: UserRecord = Depends(get_current_user),
) -> UserPublic:
    updated = await update_user_profile(user.id, name=payload.name, phone=payload.phone)
    if updated is None:
        raise api_error(status.HTTP_401_UNAUTHORIZED, ErrorCode.USER_NOT_FOUND)
    return _user_public(updated)


@router.patch(
    "/me/password",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Change password",
    description="Change the current user's password after verifying the current one.",
)
async def change_password(
    payload: ChangePasswordRequest,
    user: UserRecord = Depends(get_current_user),
) -> Response:
    stored_hash = await get_password_hash(user.id)
    if stored_hash is None:
        raise api_error(status.HTTP_401_UNAUTHORIZED, ErrorCode.USER_NOT_FOUND)
    if not verify_password(payload.current_password, stored_hash):
        raise api_error(
            status.HTTP_400_BAD_REQUEST,
            ErrorCode.INVALID_CURRENT_PASSWORD,
        )
    new_hash = hash_password(payload.new_password)
    await update_password_hash(user.id, new_hash)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

from __future__ import annotations

from enum import StrEnum
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel

# Single source: (message_code, default English message)
API_ERRORS: list[tuple[str, str]] = [
    ("EMAIL_ALREADY_REGISTERED", "Email already registered."),
    ("INVALID_CREDENTIALS", "Incorrect email or password."),
    ("INVALID_CURRENT_PASSWORD", "Current password is incorrect."),
    ("REFRESH_TOKEN_EXPIRED", "Refresh token expired"),
    ("INVALID_REFRESH_TOKEN", "Invalid refresh token"),
    ("USER_NOT_FOUND", "User no longer exists"),
    ("NOT_AUTHENTICATED", "Not authenticated"),
    ("TOKEN_EXPIRED", "Token expired"),
    ("INVALID_TOKEN", "Invalid token"),
    ("INVALID_TOKEN_SUBJECT", "Invalid token subject"),
    ("MESSAGE_EMPTY", "Message must not be empty."),
    ("SESSION_FORBIDDEN", "Session does not belong to user."),
    ("SESSION_NOT_FOUND", "Session not found."),
    ("SESSION_ACCESS_DENIED", "You do not have access to this session."),
    ("TITLE_EMPTY", "Title must not be empty."),
    ("JOB_NOT_FOUND", "Job not found."),
    ("SAVED_JOB_NOT_FOUND", "This job is not in your saved list."),
    ("INVALID_JOB_STATUS", "Invalid saved-job status."),
    ("INVALID_EMAIL", "Invalid email."),
    ("PASSWORD_TOO_SHORT", "Password must be at least 8 characters."),
    ("FIELD_REQUIRED", "Please fill in all required fields."),
    ("VALIDATION_ERROR", "Validation failed."),
    ("UNKNOWN_ERROR", "An error occurred."),
]

ErrorCode = StrEnum("ErrorCode", {code: code for code, _ in API_ERRORS})

DEFAULT_MESSAGES: dict[str, str] = dict(API_ERRORS)


class ApiErrorBody(BaseModel):
    message_code: str
    message: str


def error_detail(
    code: ErrorCode | str,
    message: str | None = None,
) -> dict[str, str]:
    code_str = str(code)
    if message is not None:
        return {"message_code": code_str, "message": message}
    default_msg = DEFAULT_MESSAGES.get(code_str, DEFAULT_MESSAGES[ErrorCode.UNKNOWN_ERROR])
    return {"message_code": code_str, "message": default_msg}


def api_error(
    status_code: int,
    code: ErrorCode | str,
    message: str | None = None,
    *,
    headers: dict[str, str] | None = None,
) -> HTTPException:
    kwargs: dict[str, Any] = {"status_code": status_code, "detail": error_detail(code, message)}
    if headers:
        kwargs["headers"] = headers
    return HTTPException(**kwargs)


def parse_validation_errors(errors: list[dict[str, Any]]) -> ApiErrorBody:
    if not errors:
        return ApiErrorBody(
            message_code=ErrorCode.VALIDATION_ERROR,
            message=DEFAULT_MESSAGES[ErrorCode.VALIDATION_ERROR],
        )

    first = errors[0]
    msg = str(first.get("msg", ""))
    lowered = msg.lower()

    if "invalid email" in lowered:
        code = ErrorCode.INVALID_EMAIL
    elif "at least 8 character" in lowered:
        code = ErrorCode.PASSWORD_TOO_SHORT
    elif "field required" in lowered or first.get("type") == "missing":
        code = ErrorCode.FIELD_REQUIRED
    else:
        code = ErrorCode.VALIDATION_ERROR

    return ApiErrorBody(
        message_code=str(code),
        message=msg or DEFAULT_MESSAGES[code],
    )

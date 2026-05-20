"""Metadata for OpenAPI / Swagger UI (/docs) and ReDoc (/redoc)."""

OPENAPI_TAGS: list[dict[str, str]] = [
    {
        "name": "health",
        "description": "Check whether the API is available and healthy.",
    },
    {
        "name": "auth",
        "description": "Register, log in, and obtain JWT access tokens (Bearer).",
    },
    {
        "name": "me",
        "description": (
            "Information and resources associated with the authenticated user "
            "(Bearer required)."
        ),
    },
    {
        "name": "chat",
        "description": (
            "Send messages to the job-search assistant. "
            "You may include `Authorization: Bearer <JWT>` to bind the session "
            "to an authenticated account; without a token, the API uses `user_id` "
            "from the request body (guest flow)."
        ),
    },
    {
        "name": "history",
        "description": (
            "Retrieve message history for a session. **JWT required**; "
            "only the session owner can access it."
        ),
    },
]

APP_DESCRIPTION = """
## Job Seeker API

Chat backend for job searching, with **JWT authentication** for registered users.

### Interactive Documentation

- **Swagger UI:** [`/docs`](/docs)

- **ReDoc:** [`/redoc`](/redoc)

- **OpenAPI JSON:** [`/openapi.json`](/openapi.json)

### Authentication

1. Call `POST /api/auth/register` or `POST /api/auth/login` to receive an `access_token`.

2. In Swagger, click **Authorize**, choose the **HTTPBearer** scheme, and paste the `access_token` (do not include the `Bearer ` prefix — Swagger adds it automatically).

3. Protected endpoints will send the header `Authorization: Bearer <token>`.

### Suggested Flow

- Authenticated chat: Authorize → `POST /api/chat` (you may omit `user_id` in the request body).

- View sessions: `GET /api/me/chat-sessions` → `GET /api/sessions/{session_id}/messages`.
"""

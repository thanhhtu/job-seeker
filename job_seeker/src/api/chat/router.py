from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Request
from langchain_core.messages import HumanMessage

from src.api.auth.deps import optional_current_user
from src.api.errors import ErrorCode, api_error
from src.api.chat.schemas import AssistantData, ChatRequest, ChatResponse
from src.chat_history.store import ChatHistoryStore
from src.users.repository import UserRecord

router = APIRouter(prefix="/api", tags=["chat"])
_store = ChatHistoryStore()


@router.post(
    "/chat",
    response_model=ChatResponse,
    summary="Chat with the agent",
    description=(
        "If the request includes the header `Authorization: Bearer <JWT>`, "
        "the message is associated with the authenticated user and `user_id` "
        "in the request body is ignored; chat history is stored in `chat_messages`. "
        "Without a token: guest flow (`user_id` + `session_id`, `chat_sessions.is_guest=true`) — "
        "LangGraph checkpoints still use `session_id`, but **no** rows are written "
        "to `chat_messages`."
    ),
)
async def chat(
    request: Request,
    payload: ChatRequest,
    auth_user: UserRecord | None = Depends(optional_current_user),
) -> ChatResponse:
    message = payload.message.strip()
    if not message:
        raise api_error(400, ErrorCode.MESSAGE_EMPTY)

    if auth_user is not None:
        effective_user_id = auth_user.id
    else:
        effective_user_id = (payload.user_id or "anonymous").strip() or "anonymous"

    try:
        session_id = await _store.ensure_session(
            effective_user_id,
            payload.session_id,
            is_guest=auth_user is None,
            adopt_client_session_id=auth_user is None,
        )
    except ValueError as exc:
        raise api_error(403, ErrorCode.SESSION_FORBIDDEN, str(exc)) from exc

    graph = request.app.state.graph
    config = {"configurable": {"thread_id": session_id}}

    result = await graph.ainvoke({"messages": [HumanMessage(content=message)]}, config)
    raw_output = (result.get("output") or "").strip()

    data: AssistantData | None = None
    assistant_message = raw_output
    try:
        parsed = json.loads(raw_output)
        if isinstance(parsed, dict) and "type" in parsed:
            data = AssistantData(**parsed)
            assistant_message = data.message or data.match_summary or raw_output
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    if not assistant_message:
        assistant_message = "I could not generate a response. Please try again."

    if auth_user is not None:
        await _store.add_message(session_id=session_id, role="user", content=message)
        await _store.set_session_title_if_empty(session_id, message)
        await _store.add_message(
            session_id=session_id,
            role="assistant",
            content=assistant_message,
            data=data.model_dump(exclude_none=True) if data else None,
        )

    return ChatResponse(
        session_id=session_id,
        user_message=message,
        assistant_message=assistant_message,
        data=data,
    )

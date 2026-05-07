from __future__ import annotations

import os

import chainlit as cl
import httpx

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8080")
CHAT_API_URL = f"{BACKEND_URL.rstrip('/')}/api/chat"


@cl.on_chat_start
async def on_chat_start() -> None:
    cl.user_session.set("session_id", None)
    cl.user_session.set("user_id", "chainlit_user")
    await cl.Message(
        content="Chao ban! Hay mo ta vi tri, ky nang, dia diem de minh tim job phu hop."
    ).send()


@cl.on_message
async def on_message(message: cl.Message) -> None:
    content = (message.content or "").strip()
    if not content:
        await cl.Message(
            content="Ban chua nhap noi dung. Hay nhap vi tri hoac ky nang ban muon tim."
        ).send()
        return

    payload = {
        "message": content,
        "user_id": cl.user_session.get("user_id"),
        "session_id": cl.user_session.get("session_id"),
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(CHAT_API_URL, json=payload)
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        await cl.Message(content=f"Backend error: {detail}").send()
        return
    except Exception as exc:  # noqa: BLE001
        await cl.Message(content=f"Cannot connect backend: {exc}").send()
        return

    cl.user_session.set("session_id", data["session_id"])
    await cl.Message(content=data["assistant_message"]).send()

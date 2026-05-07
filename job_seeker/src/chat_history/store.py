from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any
from uuid import uuid4


class ChatHistoryStore:
    def __init__(self, db_path: str = "data/chat_history.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_sessions (
                    session_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (session_id) REFERENCES chat_sessions(session_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_session ON chat_messages(session_id, id)"
            )

    def create_session(self, user_id: str) -> str:
        session_id = str(uuid4())
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO chat_sessions(session_id, user_id) VALUES (?, ?)",
                (session_id, user_id),
            )
        return session_id

    def ensure_session(self, user_id: str, session_id: str | None = None) -> str:
        if not session_id:
            return self.create_session(user_id)

        with self._connect() as conn:
            row = conn.execute(
                "SELECT user_id FROM chat_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO chat_sessions(session_id, user_id) VALUES (?, ?)",
                    (session_id, user_id),
                )
                return session_id
            if row["user_id"] != user_id:
                raise ValueError("Session does not belong to user.")
        return session_id

    def add_message(self, session_id: str, role: str, content: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO chat_messages(session_id, role, content) VALUES (?, ?, ?)",
                (session_id, role, content),
            )

    def get_messages(self, session_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, role, content, created_at
                FROM chat_messages
                WHERE session_id = ?
                ORDER BY id ASC
                """,
                (session_id,),
            ).fetchall()
        return [dict(row) for row in rows]

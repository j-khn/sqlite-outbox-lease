from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import math
import secrets
import sqlite3
import time


class LeaseLost(RuntimeError):
    pass


@dataclass(frozen=True)
class Message:
    id: int
    topic: str
    payload: bytes
    attempts: int
    lease_token: str


class SQLiteOutbox:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("use a file path so workers share durable state")
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        connection = self._connection()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS outbox_messages ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL, "
                "payload BLOB NOT NULL, created_at REAL NOT NULL, "
                "available_at REAL NOT NULL, delivered_at REAL, locked_by TEXT, "
                "lease_until REAL, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS outbox_ready "
                "ON outbox_messages(delivered_at,available_at,lease_until,id)"
            )
        finally:
            connection.close()

    def _connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def enqueue(
        self,
        topic: str,
        payload: bytes,
        *,
        now: float | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> int:
        if not topic:
            raise ValueError("topic must not be empty")
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")
        current = self._current_time(now)
        statement = (
            "INSERT INTO outbox_messages(topic,payload,created_at,available_at) "
            "VALUES (?,?,?,?)"
        )
        if connection is not None:
            cursor = connection.execute(statement, (topic, payload, current, current))
            return int(cursor.lastrowid)

        owned = self._connection()
        try:
            cursor = owned.execute(statement, (topic, payload, current, current))
            return int(cursor.lastrowid)
        finally:
            owned.close()

    def claim(
        self,
        worker_id: str,
        *,
        limit: int = 20,
        lease_seconds: float = 30,
        now: float | None = None,
    ) -> list[Message]:
        if (
            not worker_id or type(limit) is not int or limit < 1
            or lease_seconds <= 0 or not math.isfinite(lease_seconds)
        ):
            raise ValueError("worker_id, positive limit, and finite positive lease are required")
        current = self._current_time(now)
        lease_until = self._deadline(current, lease_seconds)
        connection = self._connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT id,topic,payload,attempts FROM outbox_messages "
                "WHERE delivered_at IS NULL AND available_at <= ? "
                "AND (lease_until IS NULL OR lease_until <= ?) "
                "ORDER BY id LIMIT ?",
                (current, current, limit),
            ).fetchall()
            messages: list[Message] = []
            for message_id, topic, payload, attempts in rows:
                lease_token = worker_id + ":" + secrets.token_urlsafe(18)
                cursor = connection.execute(
                    "UPDATE outbox_messages SET locked_by=?,lease_until=?,attempts=attempts+1 "
                    "WHERE id=? AND delivered_at IS NULL "
                    "AND (lease_until IS NULL OR lease_until <= ?)",
                    (lease_token, lease_until, message_id, current),
                )
                if cursor.rowcount == 1:
                    messages.append(
                        Message(message_id, topic, bytes(payload), attempts + 1, lease_token)
                    )
            connection.commit()
            return messages
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def ack(self, message_id: int, lease_token: str, *, now: float | None = None) -> None:
        current = self._current_time(now)
        self._owned_update(
            "UPDATE outbox_messages SET delivered_at=?,locked_by=NULL,lease_until=NULL "
            "WHERE id=? AND locked_by=? AND delivered_at IS NULL AND lease_until > ?",
            (current, message_id, lease_token, current),
        )

    def retry(
        self,
        message_id: int,
        lease_token: str,
        *,
        delay_seconds: float,
        error: str,
        now: float | None = None,
    ) -> None:
        if delay_seconds < 0 or not math.isfinite(delay_seconds):
            raise ValueError("delay_seconds must be finite and non-negative")
        current = self._current_time(now)
        available_at = self._deadline(current, delay_seconds)
        self._owned_update(
            "UPDATE outbox_messages SET available_at=?,last_error=?,locked_by=NULL,lease_until=NULL "
            "WHERE id=? AND locked_by=? AND delivered_at IS NULL AND lease_until > ?",
            (available_at, error[:2000], message_id, lease_token, current),
        )

    def extend(
        self,
        message_id: int,
        lease_token: str,
        *,
        lease_seconds: float,
        now: float | None = None,
    ) -> None:
        if lease_seconds <= 0 or not math.isfinite(lease_seconds):
            raise ValueError("lease_seconds must be finite and positive")
        current = self._current_time(now)
        lease_until = self._deadline(current, lease_seconds)
        self._owned_update(
            "UPDATE outbox_messages SET lease_until=? "
            "WHERE id=? AND locked_by=? AND delivered_at IS NULL AND lease_until > ?",
            (lease_until, message_id, lease_token, current),
        )

    @staticmethod
    def _current_time(now: float | None) -> float:
        current = time.time() if now is None else now
        if not math.isfinite(current):
            raise ValueError("now must be finite")
        return current

    @staticmethod
    def _deadline(current: float, duration: float) -> float:
        deadline = current + duration
        if not math.isfinite(deadline):
            raise ValueError("scheduled time must be finite")
        return deadline

    def _owned_update(self, statement: str, parameters: tuple[object, ...]) -> None:
        connection = self._connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(statement, parameters)
            if cursor.rowcount != 1:
                raise LeaseLost("message is missing, delivered, or held by another worker")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

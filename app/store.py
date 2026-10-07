"""SQLite-backed job store."""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id         TEXT PRIMARY KEY,
    status         TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    policy_json    TEXT NOT NULL,
    redactions_json TEXT,
    input_sha256   TEXT NOT NULL,
    secret_fp      TEXT,
    result_path    TEXT,
    result_sha256  TEXT,
    audit_json     TEXT,
    errors_json    TEXT
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class JobStore:
    def __init__(self, db_path: str) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """Upgrade databases created before secret-scoped dedup.

        Pre-rotation rows have no fingerprint. They remain queryable/downloadable
        by their job id, but ``secret_fp IS NULL`` means a fingerprinted lookup
        can never match them, so a legacy result can never be handed out as the
        product of a different secret.
        """
        columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(jobs)")}
        if "secret_fp" not in columns:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN secret_fp TEXT")

    def create(
        self,
        job_id: str,
        policy: dict,
        redactions: Any,
        input_sha256: str,
        secret_fp: str,
    ) -> None:
        now = _utcnow()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO jobs (job_id, status, created_at, updated_at, policy_json,"
                " redactions_json, input_sha256, secret_fp) VALUES (?, 'queued', ?, ?, ?, ?, ?, ?)",
                (
                    job_id,
                    now,
                    now,
                    json.dumps(policy),
                    json.dumps(redactions) if redactions is not None else None,
                    input_sha256,
                    secret_fp,
                ),
            )

    def get(self, job_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row) if row else None

    def find_scoped(self, job_id: str, secret_fp: str) -> Optional[dict]:
        """Return an existing job only if it was produced under the same secret.

        Matching requires an explicit fingerprint equality; legacy rows with
        ``secret_fp IS NULL`` never match, so they cannot be mistaken for the
        current secret's result even in the face of a truncated-id collision.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ? AND secret_fp = ?",
                (job_id, secret_fp),
            ).fetchone()
        return dict(row) if row else None

    def _update(self, job_id: str, **fields: Any) -> None:
        fields["updated_at"] = _utcnow()
        clause = ", ".join(f"{k} = ?" for k in fields)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE jobs SET {clause} WHERE job_id = ?", (*fields.values(), job_id)
            )

    def set_processing(self, job_id: str) -> None:
        self._update(job_id, status="processing")

    def set_failed(self, job_id: str, errors: list) -> None:
        self._update(job_id, status="failed", errors_json=json.dumps(errors))

    def set_done(self, job_id: str, result_path: str, result_sha256: str, audit: dict) -> None:
        self._update(
            job_id,
            status="done",
            result_path=result_path,
            result_sha256=result_sha256,
            audit_json=json.dumps(audit),
        )

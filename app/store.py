"""SQLite-backed job store."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
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
    errors_json    TEXT,
    lease_owner    TEXT,
    lease_expires_at REAL
);
"""

#: Statuses from which a job can still be (re-)executed. Terminal statuses
#: (done / failed) are never taken over.
RESUMABLE_STATUSES = ("queued", "processing")


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
        """Upgrade databases created before secret-scoped dedup / leases.

        Pre-rotation rows have no fingerprint. They remain queryable/downloadable
        by their job id, but ``secret_fp IS NULL`` means a fingerprinted lookup
        can never match them, so a legacy result can never be handed out as the
        product of a different secret.

        Rows written before lease tracking have ``lease_owner`` /
        ``lease_expires_at`` NULL, i.e. no live executor — an unfinished legacy
        row is therefore immediately claimable by a resubmission or by startup
        recovery.
        """
        columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(jobs)")}
        if "secret_fp" not in columns:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN secret_fp TEXT")
        if "lease_owner" not in columns:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN lease_owner TEXT")
        if "lease_expires_at" not in columns:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN lease_expires_at REAL")

    def create(
        self,
        job_id: str,
        policy: dict,
        redactions: Any,
        input_sha256: str,
        secret_fp: str,
        lease_owner: Optional[str] = None,
        lease_expires_at: Optional[float] = None,
    ) -> bool:
        """Insert a new queued job. Returns False if the id already exists.

        ``INSERT OR IGNORE`` makes concurrent first submissions of the same
        payload safe across processes: exactly one of them inserts (and therefore
        owns the execution lease), the others fall back to the existing row.
        """
        now = _utcnow()
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO jobs (job_id, status, created_at, updated_at, policy_json,"
                " redactions_json, input_sha256, secret_fp, lease_owner, lease_expires_at)"
                " VALUES (?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id,
                    now,
                    now,
                    json.dumps(policy),
                    json.dumps(redactions) if redactions is not None else None,
                    input_sha256,
                    secret_fp,
                    lease_owner,
                    lease_expires_at,
                ),
            )
        return cur.rowcount == 1

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

    def list_resumable(self) -> list[dict]:
        """All jobs that are not in a terminal state (queued / processing)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status IN ('queued', 'processing') ORDER BY created_at"
            ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Lease handling: cross-process arbitration of execution rights
    # ------------------------------------------------------------------
    def claim(self, job_id: str, owner: str, expires_at: float, now: Optional[float] = None) -> bool:
        """Atomically take over an unfinished job whose lease is absent/expired.

        A single ``UPDATE`` guarded by the lease predicate is the arbitration
        point: under SQLite's write serialisation exactly one concurrent
        claimant can observe ``rowcount == 1``. A live executor keeps its lease
        ahead of ``now`` by renewing, so it is never claimed underneath it.
        """
        now = time.time() if now is None else now
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE jobs SET lease_owner = ?, lease_expires_at = ?"
                " WHERE job_id = ? AND status IN ('queued', 'processing')"
                " AND (lease_expires_at IS NULL OR lease_expires_at < ?)",
                (owner, expires_at, job_id, now),
            )
        return cur.rowcount == 1

    def force_claim(
        self,
        job_id: str,
        owner: str,
        expires_at: float,
        expected_owner: Optional[str],
        expected_expires_at: Optional[float],
    ) -> bool:
        """Compare-and-swap takeover of a lease that has not moved.

        Used by startup recovery: a lease snapshot is taken at boot, and a job
        whose lease is *unchanged* after a full TTL demonstrably has no live
        executor renewing it (its process is gone). The CAS predicate makes the
        takeover atomic against any other process doing the same check; a live
        executor either renewed first (snapshot mismatch) or loses its next
        renewal and backs off.
        """
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE jobs SET lease_owner = ?, lease_expires_at = ?"
                " WHERE job_id = ? AND status IN ('queued', 'processing')"
                " AND lease_owner IS ? AND lease_expires_at IS ?",
                (owner, expires_at, job_id, expected_owner, expected_expires_at),
            )
        return cur.rowcount == 1

    def renew_lease(self, job_id: str, owner: str, expires_at: float) -> bool:
        """Extend the lease; only the current owner may renew. Returns False
        (without touching the row) once the lease has been taken over, which is
        how a displaced executor learns it must stop."""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE jobs SET lease_expires_at = ?"
                " WHERE job_id = ? AND lease_owner = ? AND status IN ('queued', 'processing')",
                (expires_at, job_id, owner),
            )
        return cur.rowcount == 1

    # ------------------------------------------------------------------
    # Status transitions
    # ------------------------------------------------------------------
    def _update(self, job_id: str, owner: Optional[str] = None, **fields: Any) -> bool:
        fields["updated_at"] = _utcnow()
        clause = ", ".join(f"{k} = ?" for k in fields)
        sql = f"UPDATE jobs SET {clause} WHERE job_id = ?"
        params: list[Any] = [*fields.values(), job_id]
        if owner is not None:
            # Lease-guarded transition: a displaced executor must not move the
            # record (e.g. flip a finished job back to processing).
            sql += " AND lease_owner = ?"
            params.append(owner)
        with self._lock, self._conn:
            cur = self._conn.execute(sql, params)
        return cur.rowcount == 1

    def set_processing(self, job_id: str, owner: Optional[str] = None) -> bool:
        return self._update(job_id, owner, status="processing")

    def set_failed(self, job_id: str, errors: list, owner: Optional[str] = None) -> bool:
        # Terminal state: release the lease so nothing tries to take it over.
        return self._update(
            job_id,
            owner,
            status="failed",
            errors_json=json.dumps(errors),
            lease_owner=None,
            lease_expires_at=None,
        )

    def set_done(self, job_id: str, result_path: str, result_sha256: str, audit: dict,
                 owner: Optional[str] = None) -> bool:
        # Terminal state: release the lease so nothing tries to take it over.
        return self._update(
            job_id,
            owner,
            status="done",
            result_path=result_path,
            result_sha256=result_sha256,
            audit_json=json.dumps(audit),
            lease_owner=None,
            lease_expires_at=None,
        )

"""Background job execution and crash recovery.

Execution model
---------------
Every execution (initial run or recovery) holds a *lease* on the job row:
``lease_owner`` identifies the executing process, ``lease_expires_at`` is
kept in the near future by a renewal thread while the pipeline runs. This
makes "is the old task still running?" answerable across processes:

* a live executor's lease never expires (it renews at TTL/3 intervals);
* a crashed executor's lease stops moving and can be taken over — atomically,
  via :meth:`JobStore.claim` (expired lease) or :meth:`JobStore.force_claim`
  (lease unchanged since a boot-time snapshot, i.e. the owner process is gone).

Recovery always re-runs the job with inputs that match the *record*: the
policy/redactions come from the persisted row, and the ZIP bytes (from the
resubmission body or the persisted input archive) must hash to the recorded
``input_sha256``. The result is published atomically (temp file + ``os.replace``)
and only then marked ``done``, so an interrupted write can neither expose a
half-written archive nor mark the job done prematurely.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
import uuid
from typing import Any, Optional

from .engine import anonymize_zip
from .store import RESUMABLE_STATUSES, JobStore

#: Default lease TTL; the renewal thread fires at TTL/3 intervals so a live
#: executor's lease never lapses. Configurable via Settings.lease_ttl_seconds.
DEFAULT_LEASE_TTL = 30.0


def new_owner_token() -> str:
    """Identify one executor: host, pid and a random nonce."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Persisted input archives
# ---------------------------------------------------------------------------
def input_path(inputs_dir: str, job_id: str) -> str:
    return os.path.join(inputs_dir, f"{job_id}.zip")


def write_input_atomic(inputs_dir: str, job_id: str, content: bytes) -> str:
    """Persist the uploaded ZIP so the job can be recovered after a crash.

    Written via a uniquely named temp file + ``os.replace``: a reader either
    sees the complete archive or nothing, and concurrent first submissions of
    the same payload (identical bytes, by construction of the job id) never
    observe a truncated file.
    """
    tmp = os.path.join(inputs_dir, f".{job_id}.{uuid.uuid4().hex}.tmp")
    with open(tmp, "wb") as fh:
        fh.write(content)
        fh.flush()
        os.fsync(fh.fileno())
    final = input_path(inputs_dir, job_id)
    os.replace(tmp, final)
    return final


def read_input(inputs_dir: str, job_id: str) -> Optional[bytes]:
    try:
        with open(input_path(inputs_dir, job_id), "rb") as fh:
            return fh.read()
    except OSError:
        return None


def cleanup_stale_tmp_files(*dirs: str) -> None:
    """Remove temp files left behind by interrupted atomic writes.

    Result archives are published via ``os.replace`` and inputs likewise, so a
    ``.*.tmp`` leftover can only come from a crash mid-write; it is never a
    valid result and must not be mistaken for one.
    """
    for directory in dirs:
        try:
            names = os.listdir(directory)
        except OSError:
            continue
        for name in names:
            if name.startswith(".") and name.endswith(".tmp"):
                try:
                    os.unlink(os.path.join(directory, name))
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
def process_job(
    store: JobStore,
    results_dir: str,
    secret: str,
    job_id: str,
    zip_bytes: bytes,
    policy: dict,
    redactions: Optional[list],
    owner: Optional[str] = None,
    lease_ttl: float = DEFAULT_LEASE_TTL,
) -> None:
    """Run the anonymization pipeline; persist outcome in the job store.

    The pipeline is all-or-nothing: the result file is only written after the
    whole archive has been transformed successfully, so a failed job never
    leaves partial output behind. The result is published atomically (temp
    file + ``os.replace``) *before* the row is marked done, so a crash
    anywhere in between can neither expose a half-written result nor mark the
    job done prematurely — a later recovery simply re-runs and republishes the
    deterministic output.

    When ``owner`` is given, every state transition is guarded by the lease:
    if the lease was taken over meanwhile (another executor won the
    arbitration), this run stops touching the record — the new owner produces
    the byte-identical result and final state.
    """
    stop = threading.Event()
    lost = threading.Event()
    renew_thread: Optional[threading.Thread] = None
    if owner is not None:
        interval = min(max(lease_ttl / 3.0, 0.05), 10.0)

        def _renew() -> None:
            while not stop.wait(interval):
                try:
                    ok = store.renew_lease(job_id, owner, time.time() + lease_ttl)
                except Exception:
                    ok = True  # transient failure: retry next round
                if not ok:
                    lost.set()
                    return

        renew_thread = threading.Thread(
            target=_renew, name=f"lease-renew-{job_id[:8]}", daemon=True
        )
        renew_thread.start()
    try:
        if not store.set_processing(job_id, owner=owner):
            return  # lease was taken over before we even started
        try:
            errors, result, audit, _warnings = anonymize_zip(zip_bytes, policy, redactions, secret)
        except Exception as exc:  # defensive: report, never emit partial output
            store.set_failed(
                job_id,
                [{"file": None, "code": "INTERNAL_ERROR", "message": f"{type(exc).__name__}: {exc}"}],
                owner=owner,
            )
            return
        if errors:
            store.set_failed(job_id, errors, owner=owner)
            return
        assert result is not None and audit is not None
        # Publish atomically: write to a temp file, fsync, then rename onto the
        # final path. A crash leaves either the old file or the new one, never
        # a truncated archive at the result path.
        tmp_path = os.path.join(results_dir, f".{job_id}.{uuid.uuid4().hex}.tmp")
        with open(tmp_path, "wb") as fh:
            fh.write(result)
            fh.flush()
            os.fsync(fh.fileno())
        final_path = os.path.join(results_dir, f"{job_id}.zip")
        os.replace(tmp_path, final_path)
        if lost.is_set():
            # Our lease was taken over mid-run; the new owner publishes the
            # (deterministic, byte-identical) result and the final state.
            return
        store.set_done(job_id, final_path, hashlib.sha256(result).hexdigest(), audit, owner=owner)
    finally:
        stop.set()
        if renew_thread is not None:
            renew_thread.join(timeout=2.0)


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------
def recover_claimed_record(
    store: JobStore, record: dict, owner: str, zip_bytes: Optional[bytes]
) -> Optional[tuple[bytes, dict, Optional[list]]]:
    """Prepare execution inputs for a job whose lease we now hold.

    The inputs are taken from the *record*, not from the triggering request:
    policy/redactions are the persisted JSON, and the ZIP bytes must hash to
    the recorded ``input_sha256``. Returns ``(zip_bytes, policy, redactions)``,
    or ``None`` after failing the job when the input is unavailable or does
    not match the record (a permanent condition — the job must not sit in
    ``queued`` forever).
    """
    job_id = record["job_id"]
    if zip_bytes is None or hashlib.sha256(zip_bytes).hexdigest() != record["input_sha256"]:
        store.set_failed(
            job_id,
            [
                {
                    "file": None,
                    "code": "INPUT_UNAVAILABLE",
                    "message": (
                        "the persisted input archive is missing or does not match the"
                        " recorded input_sha256; the interrupted job cannot be recovered"
                    ),
                }
            ],
            owner=owner,
        )
        return None
    policy = json.loads(record["policy_json"])
    redactions = json.loads(record["redactions_json"]) if record["redactions_json"] else None
    return zip_bytes, policy, redactions


def _launch_thread(
    store: JobStore,
    results_dir: str,
    secret: str,
    job_id: str,
    recovered: tuple[bytes, dict, Optional[list]],
    owner: str,
    lease_ttl: float,
) -> threading.Thread:
    zip_bytes, policy, redactions = recovered
    thread = threading.Thread(
        target=process_job,
        args=(store, results_dir, secret, job_id, zip_bytes, policy, redactions),
        kwargs={"owner": owner, "lease_ttl": lease_ttl},
        name=f"job-resume-{job_id[:8]}",
        daemon=True,
    )
    thread.start()
    return thread


def resume_interrupted_jobs(
    store: JobStore,
    inputs_dir: str,
    results_dir: str,
    secret: str,
    lease_ttl: float = DEFAULT_LEASE_TTL,
    secret_fp: Optional[str] = None,
) -> list[threading.Thread]:
    """Take over unfinished jobs whose lease has expired and run them.

    Only jobs in the current secret scope (``secret_fp``) are eligible: a job
    created under a rotated-out key must never be re-executed under the new
    one. Returns the launched threads (mainly so tests can join them).
    """
    launched: list[threading.Thread] = []
    for record in store.list_resumable():
        if secret_fp is not None and record["secret_fp"] != secret_fp:
            continue
        job_id = record["job_id"]
        owner = new_owner_token()
        if not store.claim(job_id, owner, time.time() + lease_ttl):
            continue  # another (live) executor holds the lease
        recovered = recover_claimed_record(store, record, owner, read_input(inputs_dir, job_id))
        if recovered is None:
            continue
        launched.append(
            _launch_thread(store, results_dir, secret, job_id, recovered, owner, lease_ttl)
        )
    return launched


def snapshot_resumable_leases(store: JobStore, secret_fp: Optional[str] = None) -> dict:
    """``{job_id: (lease_owner, lease_expires_at)}`` for unfinished jobs.

    Taken at startup; a lease that is *unchanged* one full TTL later belongs
    to a dead process (a live executor renews well within a TTL).
    """
    return {
        record["job_id"]: (record["lease_owner"], record["lease_expires_at"])
        for record in store.list_resumable()
        if secret_fp is None or record["secret_fp"] == secret_fp
    }


def adopt_stale_jobs(
    store: JobStore,
    inputs_dir: str,
    results_dir: str,
    secret: str,
    lease_ttl: float,
    snapshot: dict,
    secret_fp: Optional[str] = None,
) -> list[threading.Thread]:
    """CAS-take over jobs whose lease has not moved since *snapshot*.

    Complements :func:`resume_interrupted_jobs` for the fast-restart case:
    when a process dies and is restarted within one lease TTL, the stale lease
    has not expired yet, but it also never renews. The compare-and-swap in
    :meth:`JobStore.force_claim` arbitrates against any other process doing
    the same check.
    """
    launched: list[threading.Thread] = []
    for job_id, (expected_owner, expected_expires) in snapshot.items():
        record = store.get(job_id)
        if record is None or record["status"] not in RESUMABLE_STATUSES:
            continue
        if secret_fp is not None and record["secret_fp"] != secret_fp:
            continue
        if (record["lease_owner"], record["lease_expires_at"]) != (expected_owner, expected_expires):
            continue  # lease moved meanwhile: a live executor is on it
        owner = new_owner_token()
        if not store.force_claim(
            job_id, owner, time.time() + lease_ttl, expected_owner, expected_expires
        ):
            continue
        recovered = recover_claimed_record(store, record, owner, read_input(inputs_dir, job_id))
        if recovered is None:
            continue
        launched.append(
            _launch_thread(store, results_dir, secret, job_id, recovered, owner, lease_ttl)
        )
    return launched

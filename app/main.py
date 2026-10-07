"""FastAPI application: POST /jobs, GET /jobs/{id}, GET /jobs/{id}/download."""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from .config import Settings, get_settings
from .models import JobCreated, JobStatus
from .policy import SUPPORTED_POLICY_VERSIONS, canonical_json, default_profile_doc, policy_sha256
from .store import RESUMABLE_STATUSES, JobStore
from .worker import (
    adopt_stale_jobs,
    cleanup_stale_tmp_files,
    new_owner_token,
    process_job,
    recover_claimed_record,
    resume_interrupted_jobs,
    snapshot_resumable_leases,
    write_input_atomic,
)

DESCRIPTION = """\
Anonymize multi-series DICOM ZIP archives for research use.

* Deterministic Study/Series/Instance UID mapping (same input + same policy
  => byte-identical output)
* Standard + private PHI removal, delta-preserving date shifting
* Pixel redaction boxes burned into the pixel data, re-encoded output
* Itemized error reporting (missing references, illegal VRs, compressed
  pixels, policy conflicts) with all-or-nothing semantics
* Audit summary with verifiable SHA-256 hashes
* Crash-safe: interrupted jobs are recovered with their original id on
  resubmission and at startup (lease-based cross-process arbitration)
"""


def compute_job_id(
    input_sha256: str, policy: dict, redactions: Optional[list], secret_fp: str
) -> str:
    """Idempotent job id: identical submissions under the same secret scope
    map to the same job.

    The secret fingerprint is part of the id because the anonymized bytes are a
    function of ``ANON_SECRET`` (HMAC UID mapping, date shifting, tag hashing).
    Only a non-reversible fingerprint is ever used, never the secret itself, so
    rotating the secret yields an independent job/audit/result while old job
    ids remain fetchable (they are separate primary keys).
    """
    digest = hashlib.sha256()
    digest.update(b"job-scope-v1\n")
    digest.update(secret_fp.encode("utf-8"))
    digest.update(b"|")
    digest.update(input_sha256.encode("utf-8"))
    digest.update(b"|")
    digest.update(canonical_json(policy).encode("utf-8"))
    digest.update(b"|")
    digest.update(canonical_json(redactions).encode("utf-8"))
    return digest.hexdigest()[:32]


def job_status_payload(record: dict) -> dict:
    policy = json.loads(record["policy_json"])
    payload: dict = {
        "job_id": record["job_id"],
        "status": record["status"],
        "created_at": record["created_at"],
        "updated_at": record["updated_at"],
        "policy_version": policy.get("version"),
        "policy_sha256": policy_sha256(policy),
        "input_sha256": record["input_sha256"],
        "secret_fingerprint": record["secret_fp"],
    }
    if record["status"] == "failed":
        payload["errors"] = json.loads(record["errors_json"] or "[]")
    if record["status"] == "done":
        payload["audit"] = json.loads(record["audit_json"] or "{}")
        size_bytes: Optional[int] = None
        result_path = record["result_path"]
        if result_path and os.path.exists(result_path):
            size_bytes = os.path.getsize(result_path)
        payload["result"] = {
            "sha256": record["result_sha256"],
            "size_bytes": size_bytes,
            "download_url": f"/jobs/{record['job_id']}/download",
        }
    return payload


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or get_settings()
    os.makedirs(settings.data_dir, exist_ok=True)
    results_dir = os.path.join(settings.data_dir, "results")
    inputs_dir = os.path.join(settings.data_dir, "inputs")
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(inputs_dir, exist_ok=True)
    store = JobStore(os.path.join(settings.data_dir, "jobs.db"))
    lease_ttl = settings.lease_ttl_seconds

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # Crash recovery on boot: drop temp-file leftovers from interrupted
        # atomic writes, then take over unfinished jobs of the current secret
        # scope whose lease has already expired.
        cleanup_stale_tmp_files(results_dir, inputs_dir)
        resume_interrupted_jobs(
            store, inputs_dir, results_dir, settings.secret, lease_ttl,
            secret_fp=settings.secret_fp,
        )
        # Fast-restart case: a lease that has not expired yet but also never
        # renews for a full TTL belongs to a dead process. Snapshot now, then
        # CAS-take over whatever has not moved.
        snapshot = snapshot_resumable_leases(store, secret_fp=settings.secret_fp)
        if snapshot:
            def _adopt() -> None:
                time.sleep(lease_ttl * 1.5)
                try:
                    adopt_stale_jobs(
                        store, inputs_dir, results_dir, settings.secret, lease_ttl,
                        snapshot, secret_fp=settings.secret_fp,
                    )
                except Exception:
                    pass  # recovery is best-effort; resubmission can always retry

            threading.Thread(target=_adopt, name="job-adoption", daemon=True).start()
        yield

    app = FastAPI(
        title="DICOM Research Data Anonymization API",
        version="1.0.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )

    def schedule(job_id: str, owner: str, zip_bytes: bytes, policy_obj: dict,
                 redactions_obj: Optional[list], background_tasks: BackgroundTasks) -> None:
        background_tasks.add_task(
            process_job, store, results_dir, settings.secret, job_id,
            zip_bytes, policy_obj, redactions_obj,
            owner=owner, lease_ttl=lease_ttl,
        )

    @app.post("/jobs", status_code=202, response_model=JobCreated)
    def create_job(
        background_tasks: BackgroundTasks,
        file: UploadFile = File(..., description="ZIP archive of DICOM instances"),
        policy: str = Form(..., description="Versioned anonymization policy (JSON)"),
        redactions: Optional[str] = Form(None, description="Optional pixel redaction rules (JSON array)"),
    ):
        content = file.file.read()
        if not content:
            raise HTTPException(400, "uploaded file is empty")
        try:
            policy_obj = json.loads(policy)
        except json.JSONDecodeError as exc:
            raise HTTPException(400, f"policy is not valid JSON: {exc}")
        if not isinstance(policy_obj, dict):
            raise HTTPException(400, "policy must be a JSON object")
        redactions_obj = None
        if redactions is not None and redactions.strip():
            try:
                redactions_obj = json.loads(redactions)
            except json.JSONDecodeError as exc:
                raise HTTPException(400, f"redactions is not valid JSON: {exc}")

        input_sha256 = hashlib.sha256(content).hexdigest()
        secret_fp = settings.secret_fp
        job_id = compute_job_id(input_sha256, policy_obj, redactions_obj, secret_fp)
        existing = store.find_scoped(job_id, secret_fp)
        if existing is None:
            # New job: persist the input first (atomically) so a crash can
            # never leave a row whose input is unrecoverable, then register
            # the row together with this process's execution lease.
            write_input_atomic(inputs_dir, job_id, content)
            owner = new_owner_token()
            created = store.create(
                job_id, policy_obj, redactions_obj, input_sha256, secret_fp,
                lease_owner=owner, lease_expires_at=time.time() + lease_ttl,
            )
            if created:
                schedule(job_id, owner, content, policy_obj, redactions_obj, background_tasks)
                return JobCreated(job_id=job_id, status="queued", status_url=f"/jobs/{job_id}")
            # Concurrent first submission in another process won the insert;
            # fall through and treat the job as existing.
            existing = store.find_scoped(job_id, secret_fp)
            if existing is None:
                # The id is taken by a row outside this secret scope: a
                # truncated-id collision, not a legitimate duplicate.
                raise HTTPException(409, "job id conflict with an existing job")

        if existing["status"] in RESUMABLE_STATUSES:
            # Unfinished job: recover it in place if no live executor holds
            # the lease. The atomic claim is the cross-process arbitration;
            # whoever wins re-executes with inputs taken from the record (the
            # resubmitted ZIP is verified against the recorded hash), so the
            # job keeps its original id and produces the complete result.
            owner = new_owner_token()
            if store.claim(job_id, owner, time.time() + lease_ttl):
                recovered = recover_claimed_record(store, existing, owner, content)
                if recovered is not None:
                    zip_bytes, rec_policy, rec_redactions = recovered
                    schedule(job_id, owner, zip_bytes, rec_policy, rec_redactions, background_tasks)
        return JSONResponse(
            status_code=200,
            content={
                "job_id": job_id,
                "status": existing["status"],
                "status_url": f"/jobs/{job_id}",
                "detail": "identical submission already exists; returning the existing job",
            },
        )

    @app.get("/jobs/{job_id}", response_model=JobStatus)
    def get_job(job_id: str):
        record = store.get(job_id)
        if not record:
            raise HTTPException(404, "job not found")
        return job_status_payload(record)

    @app.get("/jobs/{job_id}/download")
    def download_result(job_id: str):
        record = store.get(job_id)
        if not record:
            raise HTTPException(404, "job not found")
        if record["status"] != "done":
            raise HTTPException(409, f"job status is '{record['status']}'; no result available")
        if not record["result_path"] or not os.path.exists(record["result_path"]):
            raise HTTPException(410, "job result file is no longer available on this server")
        return FileResponse(
            record["result_path"],
            media_type="application/zip",
            filename=f"anonymized-{job_id}.zip",
        )

    @app.get("/policies")
    def list_policies():
        return {
            "supported_versions": SUPPORTED_POLICY_VERSIONS,
            "default_profile": default_profile_doc(),
            "redactions_schema": {
                "type": "array",
                "items": {
                    "scope": "instance | series | study",
                    "uid": "SOP/series/study Instance UID to target",
                    "boxes": "[[x, y, width, height], ...] in pixel coordinates",
                    "fill": "optional integer fill value 0..65535 (default 0)",
                },
            },
        }

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app


app = create_app()

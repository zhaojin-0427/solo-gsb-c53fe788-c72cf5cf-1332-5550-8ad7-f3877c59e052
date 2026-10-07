"""Background job execution."""
from __future__ import annotations

import hashlib
import os
from typing import Any, Optional

from .engine import anonymize_zip
from .store import JobStore


def process_job(
    store: JobStore,
    results_dir: str,
    secret: str,
    job_id: str,
    zip_bytes: bytes,
    policy: dict,
    redactions: Optional[list],
) -> None:
    """Run the anonymization pipeline; persist outcome in the job store.

    The pipeline is all-or-nothing: the result file is only written after the
    whole archive has been transformed successfully, so a failed job never
    leaves partial output behind.
    """
    store.set_processing(job_id)
    try:
        errors, result, audit, _warnings = anonymize_zip(zip_bytes, policy, redactions, secret)
    except Exception as exc:  # defensive: report, never emit partial output
        store.set_failed(
            job_id,
            [{"file": None, "code": "INTERNAL_ERROR", "message": f"{type(exc).__name__}: {exc}"}],
        )
        return
    if errors:
        store.set_failed(job_id, errors)
        return
    assert result is not None and audit is not None
    path = os.path.join(results_dir, f"{job_id}.zip")
    with open(path, "wb") as fh:
        fh.write(result)
    store.set_done(job_id, path, hashlib.sha256(result).hexdigest(), audit)

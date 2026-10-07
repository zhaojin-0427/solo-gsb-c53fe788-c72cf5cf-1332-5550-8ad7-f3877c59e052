"""Recovery of interrupted jobs: resubmission, startup takeover, lease arbitration."""
from __future__ import annotations

import hashlib
import json
import os
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings, secret_fingerprint
from app.engine import anonymize_zip
from app.main import compute_job_id, create_app
from app.store import JobStore
from app.worker import (
    adopt_stale_jobs,
    input_path,
    snapshot_resumable_leases,
    write_input_atomic,
)

from test_api import (  # noqa: E402  (sibling test module helpers)
    BASE_POLICY,
    SECRET,
    SECRET_B,
    download,
    get_job,
    make_zip,
    make_dcm,
    submit,
    three_instance_zip,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def submit_raw(client, zip_bytes, policy, redactions=None):
    data = {"policy": json.dumps(policy)}
    if redactions is not None:
        data["redactions"] = json.dumps(redactions)
    return client.post(
        "/jobs",
        files={"file": ("input.zip", zip_bytes, "application/zip")},
        data=data,
    )


def make_client(tmp_path, secret=SECRET, ttl=0.5):
    app = create_app(
        Settings(data_dir=str(tmp_path), secret=secret, lease_ttl_seconds=ttl)
    )
    return TestClient(app)


def make_client_no_lifespan(tmp_path, secret=SECRET, ttl=0.5):
    """Client without entering the lifespan context: startup recovery does not
    run, so a resubmission is the only possible recovery trigger."""
    return make_client(tmp_path, secret=secret, ttl=ttl)


def make_interrupted_site(tmp_path, payload, policy, redactions=None, *,
                          secret=SECRET, status="queued", lease=None):
    """Simulate a process that registered a job and then died before (or
    during) execution: persisted input archive + queued/processing row."""
    data_dir = str(tmp_path)
    inputs_dir = os.path.join(data_dir, "inputs")
    os.makedirs(inputs_dir, exist_ok=True)
    os.makedirs(os.path.join(data_dir, "results"), exist_ok=True)
    input_sha256 = hashlib.sha256(payload).hexdigest()
    fp = secret_fingerprint(secret)
    job_id = compute_job_id(input_sha256, policy, redactions, fp)
    write_input_atomic(inputs_dir, job_id, payload)
    store = JobStore(os.path.join(data_dir, "jobs.db"))
    owner, expires = lease or (None, None)
    assert store.create(
        job_id, policy, redactions, input_sha256, fp,
        lease_owner=owner, lease_expires_at=expires,
    )
    if status == "processing":
        store.set_processing(job_id)
    store._conn.close()  # process is gone
    return job_id


def wait_for_terminal(client, job_id, timeout=15.0):
    deadline = time.time() + timeout
    status = get_job(client, job_id)
    while status["status"] not in ("done", "failed"):
        if time.time() > deadline:
            raise AssertionError(f"job {job_id} stuck in {status['status']}")
        time.sleep(0.05)
        status = get_job(client, job_id)
    return status


# ---------------------------------------------------------------------------
# Resubmission recovers the interrupted job under its original id
# ---------------------------------------------------------------------------
def test_resubmit_recovers_interrupted_queued_job(tmp_path):
    payload = three_instance_zip()
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY, status="queued")
    client = make_client_no_lifespan(tmp_path)  # no startup recovery: resubmit must recover
    resp = submit_raw(client, payload, BASE_POLICY)
    assert resp.status_code == 200
    assert resp.json()["job_id"] == job_id
    status = get_job(client, job_id)
    assert status["status"] == "done"
    result = download(client, job_id)
    _, expected, _, _ = anonymize_zip(payload, BASE_POLICY, None, SECRET)
    assert result == expected  # complete, deterministic output
    assert hashlib.sha256(result).hexdigest() == status["result"]["sha256"]


def test_resubmit_recovers_processing_job_with_expired_lease(tmp_path):
    payload = three_instance_zip()
    lease = ("dead-host:1234:abc", time.time() - 5)  # executor died, lease lapsed
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY,
                                   status="processing", lease=lease)
    client = make_client_no_lifespan(tmp_path)
    resp = submit_raw(client, payload, BASE_POLICY)
    assert resp.status_code == 200
    assert resp.json()["job_id"] == job_id
    assert get_job(client, job_id)["status"] == "done"
    assert download(client, job_id)


def test_resubmit_does_not_hijack_a_live_executor(tmp_path):
    payload = three_instance_zip()
    lease = ("other-host:4321:xyz", time.time() + 3600)  # another process is on it
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY,
                                   status="processing", lease=lease)
    client = make_client_no_lifespan(tmp_path)
    resp = submit_raw(client, payload, BASE_POLICY)
    assert resp.status_code == 200
    assert resp.json()["job_id"] == job_id
    assert resp.json()["status"] == "processing"
    # not taken over: still processing, no result published
    assert get_job(client, job_id)["status"] == "processing"
    assert client.get(f"/jobs/{job_id}/download").status_code == 409


def test_resubmit_recovers_even_if_persisted_input_is_lost(tmp_path):
    """The resubmission body itself is a valid input source (its hash matches
    the record by construction of the job id)."""
    payload = three_instance_zip()
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY, status="queued")
    os.unlink(input_path(os.path.join(str(tmp_path), "inputs"), job_id))
    client = make_client_no_lifespan(tmp_path)
    resp = submit_raw(client, payload, BASE_POLICY)
    assert resp.status_code == 200
    assert get_job(client, job_id)["status"] == "done"
    _, expected, _, _ = anonymize_zip(payload, BASE_POLICY, None, SECRET)
    assert download(client, job_id) == expected


def test_failed_job_is_not_retried_on_resubmit(tmp_path):
    with make_client(tmp_path) as client:
        job_id = submit(client, b"this is not a zip file", BASE_POLICY)
        assert get_job(client, job_id)["status"] == "failed"
        resp = submit_raw(client, b"this is not a zip file", BASE_POLICY)
        assert resp.status_code == 200
        assert resp.json()["job_id"] == job_id
        assert resp.json()["status"] == "failed"
        assert get_job(client, job_id)["status"] == "failed"


# ---------------------------------------------------------------------------
# Startup recovery (docker compose restart)
# ---------------------------------------------------------------------------
def test_startup_recovers_interrupted_job(tmp_path):
    payload = three_instance_zip()
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY, status="processing",
                                   lease=("dead-host:1:gone", time.time() - 5))
    with make_client(tmp_path) as client:
        status = wait_for_terminal(client, job_id)
        assert status["status"] == "done"
        result = download(client, job_id)
        assert hashlib.sha256(result).hexdigest() == status["result"]["sha256"]
    _, expected, _, _ = anonymize_zip(payload, BASE_POLICY, None, SECRET)
    assert result == expected


def test_startup_adopts_stale_unexpired_lease(tmp_path):
    """Fast restart: the dead executor's lease has not expired yet, but it
    never renews — after one TTL the lease is provably stale and adopted."""
    payload = three_instance_zip()
    lease = ("dead-host:1:gone", time.time() + 3600)  # unexpired
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY,
                                   status="processing", lease=lease)
    with make_client(tmp_path, ttl=0.4) as client:
        status = wait_for_terminal(client, job_id, timeout=20)
        assert status["status"] == "done"
        assert download(client, job_id)


def test_startup_recovery_fails_when_input_missing(tmp_path):
    payload = three_instance_zip()
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY, status="queued")
    os.unlink(input_path(os.path.join(str(tmp_path), "inputs"), job_id))
    with make_client(tmp_path) as client:
        status = wait_for_terminal(client, job_id)
        assert status["status"] == "failed"
        assert any(e["code"] == "INPUT_UNAVAILABLE" for e in status["errors"])
        assert client.get(f"/jobs/{job_id}/download").status_code == 409


def test_startup_recovery_fails_when_input_does_not_match_record(tmp_path):
    payload = three_instance_zip()
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY, status="queued")
    # tamper with the persisted input: it no longer matches the recorded hash
    inputs_dir = os.path.join(str(tmp_path), "inputs")
    write_input_atomic(inputs_dir, job_id, make_zip({"x.dcm": make_dcm("9.9.1", "9.9.2", "9.9.3")}))
    with make_client(tmp_path) as client:
        status = wait_for_terminal(client, job_id)
        assert status["status"] == "failed"
        assert any(e["code"] == "INPUT_UNAVAILABLE" for e in status["errors"])


def test_stale_tmp_result_is_never_served(tmp_path):
    """A crash mid-publish leaves a temp file; it must be cleaned up and must
    never be exposed as the job result."""
    payload = three_instance_zip()
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY, status="queued")
    results_dir = os.path.join(str(tmp_path), "results")
    leftover = os.path.join(results_dir, f".{job_id}.deadbeef.tmp")
    with open(leftover, "wb") as fh:
        fh.write(b"half-written-result")
    with make_client(tmp_path) as client:
        assert not os.path.exists(leftover)  # cleaned at startup
        status = wait_for_terminal(client, job_id)
        assert status["status"] == "done"
        result = download(client, job_id)
        assert result != b"half-written-result"
        assert hashlib.sha256(result).hexdigest() == status["result"]["sha256"]
    _, expected, _, _ = anonymize_zip(payload, BASE_POLICY, None, SECRET)
    assert result == expected


# ---------------------------------------------------------------------------
# Secret rotation: recovery stays inside the submitting secret's scope
# ---------------------------------------------------------------------------
def test_interrupted_job_stays_in_its_secret_scope(tmp_path):
    payload = three_instance_zip()
    job_id_a = make_interrupted_site(tmp_path, payload, BASE_POLICY,
                                     secret=SECRET, status="queued")

    # restart under a rotated secret: the old-scope job must NOT be recovered
    with make_client(tmp_path, secret=SECRET_B, ttl=0.4) as client_b:
        time.sleep(1.2)  # let startup resume + delayed adoption window pass
        assert get_job(client_b, job_id_a)["status"] == "queued"
        resp = submit_raw(client_b, payload, BASE_POLICY)
        assert resp.status_code == 202  # independent job under the new scope
        job_id_b = resp.json()["job_id"]
        assert job_id_b != job_id_a
        assert get_job(client_b, job_id_b)["status"] == "done"
        assert get_job(client_b, job_id_a)["status"] == "queued"

    # restart under the original secret: startup recovery finishes the old job
    with make_client(tmp_path, secret=SECRET, ttl=0.4) as client_a:
        status = wait_for_terminal(client_a, job_id_a)
        assert status["status"] == "done"
        assert status["secret_fingerprint"] == secret_fingerprint(SECRET)
        _, expected, _, _ = anonymize_zip(payload, BASE_POLICY, None, SECRET)
        assert download(client_a, job_id_a) == expected


# ---------------------------------------------------------------------------
# Lease arbitration primitives (cross-process)
# ---------------------------------------------------------------------------
def test_concurrent_first_submission_inserts_once(tmp_path):
    db = str(tmp_path / "jobs.db")
    store_a, store_b = JobStore(db), JobStore(db)  # two "processes"
    assert store_a.create("j" * 32, BASE_POLICY, None, "0" * 64, "fp") is True
    assert store_b.create("j" * 32, BASE_POLICY, None, "0" * 64, "fp") is False


def test_claim_arbitration_has_a_single_winner(tmp_path):
    db = str(tmp_path / "jobs.db")
    store_a, store_b = JobStore(db), JobStore(db)
    store_a.create("j" * 32, BASE_POLICY, None, "0" * 64, "fp",
                   lease_owner="owner-a", lease_expires_at=1000.0)
    now = 1001.0  # lease expired
    assert store_a.claim("j" * 32, "owner-b", now + 30, now=now) is True
    # the loser sees the fresh lease and backs off
    assert store_b.claim("j" * 32, "owner-c", now + 30, now=now) is False
    # once the new lease lapses, takeover is possible again
    assert store_b.claim("j" * 32, "owner-c", now + 60, now=now + 31) is True


def test_renewed_lease_blocks_claims(tmp_path):
    store = JobStore(str(tmp_path / "jobs.db"))
    store.create("j" * 32, BASE_POLICY, None, "0" * 64, "fp",
                 lease_owner="owner-a", lease_expires_at=1010.0)
    assert store.claim("j" * 32, "owner-b", 1040.0, now=1005.0) is False
    assert store.renew_lease("j" * 32, "owner-a", 1030.0) is True
    assert store.claim("j" * 32, "owner-b", 1060.0, now=1025.0) is False
    assert store.claim("j" * 32, "owner-b", 1060.0, now=1031.0) is True


def test_displaced_executor_cannot_update_the_record(tmp_path):
    store = JobStore(str(tmp_path / "jobs.db"))
    store.create("j" * 32, BASE_POLICY, None, "0" * 64, "fp",
                 lease_owner="owner-a", lease_expires_at=1000.0)
    assert store.claim("j" * 32, "owner-b", 1030.0, now=1001.0) is True
    # every transition of the displaced executor is rejected
    assert store.renew_lease("j" * 32, "owner-a", 1060.0) is False
    assert store.set_processing("j" * 32, owner="owner-a") is False
    assert store.set_done("j" * 32, "/tmp/x.zip", "0" * 64, {}, owner="owner-a") is False
    assert store.set_failed("j" * 32, [], owner="owner-a") is False
    # the new owner can drive the job to completion
    assert store.set_processing("j" * 32, owner="owner-b") is True
    assert store.set_done("j" * 32, "/tmp/x.zip", "0" * 64, {}, owner="owner-b") is True
    record = store.get("j" * 32)
    assert record["status"] == "done"
    assert record["lease_owner"] is None  # terminal state releases the lease


def test_adoption_skips_a_lease_that_keeps_moving(tmp_path):
    payload = three_instance_zip()
    lease = ("live-host:7:abc", time.time() + 3600)
    job_id = make_interrupted_site(tmp_path, payload, BASE_POLICY,
                                   status="processing", lease=lease)
    data_dir = str(tmp_path)
    store = JobStore(os.path.join(data_dir, "jobs.db"))
    fp = secret_fingerprint(SECRET)
    snapshot = snapshot_resumable_leases(store, secret_fp=fp)
    assert job_id in snapshot
    # the live owner renewed meanwhile -> lease moved -> not stale
    assert store.renew_lease(job_id, "live-host:7:abc", time.time() + 7200)
    launched = adopt_stale_jobs(
        store, os.path.join(data_dir, "inputs"), os.path.join(data_dir, "results"),
        SECRET, 0.4, snapshot, secret_fp=fp,
    )
    assert launched == []
    assert store.get(job_id)["status"] == "processing"

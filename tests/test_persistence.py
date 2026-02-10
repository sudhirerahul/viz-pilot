# tests/test_persistence.py
"""
Tests for DB persistence and the /api/viz/history endpoint.
Uses a disposable SQLite DB for isolation.
"""
import os
import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend import db as dbmod
from backend import app as app_module

TEST_DB_URL = "sqlite:///./test_viz_agent.db"
TEST_DB_FILE = "./test_viz_agent.db"
client = TestClient(app)


def fake_handle_request(prompt):
    """Patched handle_request that also persists to DB (like the real one)."""
    resp = {
        "request_id": "persist-test-1",
        "status": "success",
        "spec": {"$schema": "x"},
        "data_preview": [{"date": "2025-01-01", "Close": 100}],
        "provenance": {"sources": [{"source": "mock", "symbol": "TSLA"}]},
        "caption": "Test"
    }
    dbmod.save_request_record({
        "request_id": resp["request_id"],
        "prompt": prompt,
        "status": resp["status"],
        "response": resp,
        "provenance": resp.get("provenance", {}),
        "timestamp": None
    })
    return resp


@pytest.fixture(autouse=True)
def patch_orch(monkeypatch):
    try:
        if os.path.exists(TEST_DB_FILE):
            os.remove(TEST_DB_FILE)
    except Exception:
        pass
    dbmod.reconfigure(TEST_DB_URL)
    dbmod.init_db()
    # patch orchestrator
    orch = app_module.orchestrator
    monkeypatch.setattr(orch, "handle_request", fake_handle_request)
    yield
    try:
        if os.path.exists(TEST_DB_FILE):
            os.remove(TEST_DB_FILE)
    except Exception:
        pass


def test_persistence_and_history_endpoint():
    r = client.post("/api/viz", json={"prompt": "anything"})
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "success"
    rid = j["request_id"]

    # now check history endpoint
    r2 = client.get(f"/api/viz/history/{rid}")
    assert r2.status_code == 200
    rec = r2.json()["record"]
    assert rec["request_id"] == rid
    assert rec["status"] == "success"


def test_history_not_found():
    r = client.get("/api/viz/history/nonexistent-id")
    assert r.status_code == 404
    assert r.json()["error_code"] == "E_NOT_FOUND"

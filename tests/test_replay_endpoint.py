# tests/test_replay_endpoint.py
"""
Tests for the /api/viz/replay endpoint.
Saves a fake original record, patches orchestrator, calls replay, and asserts provenance.
"""
import os
import pytest
from fastapi.testclient import TestClient

from backend import db as dbmod
from backend.app import app
from backend import app as app_module

TEST_DB_URL = "sqlite:///./test_viz_agent_replay.db"
TEST_DB_FILE = "./test_viz_agent_replay.db"
client = TestClient(app)


@pytest.fixture(autouse=True)
def init_db_and_patch(monkeypatch):
    try:
        if os.path.exists(TEST_DB_FILE):
            os.remove(TEST_DB_FILE)
    except Exception:
        pass
    dbmod.reconfigure(TEST_DB_URL)
    dbmod.init_db()

    # create a saved original record
    original = {
        "request_id": "orig-123",
        "prompt": "Plot TSLA daily close",
        "status": "success",
        "response": {"dummy": "v"},
        "provenance": {"sources": [{"source": "mock", "symbol": "TSLA"}]},
        "timestamp": None
    }
    pid = dbmod.save_request_record(original)
    assert pid is not None

    # patch handle_autofix and handle_request to deterministic returns
    orch = app_module.orchestrator

    def fake_handle_autofix(prompt, autofix=None, explicit_transforms=None):
        return {
            "request_id": "replay-456",
            "status": "success",
            "spec": {"$schema": "x"},
            "data_preview": [{"date": "2025-01-01", "Close": 100}],
            "provenance": {"sources": [{"source": "mock", "symbol": "TSLA"}]},
            "caption": "Replayed"
        }

    def fake_handle_request(prompt):
        return {
            "request_id": "replay-789",
            "status": "success",
            "spec": {"$schema": "x"},
            "data_preview": [{"date": "2025-01-01", "Close": 100}],
            "provenance": {"sources": [{"source": "mock", "symbol": "TSLA"}]},
            "caption": "Replayed request"
        }

    monkeypatch.setattr(orch, "handle_autofix", fake_handle_autofix)
    monkeypatch.setattr(orch, "handle_request", fake_handle_request)

    yield

    try:
        if os.path.exists(TEST_DB_FILE):
            os.remove(TEST_DB_FILE)
    except Exception:
        pass


def test_replay_with_no_override():
    payload = {"request_id": "orig-123"}
    r = client.post("/api/viz/replay", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "success"
    assert j.get("provenance", {}).get("parent_request_id") == "orig-123"


def test_replay_with_override_and_autofix():
    payload = {
        "request_id": "orig-123",
        "override_prompt": "Plot TSLA weekly close",
        "autofix": {"method": "decimate"}
    }
    r = client.post("/api/viz/replay", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "success"
    assert j.get("provenance", {}).get("parent_request_id") == "orig-123"


def test_replay_not_found():
    payload = {"request_id": "nonexistent-id"}
    r = client.post("/api/viz/replay", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "error"
    assert j["error_code"] == "E_NOT_FOUND"

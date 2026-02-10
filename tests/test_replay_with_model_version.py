# tests/test_replay_with_model_version.py
"""
Tests that replay endpoint honors model_override and stores
parent_request_id in provenance.
"""
import os
import pytest
from fastapi.testclient import TestClient

from backend import db as dbmod
from backend.app import app
from backend import app as app_module

TEST_DB_URL = "sqlite:///./test_viz_agent_modelv.db"
TEST_DB_FILE = "./test_viz_agent_modelv.db"
client = TestClient(app)
orch = app_module.orchestrator


@pytest.fixture(autouse=True)
def setup_db_and_patch(monkeypatch):
    try:
        if os.path.exists(TEST_DB_FILE):
            os.remove(TEST_DB_FILE)
    except Exception:
        pass
    dbmod.reconfigure(TEST_DB_URL)
    dbmod.init_db()

    # save original record with spec_generator model "gpt-old"
    original = {
        "request_id": "orig-model-1",
        "prompt": "Plot TSLA",
        "status": "success",
        "response": {"dummy": "v"},
        "provenance": {
            "llm_calls": [
                {"role": "spec_generator", "model": "gpt-old", "response_id": "rid-old"}
            ]
        },
        "timestamp": None
    }
    dbmod.save_request_record(original)

    # patch handle_autofix & handle_request
    def fake_handle_autofix(prompt, autofix=None, explicit_transforms=None):
        return {
            "request_id": "replay-1",
            "status": "success",
            "spec": {},
            "data_preview": [],
            "provenance": {
                "llm_calls": [
                    {"role": "spec_generator", "model": "gpt-new", "response_id": "rid-new"}
                ]
            }
        }

    def fake_handle_request(prompt):
        return {
            "request_id": "replay-2",
            "status": "success",
            "spec": {},
            "data_preview": [],
            "provenance": {
                "llm_calls": [
                    {"role": "spec_generator", "model": "gpt-new", "response_id": "rid-new"}
                ]
            }
        }

    monkeypatch.setattr(orch, "handle_autofix", fake_handle_autofix)
    monkeypatch.setattr(orch, "handle_request", fake_handle_request)

    yield

    try:
        if os.path.exists(TEST_DB_FILE):
            os.remove(TEST_DB_FILE)
    except Exception:
        pass


def test_replay_with_model_override():
    payload = {"request_id": "orig-model-1", "model_override": "gpt-new"}
    r = client.post("/api/viz/replay", json=payload)
    assert r.status_code == 200
    j = r.json()
    prov = j.get("provenance", {})
    # ensure parent_request_id present
    assert prov.get("parent_request_id") == "orig-model-1"
    # ensure the replay provenance contains a spec_generator model
    sg = [c for c in prov.get("llm_calls", []) if c.get("role") == "spec_generator"]
    assert sg and sg[0].get("model") == "gpt-new"

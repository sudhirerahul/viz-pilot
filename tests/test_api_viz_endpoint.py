# tests/test_api_viz_endpoint.py
import pytest
from fastapi.testclient import TestClient
import datetime
import json

from backend.app import app

# import the orchestrator instance to monkeypatch its handle_request
from backend import app as app_module
orchestrator = app_module.orchestrator  # the single instance created in backend.app

# canned successful response
SAMPLE_SUCCESS = {
    "request_id": "test-req-123",
    "status": "success",
    "spec": {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Sample Chart",
        "data": {"values": [{"date": "2025-01-01", "Close": 100.0}]},
        "mark": "line",
        "encoding": {"x":{"field":"date","type":"temporal"}, "y":{"field":"Close","type":"quantitative"}},
        "title": "Sample Chart"
    },
    "data_preview": [{"date": "2025-01-01", "Close": 100.0}],
    "provenance": {"request_id":"test-req-123", "sources": [{"source":"mock-yfinance","symbol_or_key":"TSLA","fetched_at":datetime.datetime.utcnow().isoformat()+"Z","url_or_api_endpoint":None,"http_status":200,"raw_sample":""}], "validator":{"ok":True,"errors":[]}},
    "caption": "Sample Chart (auto-generated)"
}

SAMPLE_CLARIFY = {
    "request_id": "test-req-clarify",
    "status": "clarify_needed",
    "clarify_question": "Do you mean stock price or revenue?"
}

SAMPLE_ERROR = {
    "request_id": "test-req-err",
    "status": "error",
    "error_code": "E_NO_DATA",
    "message": "No data for symbol FOOBAR",
    "details": {}
}


@pytest.fixture
def client():
    return TestClient(app)


def test_api_viz_success(monkeypatch, client):
    # patch the orchestrator.handle_request to return success
    monkeypatch.setattr(orchestrator, "handle_request", lambda prompt: SAMPLE_SUCCESS)
    payload = {"prompt": "Plot TSLA close last 6 months"}
    r = client.post("/api/viz", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "success"
    assert "spec" in j and isinstance(j["spec"], dict)
    assert j["request_id"] == "test-req-123"
    assert j["provenance"]["validator"]["ok"] is True

def test_api_viz_clarify(monkeypatch, client):
    monkeypatch.setattr(orchestrator, "handle_request", lambda prompt: SAMPLE_CLARIFY)
    payload = {"prompt": "Show growth of Apple"}
    r = client.post("/api/viz", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "clarify_needed"
    assert "clarify_question" in j

def test_api_viz_error(monkeypatch, client):
    monkeypatch.setattr(orchestrator, "handle_request", lambda prompt: SAMPLE_ERROR)
    payload = {"prompt": "Plot FOOBAR nonsense"}
    r = client.post("/api/viz", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "error"
    assert j["error_code"] == "E_NO_DATA"

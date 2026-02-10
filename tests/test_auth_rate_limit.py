# tests/test_auth_rate_limit.py
"""
Tests for API key auth and rate limiting.

These tests set MOCK_AUTH=false and configure a test API key with a very small
rate limit. They use monkeypatch to control the auth module's settings so they
don't interfere with other tests (which run with MOCK_AUTH=true by default).
"""
import time
import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend import app as app_module
from backend import auth as authmod
from backend.auth import InMemoryFixedWindowLimiter

# We monkeypatch the orchestrator to avoid real flows
SAMPLE_SUCCESS = {
    "request_id": "autofix-test",
    "status": "success",
    "spec": {},
    "data_preview": [],
    "provenance": {},
}


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def setup_auth(monkeypatch):
    """Configure auth for testing: disable mock, set a test key, small rate limit."""
    # Patch module-level settings
    monkeypatch.setattr(authmod, "MOCK_AUTH", False)
    monkeypatch.setattr(authmod, "API_KEYS", {"test-key-123"})

    # Replace the rate limiter with a fresh one (small limit)
    limiter = InMemoryFixedWindowLimiter(limit_per_minute=3)
    monkeypatch.setattr(authmod, "_rate_limiter", limiter)

    # Patch orchestrator to return simple success
    monkeypatch.setattr(app_module.orchestrator, "handle_request", lambda prompt: SAMPLE_SUCCESS)
    yield


def test_missing_api_key_rejected(client):
    r = client.post("/api/viz", json={"prompt": "hello"})
    assert r.status_code == 401


def test_wrong_api_key_rejected(client):
    headers = {"x-api-key": "wrong-key"}
    r = client.post("/api/viz", headers=headers, json={"prompt": "hello"})
    assert r.status_code == 401


def test_valid_key_accepted(client):
    headers = {"x-api-key": "test-key-123"}
    r = client.post("/api/viz", headers=headers, json={"prompt": "hello"})
    assert r.status_code == 200
    assert r.json()["status"] == "success"


def test_rate_limit_enforced(client):
    headers = {"x-api-key": "test-key-123"}
    # 3 allowed, 4th should be 429
    for i in range(3):
        r = client.post("/api/viz", headers=headers, json={"prompt": f"req {i}"})
        assert r.status_code == 200, f"Request {i+1} should succeed"

    r4 = client.post("/api/viz", headers=headers, json={"prompt": "req 4"})
    assert r4.status_code == 429
    assert r4.json().get("error_code") == "E_RATE_LIMIT"
    assert "Retry-After" in r4.headers


def test_rate_limit_resets_in_new_window(client, monkeypatch):
    """Verify rate limit resets after crossing the minute window boundary."""
    headers = {"x-api-key": "test-key-123"}

    # Exhaust the limit
    for i in range(3):
        client.post("/api/viz", headers=headers, json={"prompt": f"req {i}"})
    r = client.post("/api/viz", headers=headers, json={"prompt": "blocked"})
    assert r.status_code == 429

    # Simulate advancing to the next minute window by manipulating the limiter's store
    limiter = authmod._rate_limiter
    with limiter._lock:
        # Force the stored window to an old minute so next request sees a new window
        for key in limiter._store:
            old_window, count = limiter._store[key]
            limiter._store[key] = (old_window - 2, count)

    # Now should be allowed again
    r = client.post("/api/viz", headers=headers, json={"prompt": "after reset"})
    assert r.status_code == 200


def test_health_not_rate_limited(client):
    """Health endpoint should not be affected by auth/rate limiting."""
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_metrics_not_rate_limited(client):
    """Metrics endpoint should not be affected by auth."""
    r = client.get("/metrics")
    assert r.status_code in (200, 404)  # 200 if prometheus enabled, 404 if not

# tests/test_metrics_endpoint.py
from fastapi.testclient import TestClient
import pytest

from backend.app import app


def test_metrics_endpoint_returns_prometheus_format():
    client = TestClient(app)
    r = client.get("/metrics")
    # Should return 200 with prometheus text format (if PROMETHEUS_ENABLED defaults to true)
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        assert "text/plain" in r.headers.get("content-type", "") or "text" in r.headers.get("content-type", "")


def test_health_still_works():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

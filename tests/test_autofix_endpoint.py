# tests/test_autofix_endpoint.py
import pytest
import datetime
from fastapi.testclient import TestClient

from backend.app import app
from backend import app as app_module

import backend.connectors.yfinance_connector as yconn
import backend.processors.intent_parser as iparser
import backend.processors.spec_generator as sgen

# Large preview to trigger decimation (200 rows, orchestrator.max_render_rows=5000 but
# we want to test that autofix actually applies transforms, not just decimation)
LARGE_PREVIEW = [
    {"date": f"2025-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}", "Close": float(100 + i)}
    for i in range(50)
]


def fake_fetch_ticker_preview(symbol, period="6mo", interval="1d", max_rows=50):
    return {
        "table": LARGE_PREVIEW,
        "metadata": {
            "source": "mock-yfinance",
            "symbol": symbol,
            "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
        },
        "raw": "",
    }


def fake_llm_parse_intent(prompt, max_retries=1):
    return {
        "goal": "Plot TSLA",
        "chart_type": "line",
        "metrics": ["Close"],
        "symbol": "TSLA",
        "transforms": [],
        "clarify": None,
    }


def fake_generate_spec(task, data_preview, max_retries=1):
    return {
        "vega_lite_spec": {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "description": task.get("goal"),
            "data": {"values": data_preview},
            "mark": "line",
            "encoding": {
                "x": {"field": "date", "type": "temporal"},
                "y": {"field": "Close", "type": "quantitative"},
            },
            "title": task.get("goal"),
        },
        "__caption__": "caption",
        "__used_fields__": ["date", "Close"],
        "__notes__": "",
    }


@pytest.fixture(autouse=True)
def patch_deps(monkeypatch):
    monkeypatch.setattr(yconn, "fetch_ticker_preview", fake_fetch_ticker_preview)
    monkeypatch.setattr(iparser, "llm_parse_intent", fake_llm_parse_intent)
    monkeypatch.setattr(sgen, "generate_vega_spec", fake_generate_spec)
    yield


@pytest.fixture
def client():
    return TestClient(app)


def test_autofix_explicit_transforms(client):
    """Apply a moving average transform explicitly via autofix endpoint."""
    transforms = [{"op": "moving_average", "field": "Close", "window": 3}]
    payload = {"prompt": "Plot TSLA daily close", "transforms": transforms}
    r = client.post("/api/viz/autofix", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "success"
    assert any("moving_average" in t for t in (j["provenance"].get("transforms") or []))


def test_autofix_no_params_still_succeeds(client):
    """Autofix with no autofix or transforms just runs the normal pipeline."""
    payload = {"prompt": "Plot TSLA daily close"}
    r = client.post("/api/viz/autofix", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "success"


def test_autofix_with_pct_change(client):
    """Apply pct_change transform via autofix endpoint."""
    transforms = [{"op": "pct_change", "field": "Close", "periods": 1}]
    payload = {"prompt": "Plot TSLA daily close", "transforms": transforms}
    r = client.post("/api/viz/autofix", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "success"
    assert any("pct_change" in t for t in (j["provenance"].get("transforms") or []))

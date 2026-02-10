# tests/test_model_versioning_capture.py
"""
Tests that LLM metadata (model, response_id) is captured into provenance
when the orchestrator handles a request through the spec generator.
"""
import os
import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend import app as app_module

client = TestClient(app)
orch = app_module.orchestrator


# patch spec_generator.generate_vega_spec to return _llm_meta
def fake_generate_spec(task, data_preview, max_retries=1):
    payload = {
        "vega_lite_spec": {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {"values": data_preview},
            "mark": "line",
            "encoding": {
                "x": {"field": "date", "type": "temporal"},
                "y": {"field": "Close", "type": "quantitative"}
            }
        },
        "__caption__": "cap",
        "__used_fields__": ["date", "Close"],
        "__notes__": "",
        "_llm_meta": {"model": "gpt-test-1", "response_id": "resp-123", "prompt_hash": None}
    }
    return payload


@pytest.fixture(autouse=True)
def patch_monkey(monkeypatch):
    # patch connectors and intent parser
    import backend.connectors.yfinance_connector as yconn

    def fake_fetch_ticker_preview(symbol, period="6mo", interval="1d", max_rows=50):
        return {
            "table": [{"date": "2025-01-01", "Close": 100}],
            "metadata": {"source": "mock", "symbol": symbol, "fetched_at": "2025-01-01T00:00:00Z"},
            "raw": ""
        }

    monkeypatch.setattr(yconn, "fetch_ticker_preview", fake_fetch_ticker_preview)

    # patch intent parser
    import backend.processors.intent_parser as ip
    monkeypatch.setattr(ip, "llm_parse_intent", lambda p: {
        "goal": "g", "symbol": "TSLA", "clarify": None,
        "chart_type": "line", "metrics": ["Close"]
    })

    # patch generate spec
    import backend.processors.spec_generator as sgen
    monkeypatch.setattr(sgen, "generate_vega_spec", fake_generate_spec)
    yield


def test_spec_generator_llm_meta_recorded():
    r = client.post("/api/viz", json={"prompt": "plot TSLA"})
    assert r.status_code == 200
    j = r.json()
    prov = j.get("provenance", {})
    llm_calls = prov.get("llm_calls", [])
    # find spec_generator entry
    sg = [c for c in llm_calls if c.get("role") == "spec_generator"]
    assert sg, "spec_generator llm call metadata not present"
    assert sg[0].get("model") == "gpt-test-1"
    assert sg[0].get("response_id") == "resp-123"

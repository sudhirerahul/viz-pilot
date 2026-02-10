# tests/test_end_to_end_orchestrator.py
import os
import json
import pytest
from datetime import datetime

from backend.orchestrator import VisualizationOrchestrator

# Monkeypatch connectors/LLM behavior by patching the modules the orchestrator imports
import backend.connectors.yfinance_connector as yconn
import backend.processors.intent_parser as iparser
import backend.processors.spec_generator as sgen

# Create a small deterministic preview to use in tests
SAMPLE_PREVIEW = [
    {"date": "2025-01-01", "Close": 100.0},
    {"date": "2025-01-02", "Close": 101.5},
    {"date": "2025-01-03", "Close": 102.0}
]

def fake_fetch_ticker_preview(symbol: str, period: str = "6mo", interval: str = "1d", max_rows: int = 50):
    return {
        "table": SAMPLE_PREVIEW,
        "metadata": {"source": "mock-yfinance", "symbol": symbol, "fetched_at": datetime.utcnow().isoformat() + "Z"},
        "raw": "date,Close\n2025-01-01,100.0\n..."
    }

def fake_llm_parse_intent(prompt: str, max_retries: int = 1):
    # Simple rule: if prompt contains TSLA -> return expected task
    p = prompt.lower()
    if "tsla" in p:
        return {
            "goal": "Plot TSLA daily close with 30d MA",
            "chart_type": "line",
            "metrics": ["Close"],
            "symbol": "TSLA",
            "dataset_key": None,
            "group_by": None,
            "time_range": {"start": "2024-01-01", "end": None},
            "sources": ["yfinance"],
            "filters": [],
            "transforms": [{"op": "moving_average", "field": "Close", "window": 30}],
            "clarify": None
        }
    raise ValueError("Unexpected prompt in fake parser")

def fake_generate_spec(task, data_preview):
    # return a very simple spec referencing 'date' and 'Close' (should pass validator)
    return {
        "vega_lite_spec": {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "description": task.get("goal", "Test Chart"),
            "data": {"values": data_preview},
            "mark": "line",
            "encoding": {
                "x": {"field": "date", "type": "temporal"},
                "y": {"field": "Close", "type": "quantitative"}
            },
            "title": task.get("goal", "Test Chart")
        },
        "__caption__": "Test caption",
        "__used_fields__": ["date", "Close"],
        "__notes__": ""
    }

@pytest.fixture(autouse=True)
def patch_connectors_and_parsers(monkeypatch):
    # patch connector
    monkeypatch.setattr(yconn, "fetch_ticker_preview", fake_fetch_ticker_preview)
    # patch intent parser
    monkeypatch.setattr(iparser, "llm_parse_intent", fake_llm_parse_intent)
    # patch spec generator
    monkeypatch.setattr(sgen, "generate_vega_spec", fake_generate_spec)
    yield

def test_end_to_end_success():
    orchestrator = VisualizationOrchestrator(preview_rows=50, max_render_rows=5000)
    prompt = "Plot TSLA daily close since 2024-01-01 with 30-day moving average"
    result = orchestrator.handle_request(prompt)
    assert result["status"] == "success"
    assert "spec" in result and isinstance(result["spec"], dict)
    assert "data_preview" in result and isinstance(result["data_preview"], list)
    assert result["provenance"]["sources"][0]["source"] == "mock-yfinance"
    # validator ok
    assert result["provenance"]["validator"]["ok"] is True
    # spec uses 'date' and 'Close'
    enc = result["spec"].get("encoding", {})
    assert enc.get("x", {}).get("field") == "date"
    assert enc.get("y", {}).get("field") == "Close"

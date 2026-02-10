# tests/test_spec_generator_llm.py
import json
import os
import pytest

# Force non-mock mode so generate_vega_spec takes the real LLM code path.
# We mock _call_llm (the isolated function) so no actual API calls are made.
os.environ["MOCK_SPEC_GENERATOR"] = "false"

# Re-import AFTER setting env var so the module-level flag picks it up.
# We need to reload the module to pick up the env change.
import importlib
import backend.processors.spec_generator as sgen
importlib.reload(sgen)
from backend.processors.spec_generator import generate_vega_spec

# Sample inputs
TASK = {
    "goal": "Plot TSLA close with 30d MA",
    "chart_type": "line",
    "metrics": ["Close"],
    "symbol": "TSLA",
    "transforms": [{"op": "moving_average", "field": "Close", "window": 30}]
}
DATA_PREVIEW = [
    {"date": "2025-01-01", "Close": 100.0},
    {"date": "2025-01-02", "Close": 101.5},
    {"date": "2025-01-03", "Close": 102.0}
]


def valid_model_response():
    payload = {
        "vega_lite_spec": {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "description": "Test spec",
            "data": {"values": DATA_PREVIEW},
            "mark": "line",
            "encoding": {"x": {"field": "date", "type": "temporal"}, "y": {"field": "Close", "type": "quantitative"}},
            "title": "TSLA Close"
        },
        "explanation": "The chart shows Tesla's daily closing price as a simple line chart over time.",
        "provenance": {
            "connectors_required": ["yfinance"],
            "api_keys_required": ["yfinance"],
            "notes": "Use yfinance connector with server-side key."
        }
    }
    return json.dumps(payload)


def malformed_model_response():
    return "Sorry I can't help with that right now."


def invalid_field_response():
    # references "AdjClose" which doesn't exist in preview; validator should catch this
    payload = {
        "vega_lite_spec": {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "description": "Invalid spec",
            "data": {"values": DATA_PREVIEW},
            "mark": "line",
            "encoding": {"x": {"field": "date", "type": "temporal"}, "y": {"field": "AdjClose", "type": "quantitative"}},
            "title": "Broken"
        },
        "explanation": "Broken chart with wrong field.",
        "provenance": {
            "connectors_required": ["yfinance"],
            "api_keys_required": ["yfinance"],
            "notes": ""
        }
    }
    return json.dumps(payload)


def corrected_model_response():
    payload = {
        "vega_lite_spec": {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "description": "Corrected spec",
            "data": {"values": DATA_PREVIEW},
            "mark": "line",
            "encoding": {"x": {"field": "date", "type": "temporal"}, "y": {"field": "Close", "type": "quantitative"}},
            "title": "Fixed"
        },
        "explanation": "Corrected chart showing Tesla daily close.",
        "provenance": {
            "connectors_required": ["yfinance"],
            "api_keys_required": ["yfinance"],
            "notes": ""
        }
    }
    return json.dumps(payload)


def test_spec_generator_success(monkeypatch):
    """Model returns valid JSON on first attempt."""
    monkeypatch.setattr(sgen, "_call_llm", lambda prompt_text, max_tokens=2000: valid_model_response())
    out = generate_vega_spec(TASK, DATA_PREVIEW, max_retries=1)
    assert isinstance(out, dict)
    assert "explanation" in out and out["explanation"] != ""
    # Legacy keys should also be populated via _normalize_response
    assert "__caption__" in out
    assert out["vega_lite_spec"]["encoding"]["y"]["field"] == "Close"


def test_spec_generator_malformed_json(monkeypatch):
    """Model returns malformed text; generator should fail after retries exhausted."""
    monkeypatch.setattr(sgen, "_call_llm", lambda prompt_text, max_tokens=2000: malformed_model_response())
    with pytest.raises(ValueError):
        generate_vega_spec(TASK, DATA_PREVIEW, max_retries=0)


def test_spec_generator_validator_retry(monkeypatch):
    """First response has invalid field, second response is corrected."""
    call_count = {"n": 0}

    def fake_call_llm(prompt_text, max_tokens=2000):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return invalid_field_response()
        return corrected_model_response()

    monkeypatch.setattr(sgen, "_call_llm", fake_call_llm)
    out = generate_vega_spec(TASK, DATA_PREVIEW, max_retries=1)
    assert out["vega_lite_spec"]["encoding"]["y"]["field"] == "Close"
    assert call_count["n"] == 2  # confirms retry happened

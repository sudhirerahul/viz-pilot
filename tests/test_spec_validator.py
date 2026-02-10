# tests/test_spec_validator.py
import json
import copy
import os
import pytest

from backend.validator import validate_vega_spec

# sample valid data_preview
SAMPLE_PREVIEW = [
    {"date": "2025-01-01", "Close": 100.0},
    {"date": "2025-01-02", "Close": 101.5},
    {"date": "2025-01-03", "Close": 102.0}
]

# valid basic spec
VALID_SPEC = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "description": "Sample line chart",
    "data": {"values": SAMPLE_PREVIEW},
    "mark": "line",
    "encoding": {
        "x": {"field": "date", "type": "temporal"},
        "y": {"field": "Close", "type": "quantitative"}
    },
    "title": "Sample Chart"
}

def test_valid_spec_passes():
    res = validate_vega_spec(VALID_SPEC, SAMPLE_PREVIEW)
    assert res["valid"], f"Expected valid spec; errors: {res['errors']}"

def test_missing_encoding_y_fails():
    spec = copy.deepcopy(VALID_SPEC)
    del spec["encoding"]["y"]
    res = validate_vega_spec(spec, SAMPLE_PREVIEW)
    assert not res["valid"]
    assert any("Missing encoding.y" in e or "encoding.y" in e for e in res["errors"])

def test_nonexistent_field_fails():
    spec = copy.deepcopy(VALID_SPEC)
    spec["encoding"]["y"]["field"] = "AdjClose"  # not in preview
    res = validate_vega_spec(spec, SAMPLE_PREVIEW)
    assert not res["valid"]
    assert any("not found in data preview" in e for e in res["errors"])

def test_forbidden_string_fails():
    spec = copy.deepcopy(VALID_SPEC)
    # inject a forbidden substring in title (simulated attack)
    spec["title"] = "Malicious <script>alert(1)</script>"
    res = validate_vega_spec(spec, SAMPLE_PREVIEW)
    assert not res["valid"]
    assert any("forbidden substrings" in e.lower() for e in res["errors"])

def test_large_preview_fails():
    big_preview = [{"date": f"2025-01-{i:02d}", "Close": 100.0 + i} for i in range(1, 200)]
    spec = copy.deepcopy(VALID_SPEC)
    res = validate_vega_spec(spec, big_preview)
    assert not res["valid"]
    assert any("exceeds allowed preview limit" in e for e in res["errors"])

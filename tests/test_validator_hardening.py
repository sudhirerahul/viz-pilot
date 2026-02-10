# tests/test_validator_hardening.py
import json
import copy
import pytest

from backend.validator import validate_vega_spec, sanitize_spec

SAMPLE_PREVIEW = [
    {"date": "2025-01-01", "Close": 100.0},
    {"date": "2025-01-02", "Close": 101.0},
]

VALID_SPEC = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "description": "Valid chart",
    "data": {"values": SAMPLE_PREVIEW},
    "mark": "line",
    "encoding": {
        "x": {"field": "date", "type": "temporal"},
        "y": {"field": "Close", "type": "quantitative"},
    },
    "title": "Valid Chart",
}


def test_valid_spec_passes():
    res = validate_vega_spec(copy.deepcopy(VALID_SPEC), SAMPLE_PREVIEW, max_rows=5000)
    assert res["valid"] is True
    assert res["errors"] == []


def test_forbidden_substring_in_title_detected():
    spec = copy.deepcopy(VALID_SPEC)
    spec["title"] = "Click <script>alert(1)</script>"
    sanitized, report = sanitize_spec(spec, max_preview_rows=50)
    # title should be cleaned
    assert "<script" not in sanitized.get("title", "").lower()
    # report shows sanitized_fields
    assert "title" in report["sanitized_fields"]
    # forbidden_matches should contain the match from original spec
    assert len(report["forbidden_matches"]) > 0


def test_function_in_spec_fails():
    spec = copy.deepcopy(VALID_SPEC)
    spec["description"] = "This includes a function() invocation"
    res = validate_vega_spec(spec, SAMPLE_PREVIEW)
    assert res["valid"] is False
    assert any("forbidden substrings" in e.lower() for e in res["errors"])


def test_data_uri_in_usermeta_gets_removed():
    spec = copy.deepcopy(VALID_SPEC)
    spec["usermeta"] = {"img": "data:image/png;base64,AAA"}
    sanitized, report = sanitize_spec(spec)
    # usermeta is a forbidden top-level key, should be removed
    assert "usermeta" not in sanitized
    assert "usermeta" in report["removed_top_keys"]


def test_inline_data_too_large_gets_trimmed():
    spec = copy.deepcopy(VALID_SPEC)
    # create 200 inline rows while max_preview_rows is 50
    spec["data"] = {
        "values": [{"date": f"2025-01-{(i % 28) + 1:02d}", "Close": float(i)} for i in range(200)]
    }
    res = validate_vega_spec(spec, SAMPLE_PREVIEW, max_rows=5000)
    # sanitizer trims inline data to 50, so the spec passes after trimming
    # but a warning is emitted about the trimming
    assert res["sanitization"]["inline_data_trimmed"] is not None
    assert "reduced" in res["sanitization"]["inline_data_trimmed"].lower()
    assert any("reduced" in w.lower() for w in res["warnings"])


def test_inline_data_exceeds_max_rows_rejected():
    spec = copy.deepcopy(VALID_SPEC)
    # create 6000 inline rows exceeding max_render_rows=5000
    spec["data"] = {
        "values": [{"date": f"2025-01-{(i % 28) + 1:02d}", "Close": float(i)} for i in range(6000)]
    }
    # max_preview_rows=50 in vega_allowed, so sanitizer trims to 50
    # but if we set max_rows=30, the 50 trimmed rows still exceed it
    res = validate_vega_spec(spec, SAMPLE_PREVIEW, max_rows=30)
    assert res["valid"] is False
    assert any("exceeds max allowed" in e.lower() for e in res["errors"])


def test_forbidden_top_level_keys_removed_and_warned():
    spec = copy.deepcopy(VALID_SPEC)
    spec["signals"] = [{"name": "s", "value": 1}]
    sanitized, report = sanitize_spec(spec)
    assert "signals" not in sanitized
    assert "signals" in report["removed_top_keys"]
    # validate also includes sanitization report
    res = validate_vega_spec(spec, SAMPLE_PREVIEW)
    assert "removed_top_keys" in res["sanitization"]
    assert "signals" in res["sanitization"]["removed_top_keys"]


def test_encoding_field_not_in_preview_fails():
    spec = copy.deepcopy(VALID_SPEC)
    spec["encoding"]["y"]["field"] = "AdjClose"
    res = validate_vega_spec(spec, SAMPLE_PREVIEW)
    assert res["valid"] is False
    assert any("not found in data preview" in e for e in res["errors"])


def test_javascript_uri_detected_and_sanitized():
    spec = copy.deepcopy(VALID_SPEC)
    spec["title"] = "javascript:alert(1)"
    sanitized, report = sanitize_spec(spec)
    # forbidden_matches should detect the javascript: pattern
    assert len(report["forbidden_matches"]) > 0
    # title is sanitized (regex removes "javascript:" portion)
    assert "title" in report["sanitized_fields"]
    assert "javascript:" not in sanitized.get("title", "").lower()


def test_iframe_in_description_fails():
    spec = copy.deepcopy(VALID_SPEC)
    spec["description"] = "See <iframe src='evil.com'></iframe>"
    res = validate_vega_spec(spec, SAMPLE_PREVIEW)
    assert res["valid"] is False
    assert any("forbidden substrings" in e.lower() for e in res["errors"])

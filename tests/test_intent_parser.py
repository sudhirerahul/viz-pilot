# tests/test_intent_parser.py
import json
import os
import pytest
from jsonschema import validate, ValidationError

from backend.processors.intent_parser import llm_parse_intent

HERE = os.path.dirname(__file__)
SCHEMA_PATH = os.path.join(HERE, "..", "schemas", "intent_task_schema.json")
GOLDEN_PATH = os.path.join(HERE, "golden_prompts.json")

with open(SCHEMA_PATH, "r") as f:
    INTENT_SCHEMA = json.load(f)

with open(GOLDEN_PATH, "r") as f:
    GOLDEN_PROMPTS = json.load(f)


def make_response_for_prompt(prompt_text: str) -> str:
    """
    Return a JSON string that the mocked LLM will return for a given prompt_text.
    """
    p = prompt_text.strip().lower()

    if "growth of apple" in p or p.startswith("show growth"):
        return json.dumps({
            "goal": "Clarify growth type for Apple",
            "chart_type": "auto",
            "metrics": [],
            "symbol": None,
            "dataset_key": None,
            "group_by": None,
            "time_range": {"start": None, "end": None},
            "sources": [],
            "filters": [],
            "transforms": [],
            "clarify": {"question": "Do you mean stock price growth (close) or revenue growth?"},
        })
    if "tsla" in p and "moving average" in p:
        return json.dumps({
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
            "clarify": None,
        })
    if "aapl adjusted close" in p:
        return json.dumps({
            "goal": "Plot AAPL adjusted close last 12 months",
            "chart_type": "line",
            "metrics": ["Adj Close"],
            "symbol": "AAPL",
            "dataset_key": None,
            "group_by": None,
            "time_range": {"start": None, "end": None},
            "sources": ["yfinance"],
            "filters": [],
            "transforms": [],
            "clarify": None,
        })
    if "compare aapl and msft" in p:
        return json.dumps({
            "goal": "Compare AAPL and MSFT close prices",
            "chart_type": "line",
            "metrics": ["Close"],
            "symbol": None,
            "dataset_key": None,
            "group_by": None,
            "time_range": {"start": None, "end": None},
            "sources": ["yfinance"],
            "filters": [],
            "transforms": [{"op": "rebased_index", "field": "Close"}],
            "clarify": None,
        })
    if "cpiaucsl" in p or "cpi" in p:
        return json.dumps({
            "goal": "Plot US CPIAUCSL monthly since 2010-01-01",
            "chart_type": "line",
            "metrics": ["value"],
            "symbol": None,
            "dataset_key": "CPIAUCSL",
            "group_by": None,
            "time_range": {"start": "2010-01-01", "end": None},
            "sources": ["fred"],
            "filters": [],
            "transforms": [],
            "clarify": None,
        })
    if "volume" in p and "tsla" in p:
        return json.dumps({
            "goal": "Plot TSLA volume last 6 months",
            "chart_type": "bar",
            "metrics": ["Volume"],
            "symbol": "TSLA",
            "dataset_key": None,
            "group_by": None,
            "time_range": {"start": None, "end": None},
            "sources": ["yfinance"],
            "filters": [],
            "transforms": [],
            "clarify": None,
        })
    # Default fallback
    return json.dumps({
        "goal": "Unknown",
        "chart_type": "auto",
        "metrics": [],
        "symbol": None,
        "dataset_key": None,
        "group_by": None,
        "time_range": {"start": None, "end": None},
        "sources": [],
        "filters": [],
        "transforms": [],
        "clarify": {"question": "Could you clarify what metric you want plotted?"},
    })


@pytest.fixture(autouse=True)
def mock_llm_call(monkeypatch):
    """
    Monkeypatch _call_llm in intent_parser module to return controlled values
    based on the user prompt content. No real API calls are made.
    """

    def fake_call_llm(system: str, user: str) -> str:
        return make_response_for_prompt(user)

    import backend.processors.intent_parser as ip_module
    monkeypatch.setattr(ip_module, "_call_llm", fake_call_llm)
    yield


def test_golden_prompts_validate_against_schema():
    """
    For each golden prompt, call llm_parse_intent and validate returned JSON
    against the intent schema. For prompts expecting 'clarify' ensure clarify exists.
    """
    for entry in GOLDEN_PROMPTS:
        prompt = entry["prompt"]
        expected = entry["expected_type"]
        parsed = llm_parse_intent(prompt)
        assert isinstance(parsed, dict), f"Parsed result not object for prompt: {prompt}"

        if expected == "clarify":
            assert parsed.get("clarify") and isinstance(parsed["clarify"], dict), \
                f"Expected clarify object for prompt: {prompt}"
            assert "question" in parsed["clarify"], \
                f"Clarify missing 'question' for prompt: {prompt}"
            continue

        # Validate against schema
        try:
            validate(instance=parsed, schema=INTENT_SCHEMA)
        except ValidationError as ve:
            pytest.fail(f"Schema validation failed for prompt '{prompt}': {ve}")

        assert parsed.get("goal"), "Goal should be present and non-empty"
        assert parsed.get("chart_type") in ["line", "bar", "scatter", "area", "auto"]
        assert isinstance(parsed.get("metrics"), list)


def test_malformed_llm_output_retries(monkeypatch):
    """
    If the LLM returns non-JSON on the first call, intent_parser retries once.
    On second call it returns valid JSON. Verify the final result is valid.
    """
    import backend.processors.intent_parser as ip_module

    call_count = {"n": 0}

    def flaky_call_llm(system: str, user: str) -> str:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return "This is not JSON at all!"
        return json.dumps({
            "goal": "Test recovery",
            "chart_type": "line",
            "metrics": ["Close"],
            "symbol": "TSLA",
            "dataset_key": None,
            "group_by": None,
            "time_range": {"start": "2024-01-01", "end": None},
            "sources": ["yfinance"],
            "filters": [],
            "transforms": [],
            "clarify": None,
        })

    monkeypatch.setattr(ip_module, "_call_llm", flaky_call_llm)

    parsed = llm_parse_intent("Plot TSLA close")
    assert parsed["goal"] == "Test recovery"
    assert call_count["n"] == 2


def test_persistent_malformed_output_raises(monkeypatch):
    """
    If the LLM returns non-JSON on both attempts, llm_parse_intent raises ValueError.
    """
    import backend.processors.intent_parser as ip_module

    def always_bad(system: str, user: str) -> str:
        return "NOT JSON EVER"

    monkeypatch.setattr(ip_module, "_call_llm", always_bad)

    with pytest.raises(ValueError, match="Intent parser failed"):
        llm_parse_intent("Plot something")

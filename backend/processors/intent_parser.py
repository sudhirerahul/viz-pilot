# backend/processors/intent_parser.py
import os
import json
import time
from typing import Any, Dict, Optional

from backend.llm_wrapper import call_llm as _llm_call, DEFAULT_MODEL as _WRAPPER_DEFAULT

INTENT_SYSTEM_PROMPT = """
You are an Intent Parser for a Visualization Agent. Convert the user's visualization request into EXACT JSON matching the TASK schema.
Return ONLY JSON. DO NOT add any explanatory text.

TASK schema (required keys):
- goal: string
- chart_type: "line"|"bar"|"scatter"|"area"|"auto"
- metrics: list of strings (e.g., ["Close"])
- symbol: ticker string or null
- dataset_key: dataset key (for macro series) or null
- group_by: string|null
- time_range: { "start": "YYYY-MM-DD" | null, "end": "YYYY-MM-DD" | null }
- sources: array of preferred sources (optional)
- filters: array of {column, op, value}
- transforms: array of {op, field, window?}
- clarify: null or { "question": "single sentence" }

Rules:
1) If any required field is ambiguous or missing, set "clarify" to {"question":"..."} with a single clarifying question.
2) Do not guess values for missing mandatory fields.
3) Use "symbol" for tickers (e.g., TSLA) and "dataset_key" for named macro series (e.g., CPIAUCSL). Only one of symbol/dataset_key should be non-null for MVP.
4) Return syntactically valid JSON only.
"""

INTENT_USER_PROMPT_TEMPLATE = 'User prompt: "{}"'

LLM_MODEL = os.getenv("INTENT_LLM_MODEL", _WRAPPER_DEFAULT)


# ---------------------------------------------------------------------------
# Mock responses for dev/test mode (MOCK_OPENAI=true)
# ---------------------------------------------------------------------------

def _mock_response_for_prompt(user_prompt: str) -> str:
    """Deterministic mock responses matching Step 2 golden prompts."""
    p = user_prompt.strip().lower()

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
    if "aapl" in p and ("adjusted close" in p or "adj close" in p):
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
    if "cpiaucsl" in p:
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
    # --- generic ticker fallback ---
    # Try to detect a ticker-like word (all caps, 1-5 letters)
    import re
    ticker_match = re.search(r"\b([A-Z]{1,5})\b", user_prompt)
    if ticker_match:
        sym = ticker_match.group(1)
        return json.dumps({
            "goal": f"Plot {sym} close price",
            "chart_type": "line",
            "metrics": ["Close"],
            "symbol": sym,
            "dataset_key": None,
            "group_by": None,
            "time_range": {"start": None, "end": None},
            "sources": ["yfinance"],
            "filters": [],
            "transforms": [],
            "clarify": None,
        })
    # Absolute fallback: clarify
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


# ---------------------------------------------------------------------------
# LLM call (real or mock)
# ---------------------------------------------------------------------------

def _call_llm(system: str, user: str) -> str:
    """
    Call LLM (OpenAI or Anthropic via centralized wrapper) and return raw content string.
    If MOCK_OPENAI is set, returns deterministic mock responses instead.
    Isolated so tests can monkeypatch without touching the SDK.
    """
    use_mock = os.getenv("MOCK_OPENAI", "true").lower() in ("1", "true", "yes")
    if use_mock:
        return _mock_response_for_prompt(user)

    resp = _llm_call(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        model=LLM_MODEL,
        max_tokens=1000,
        temperature=0.0,
    )
    return resp["text"]


def llm_parse_intent(user_prompt: str, max_retries: int = 1) -> Dict[str, Any]:
    """
    Ask the LLM to parse the user's prompt into the TASK JSON.
    Retries once if the LLM returns malformed JSON.
    Raises ValueError on persistent failure.
    """
    system = INTENT_SYSTEM_PROMPT
    user = INTENT_USER_PROMPT_TEMPLATE.format(user_prompt)

    attempt = 0
    last_exception: Optional[Exception] = None

    while attempt <= max_retries:
        attempt += 1
        try:
            raw_content = _call_llm(system, user)

            text = raw_content.strip()
            if text.startswith("```") and text.endswith("```"):
                lines = text.splitlines()
                if len(lines) >= 3:
                    text = "\n".join(lines[1:-1]).strip()

            parsed = json.loads(text)
            if not isinstance(parsed, dict):
                raise ValueError("Parsed JSON is not an object.")

            for k in ("goal", "chart_type", "metrics"):
                if k not in parsed:
                    raise ValueError(f"Missing required key '{k}' in parsed intent.")

            if parsed.get("symbol") == "":
                parsed["symbol"] = None
            if parsed.get("dataset_key") == "":
                parsed["dataset_key"] = None

            return parsed

        except Exception as e:
            last_exception = e
            time.sleep(0.2)
            continue

    raise ValueError(
        f"Intent parser failed after {max_retries + 1} attempts: {last_exception}"
    )

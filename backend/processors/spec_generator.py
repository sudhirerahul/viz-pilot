# backend/processors/spec_generator.py
import os
import json
import time
import pathlib
from typing import Dict, Any, List, Optional

from jsonschema import validate as jsonschema_validate, ValidationError

from backend.validator import validate_vega_spec
from backend.llm_wrapper import call_llm as _llm_call, LLM_PROVIDER, DEFAULT_MODEL as _WRAPPER_DEFAULT

# Load response schema (schemas/ lives at repo root, one level above backend/)
_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SCHEMA_PATH = _ROOT / "schemas" / "spec_generator_response_schema.json"
try:
    with open(_SCHEMA_PATH, "r", encoding="utf-8") as f:
        SPEC_RESPONSE_SCHEMA = json.load(f)
except Exception:
    SPEC_RESPONSE_SCHEMA = {
        "type": "object",
        "required": ["vega_lite_spec", "explanation"]
    }

# Flags & defaults
MOCK_SPEC_MODE = os.getenv("MOCK_SPEC_GENERATOR", "true").lower() in ("1", "true", "yes")
SPEC_LLM_MODEL = os.getenv("SPEC_LLM_MODEL", _WRAPPER_DEFAULT)

# ---------------------------------------------------------------------------
# Advanced system prompt (state-of-the-art visualization + explanation)
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """
You are an institutional financial visualization generator operating inside a secure server environment. Produce exact Vega-Lite v5 specs and a concise natural-language explanation for analyst consumption.

This task is correctness-critical and security-sensitive. Follow every rule below.

INPUT you will receive (structured)
- USER_QUERY — natural-language request
- DATA_SCHEMA — columns and types available
- DATA_PREVIEW — up to 10 sample rows (never assume beyond preview)
- DERIVED_FIELDS — server-computed fields already available (e.g., ma_30, vol_30)
- AVAILABLE_CONNECTORS — list of named data connectors the orchestrator provides
- API_KEYS_AVAILABLE — list of connector names for which server-side API keys/secrets are configured — NOTE: this is a boolean/list indicator only, never includes secret values

PRIMARY OBJECTIVES
1. Produce a Vega-Lite v5 JSON spec that implements the user intent using best-practice, state-of-the-art visual encodings and interactivity.
2. Produce a 2–4 sentence professional explanation describing what the chart shows, in analyst tone.
3. Produce provenance metadata indicating which connector(s) should be used and which API keys the orchestrator must use — do not include secret values.
4. Fail safely and clearly if the requested metric/transform is missing.

HARD REQUIREMENTS (must follow exactly)

A. Output format (STRICT JSON ONLY)
Return only this JSON object — no markdown, no extra commentary:
{
  "vega_lite_spec": { /* valid Vega-Lite v5 spec */ },
  "explanation": "string (2-4 sentences, analyst tone)",
  "provenance": {
    "connectors_required": ["yfinance"],
    "api_keys_required": ["yfinance"],
    "notes": "string (optional guidance for orchestrator; no secrets)"
  }
}

B. Intent fidelity
- If user requests close use close field. If user requests moving average 30, include a derived series named exactly MA 30 or ma_30 and plot as separate line.
- If user requests volatility band, include a confidence/volatility band (area) computed by ±1 stddev or provided vol_30 fields.
- If requested transform is not present in DERIVED_FIELDS, prefer using Vega-Lite transforms (window, aggregate) only if DATA_PREVIEW contains enough rows; otherwise return empty spec and explanation stating which data/derived field is missing.

C. Visualization quality (state-of-the-art)
- Time series must be a layered spec:
  - Layer 1: primary line (daily close)
  - Layer 2: derived smoothing line (30-day moving average) — distinguish via strokeDash or thickness
  - Layer 3: volatility/confidence band (semi-transparent area) when requested
  - Optional Layer 4: markers for anomalies or events if user asked
- Interactivity:
  - Allow hover tooltips showing date, close, MA30, vol30
  - Provide a brush/selection for zoom/pan on the x-axis (bind to an interval selection)
  - Legend must be clickable to toggle series visibility
  - Provide tooltip and nearest point highlight
- Aggregation & controls:
  - If user asks for resampling (weekly/monthly), include a transform that aggregates and label axis accordingly
  - If user asks for moving-window parameters, reflect them in the spec (window transform with frame)
- Encoding:
  - x must be temporal using date field
  - y quantitative, format axis tick labels with two decimals and currency when applicable
  - Use color channel for series, and opacity for bands
- Aesthetics:
  - Spec must be compatible with dark backgrounds (use light color palette for lines/bands)
  - Minimal chart chrome; clear axis titles and a concise title
- Performance:
  - Do not include more than max_preview_rows inline rows in data.values.
  - Avoid transforms that are O(n^2). Use window/aggregate transforms supported by Vega-Lite.
- Accessibility:
  - Include textual axis labels, legend text, and high-contrast color choices

D. Provenance & API key handling
- The returned provenance.connectors_required must list one or more connectors from AVAILABLE_CONNECTORS.
- The returned provenance.api_keys_required must list which connectors require server-side API keys. Do not output any secret values.
- Add provenance.notes that tell the orchestrator guidance, e.g., "Use yfinance connector with server-side key; compute ma_30 on server if available."

E. Security & failure behavior
- Never emit API keys or any secret in any field.
- If requested computation cannot be done safely given DATA_PREVIEW and DERIVED_FIELDS, set "vega_lite_spec": {} and put a clear explanation in explanation field.

EXPLANATION content rules (2–4 sentences)
- Describe what is plotted (series names)
- Explain moving average meaning and smoothing effect
- Call out notable trend or divergence visible in the sample (avoid prediction)
- Tone: professional, concise, neutral

FAILURE OUTPUT RULE (if cannot satisfy)
Return:
{
  "vega_lite_spec": {},
  "explanation": "Clear 1-2 sentence reason: which connector/derived field is missing.",
  "provenance": { "connectors_required": [...], "api_keys_required": [...], "notes": "..." }
}

REMINDERS
- Accuracy over aesthetics.
- Do not invent connectors or API keys — only reference those in AVAILABLE_CONNECTORS and API_KEYS_AVAILABLE.
- Never include secrets in any output.
"""


def _build_user_prompt(task: Dict[str, Any], data_preview: List[Dict[str, Any]], validator_feedback: Optional[List[str]] = None) -> str:
    parts = []
    parts.append("TASK_JSON:")
    parts.append(json.dumps(task, indent=0))

    # Build DATA_SCHEMA from preview
    if data_preview:
        schema_fields = {}
        sample = data_preview[0]
        for k, v in sample.items():
            if k.lower() == "date":
                schema_fields[k] = "temporal"
            elif isinstance(v, (int, float)):
                schema_fields[k] = "quantitative"
            else:
                schema_fields[k] = "nominal"
        parts.append("\nDATA_SCHEMA:")
        parts.append(json.dumps(schema_fields, indent=0))

    parts.append("\nDATA_PREVIEW:")
    parts.append(json.dumps(data_preview[:10], indent=0))

    # Derived fields (from transforms in task if any)
    derived = task.get("transforms", [])
    if derived:
        derived_names = []
        for t in derived:
            if isinstance(t, dict) and t.get("op") and t.get("field"):
                derived_names.append(f"{t['op']}_{t.get('window', '')}" if t.get("window") else t["op"])
        if derived_names:
            parts.append("\nDERIVED_FIELDS:")
            parts.append(json.dumps(derived_names, indent=0))

    # Available connectors and API keys (mock for dev; real orchestrator would inject these)
    parts.append('\nAVAILABLE_CONNECTORS: ["yfinance","fred","s3-csv"]')
    parts.append('\nAPI_KEYS_AVAILABLE: ["yfinance"]')

    if validator_feedback:
        parts.append("\nVALIDATOR_FEEDBACK:")
        parts.append(json.dumps({"errors": validator_feedback}, indent=0))
        parts.append("\nPlease regenerate the spec addressing the validator issues and following the constraints.")
    return "\n".join(parts)


def _extract_first_json(text: str) -> str:
    """Defensive extraction: find first JSON object in text, remove surrounding fences if any."""
    s = text.strip()
    if s.startswith("```") and s.endswith("```"):
        lines = s.splitlines()
        if len(lines) >= 3:
            s = "\n".join(lines[1:-1]).strip()
    first = s.find('{')
    if first == -1:
        return s
    last = s.rfind('}')
    if last == -1:
        return s
    return s[first:last + 1]


def _normalize_response(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize response to support both old (__caption__/__used_fields__/__notes__)
    and new (explanation/provenance) output formats.
    Ensures both sets of keys exist for backward compatibility.
    """
    # New format → fill legacy keys if missing
    if "explanation" in parsed and "__caption__" not in parsed:
        parsed["__caption__"] = parsed["explanation"]
    if "__used_fields__" not in parsed:
        # Infer from spec encoding if possible
        used = []
        spec = parsed.get("vega_lite_spec", {})
        enc = spec.get("encoding", {})
        for ch in enc.values():
            if isinstance(ch, dict) and "field" in ch:
                used.append(ch["field"])
        # Also check layers
        for layer in spec.get("layer", []):
            enc2 = layer.get("encoding", {})
            for ch in enc2.values():
                if isinstance(ch, dict) and "field" in ch:
                    used.append(ch["field"])
        parsed["__used_fields__"] = list(set(used)) if used else []
    if "__notes__" not in parsed:
        prov = parsed.get("provenance", {})
        parsed["__notes__"] = prov.get("notes", "")

    # Legacy format → fill new keys if missing
    if "explanation" not in parsed:
        parsed["explanation"] = parsed.get("__caption__", "")
    if "provenance" not in parsed or not isinstance(parsed.get("provenance"), dict):
        parsed["provenance"] = {
            "connectors_required": [],
            "api_keys_required": [],
            "notes": parsed.get("__notes__", "")
        }

    return parsed


def _parse_model_response_to_json(resp_text: str) -> Dict[str, Any]:
    payload_text = _extract_first_json(resp_text)
    try:
        parsed = json.loads(payload_text)
    except Exception as e:
        raise ValueError(f"Failed to parse JSON from model response: {e}. Raw: {resp_text[:1000]}")
    try:
        jsonschema_validate(instance=parsed, schema=SPEC_RESPONSE_SCHEMA)
    except ValidationError as ve:
        raise ValueError(f"Response JSON failed schema validation: {ve.message}")
    return _normalize_response(parsed)


def _call_llm(prompt_text: str, max_tokens: int = 4096) -> str:
    """
    Single LLM call. Returns raw content string.
    Uses the centralized llm_wrapper (supports OpenAI + Anthropic).
    Isolated so tests can monkeypatch this function.
    """
    resp = _llm_call(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt_text}
        ],
        model=SPEC_LLM_MODEL,
        max_tokens=max_tokens,
        temperature=0.0,
    )
    return resp["text"]


def _call_model_and_get_json(prompt_text: str, max_tokens: int = 2000) -> Dict[str, Any]:
    """
    Call LLM (or mock) and return parsed JSON dict.
    Raises on parsing/schema error or API errors.
    """
    if MOCK_SPEC_MODE:
        # Deterministic mock for local dev without hitting API
        try:
            if "DATA_PREVIEW:" in prompt_text:
                after = prompt_text.split("DATA_PREVIEW:")[1]
                # The preview may be followed by VALIDATOR_FEEDBACK or end of string
                for marker in ("\nVALIDATOR_FEEDBACK:", "\nPlease regenerate", "\nDERIVED_FIELDS:", "\nAVAILABLE_CONNECTORS:"):
                    if marker in after:
                        after = after[:after.index(marker)]
                dp = json.loads(after.strip())
            else:
                dp = []
        except Exception:
            dp = []
        if not dp:
            return {
                "vega_lite_spec": {},
                "explanation": "Mock could not infer data preview. Please provide data.",
                "provenance": {
                    "connectors_required": ["yfinance"],
                    "api_keys_required": ["yfinance"],
                    "notes": "mock — no data available"
                },
                "__caption__": "",
                "__used_fields__": [],
                "__notes__": "mock could not infer preview",
                "_llm_meta": {"model": SPEC_LLM_MODEL, "response_id": f"mock-{SPEC_LLM_MODEL}", "prompt_hash": None}
            }
        sample_fields = list(dp[0].keys())
        metric_field = next((f for f in sample_fields if f.lower() != "date"), sample_fields[0])
        spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "description": "Auto-generated (mock) spec",
            "data": {"values": dp},
            "mark": "line",
            "encoding": {"x": {"field": "date", "type": "temporal"}, "y": {"field": metric_field, "type": "quantitative"}},
            "title": "Mock Chart"
        }
        return {
            "vega_lite_spec": spec,
            "explanation": f"The chart shows the {metric_field} series over time as a line chart.",
            "provenance": {
                "connectors_required": ["yfinance"],
                "api_keys_required": ["yfinance"],
                "notes": "Mock spec — server-side computation recommended for production."
            },
            "__caption__": "Mock caption",
            "__used_fields__": ["date", metric_field],
            "__notes__": "",
            "_llm_meta": {"model": SPEC_LLM_MODEL, "response_id": f"mock-{SPEC_LLM_MODEL}", "prompt_hash": None}
        }

    # Real model call (uses _call_llm which tests can monkeypatch)
    text = _call_llm(prompt_text, max_tokens)
    parsed = _parse_model_response_to_json(text)

    # Auto-inject $schema if the LLM omitted it (common with Claude)
    vspec = parsed.get("vega_lite_spec", {})
    if isinstance(vspec, dict) and vspec and "$schema" not in vspec:
        vspec["$schema"] = "https://vega.github.io/schema/vega-lite/v5.json"
        parsed["vega_lite_spec"] = vspec

    parsed["_llm_meta"] = {
        "model": SPEC_LLM_MODEL,
        "provider": LLM_PROVIDER,
        "response_id": None,
        "prompt_hash": None
    }
    return parsed


def generate_vega_spec(task: Dict[str, Any], data_preview: List[Dict[str, Any]], max_retries: int = 1) -> Dict[str, Any]:
    """
    LLM-driven spec generator with retry-on-validation-failure.

    Returns:
      {
        "vega_lite_spec": {...},
        "explanation": "...",
        "provenance": {"connectors_required": [...], "api_keys_required": [...], "notes": "..."},
        "__caption__": "...", "__used_fields__": [...], "__notes__": "",
        "_llm_meta": {"model": "...", "response_id": "...", "prompt_hash": ...}
      }

    Raises ValueError on irrecoverable failures (malformed JSON, repeated invalid spec).
    """
    if not isinstance(task, dict):
        raise ValueError("Task must be a dict.")
    if not isinstance(data_preview, list) or len(data_preview) == 0:
        raise ValueError("data_preview must be a non-empty list of rows.")

    user_prompt = _build_user_prompt(task, data_preview)
    attempt = 0
    last_err: Optional[str] = None

    while attempt <= max_retries:
        attempt += 1
        try:
            parsed = _call_model_and_get_json(user_prompt)
        except Exception as e:
            last_err = f"LLM/parse error: {e}"
            time.sleep(0.2)
            continue

        # If vega is empty (generator says it cannot produce spec), return with notes
        vega = parsed.get("vega_lite_spec", {})
        if not vega or (isinstance(vega, dict) and len(vega) == 0):
            return parsed

        # Run validator
        validation = validate_vega_spec(vega, data_preview)
        if validation.get("valid", False):
            return parsed
        else:
            last_err = "Validator errors: " + "; ".join(validation.get("errors", []))
            if attempt <= max_retries:
                user_prompt = _build_user_prompt(task, data_preview, validator_feedback=validation.get("errors", []))
                time.sleep(0.2)
                continue
            else:
                raise ValueError(f"Spec failed validation after {attempt} attempts: {validation.get('errors', [])}")

    raise ValueError(f"Spec generation failed: {last_err}")

# backend/validator.py
"""
Hardened Vega-Lite spec validator + sanitizer for Visualization Agent (MVP+).

This module provides:
- sanitize_spec(spec) -> (clean_spec, sanitization_report)
- validate_vega_spec(spec, data_preview, max_rows=None) -> { valid, errors, warnings, sanitization }

Goals:
- Remove or flag executable / dangerous content embedded in specs.
- Enforce allowed grammar (marks, encodings).
- Enforce inline data size limits.
- Provide structured diagnostics suitable for provenance and automated retry feedback.
"""

import json
import re
from typing import Dict, Any, List, Optional, Tuple
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
ALLOWED_PATH = ROOT.joinpath("schemas", "vega_allowed.json")

# load allowed grammar
try:
    with open(ALLOWED_PATH, "r", encoding="utf-8") as f:
        VEGA_ALLOWED = json.load(f)
except Exception:
    VEGA_ALLOWED = {
        "allowed_marks": ["line", "area", "bar", "point", "rect", "rule", "text", "tick", "circle", "square"],
        "required_top_level": ["$schema", "data"],
        "encoding_required_fields": ["x", "y"],
        "x_required": {"type": ["temporal", "ordinal", "nominal"], "field": "date"},
        "forbidden_keys": ["usermeta", "signals"],
        "max_preview_rows": 50,
        "max_render_rows": 5000
    }

# Dangerous patterns to detect (case-insensitive)
DANGEROUS_PATTERNS = [
    r"function\s*\(",
    r"<\s*script",
    r"eval\s*\(",
    r"window\.",
    r"document\.",
    r"__proto__",
    r"constructor\s*\(",
    r"new\s+Function",
    r"data:\s*image\/",
    r"javascript\s*:",
    r"<\s*iframe",
]

DANGEROUS_REGEX = re.compile("|".join(f"({p})" for p in DANGEROUS_PATTERNS), flags=re.I)


def _is_json_like(obj: Any) -> bool:
    return isinstance(obj, dict)


def _find_forbidden_substrings(obj: Any) -> List[str]:
    """Search spec JSON string for dangerous substrings; return matches (unique)."""
    try:
        s = json.dumps(obj)
    except Exception:
        s = str(obj)
    matches = set()
    for m in DANGEROUS_REGEX.finditer(s):
        matches.add(m.group(0))
    return sorted(matches)


def _strip_forbidden_top_keys(spec: Dict[str, Any], forbidden_keys: List[str]) -> Tuple[Dict[str, Any], List[str]]:
    """Remove forbidden top-level keys from the spec (if present)."""
    removed = []
    spec_copy = dict(spec)
    for fk in forbidden_keys:
        if fk in spec_copy:
            removed.append(fk)
            spec_copy.pop(fk, None)
    return spec_copy, removed


def _strip_suspicious_strings_in_titles(spec: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Sanitize title and description strings by removing script-like substrings."""
    modified = []
    spec_copy = dict(spec)
    for field in ("title", "description"):
        val = spec_copy.get(field)
        if isinstance(val, str):
            if DANGEROUS_REGEX.search(val):
                cleaned = DANGEROUS_REGEX.sub("", val)
                spec_copy[field] = cleaned
                modified.append(field)
    return spec_copy, modified


def _limit_inline_data(spec: Dict[str, Any], inline_limit: int) -> Tuple[Dict[str, Any], Optional[str]]:
    """If spec.data.values exists and is too large, truncate and return reason."""
    spec_copy = dict(spec)
    data_obj = spec_copy.get("data")
    if isinstance(data_obj, dict) and "values" in data_obj and isinstance(data_obj["values"], list):
        n = len(data_obj["values"])
        if n > inline_limit:
            spec_copy["data"] = {"values": data_obj["values"][:inline_limit]}
            message = f"Inline data reduced from {n} to {inline_limit} rows for safety."
            return spec_copy, message
    return spec_copy, None


def _scrub_strings(obj: Any) -> Any:
    """Recursively blank out strings containing data URIs or javascript: schemes."""
    if isinstance(obj, dict):
        return {k: _scrub_strings(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_scrub_strings(x) for x in obj]
    elif isinstance(obj, str):
        lower = obj.lower()
        if "data:image" in lower or "javascript:" in lower:
            return ""
        return obj
    return obj


def sanitize_spec(spec: Dict[str, Any], max_preview_rows: Optional[int] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Sanitize a Vega-Lite spec (returning a sanitized copy) and produce a report:
      - removed_top_keys: []
      - sanitized_fields: []
      - forbidden_matches: []
      - inline_data_trimmed: message|null
    """
    if max_preview_rows is None:
        max_preview_rows = VEGA_ALLOWED.get("max_preview_rows", 50)

    report: Dict[str, Any] = {
        "removed_top_keys": [],
        "sanitized_fields": [],
        "forbidden_matches": [],
        "inline_data_trimmed": None
    }

    if not _is_json_like(spec):
        report["forbidden_matches"] = ["spec_not_json"]
        return spec, report

    # 1) detect forbidden substrings on the ORIGINAL spec (before any stripping)
    forbidden_matches = _find_forbidden_substrings(spec)
    report["forbidden_matches"] = forbidden_matches

    # 2) remove forbidden top-level keys
    forbidden = VEGA_ALLOWED.get("forbidden_keys", [])
    spec1, removed = _strip_forbidden_top_keys(spec, forbidden)
    report["removed_top_keys"] = removed

    # 3) sanitize title/description strings
    spec2, modified_fields = _strip_suspicious_strings_in_titles(spec1)
    report["sanitized_fields"].extend(modified_fields)

    # 4) trim inline data if > max_preview_rows
    spec3, inline_msg = _limit_inline_data(spec2, max_preview_rows)
    report["inline_data_trimmed"] = inline_msg

    # 5) scrub data URIs/JS URLs embedded anywhere
    final_spec = _scrub_strings(spec3)

    return final_spec, report


def _get_encoding_field_type(encoding_field: Dict[str, Any]) -> Optional[str]:
    t = encoding_field.get("type")
    if isinstance(t, str):
        return t
    return None


def validate_vega_spec(spec: Dict[str, Any], data_preview: List[Dict[str, Any]], max_rows: Optional[int] = None) -> Dict[str, Any]:
    """
    Hardened validator:
      - sanitizes the spec (returns modifications)
      - performs structural checks against allowed grammar
      - enforces inline-data and render-row limits
      - checks encoding fields against data_preview
      - returns: { valid, errors, warnings, sanitization }
    """
    errors: List[str] = []
    warnings: List[str] = []
    sanitization_report: Dict[str, Any] = {}

    if not _is_json_like(spec):
        return {
            "valid": False,
            "errors": ["Spec must be a JSON object/dict."],
            "warnings": [],
            "sanitization": {"forbidden_matches": ["not_json"]}
        }

    # 0) sanitize spec and capture report
    max_preview = VEGA_ALLOWED.get("max_preview_rows", 50)
    sanitized_spec, sanitization_report = sanitize_spec(spec, max_preview_rows=max_preview)

    # If sanitizer found forbidden substrings, treat as error
    if sanitization_report.get("forbidden_matches"):
        errors.append(f"Spec contains forbidden substrings: {sanitization_report.get('forbidden_matches')}")

    # If sanitizer removed or trimmed inline data, add warnings
    if sanitization_report.get("inline_data_trimmed"):
        warnings.append(sanitization_report.get("inline_data_trimmed"))
    if sanitization_report.get("removed_top_keys"):
        warnings.append(f"Removed top-level forbidden keys: {sanitization_report.get('removed_top_keys')}")

    # 1) required top-level keys (check on sanitized spec)
    req_top = VEGA_ALLOWED.get("required_top_level", [])
    for k in req_top:
        if k not in sanitized_spec:
            errors.append(f"Missing required top-level key: '{k}'.")

    # 2) mark check — support both flat specs (mark at top) and layered specs
    is_layered = "layer" in sanitized_spec and isinstance(sanitized_spec.get("layer"), list)
    mark = sanitized_spec.get("mark")
    allowed_marks = VEGA_ALLOWED.get("allowed_marks", [])

    if not is_layered:
        mark_value = None
        if isinstance(mark, str):
            mark_value = mark
        elif isinstance(mark, dict) and "type" in mark and isinstance(mark["type"], str):
            mark_value = mark["type"]
        else:
            errors.append("Spec 'mark' must be a string or an object with 'type' (allowed marks: {}).".format(allowed_marks))
        if mark_value and mark_value not in allowed_marks:
            errors.append(f"Mark '{mark_value}' is not allowed. Allowed: {allowed_marks}")
    else:
        # Validate marks inside each layer (warnings only — don't block)
        for i, layer in enumerate(sanitized_spec["layer"]):
            if isinstance(layer, dict):
                lm = layer.get("mark")
                lm_val = None
                if isinstance(lm, str):
                    lm_val = lm
                elif isinstance(lm, dict) and "type" in lm:
                    lm_val = lm["type"]
                if lm_val and lm_val not in allowed_marks:
                    warnings.append(f"Layer {i} mark '{lm_val}' not in standard set. Allowed: {allowed_marks}")

    # 3) encoding checks — support both flat and layered specs
    sample_row = None
    if data_preview and isinstance(data_preview, list) and len(data_preview) > 0:
        sample_row = data_preview[0]
        sample_fields = set(sample_row.keys())
    else:
        sample_fields = set()

    # Collect fields produced by transforms (calculate, window, fold, etc.)
    # so the validator doesn't reject derived fields
    def _collect_transform_fields(spec_obj: Dict[str, Any]) -> set:
        derived = set()
        transforms = spec_obj.get("transform", [])
        if isinstance(transforms, list):
            for t in transforms:
                if isinstance(t, dict):
                    # "calculate" transform → "as" field
                    if "as" in t:
                        val = t["as"]
                        if isinstance(val, str):
                            derived.add(val)
                        elif isinstance(val, list):
                            derived.update(v for v in val if isinstance(v, str))
                    # "window" transform → list of "as" in window array
                    if "window" in t and isinstance(t["window"], list):
                        for w in t["window"]:
                            if isinstance(w, dict) and "as" in w:
                                derived.add(w["as"])
                    # "fold" transform → default fold field names
                    if "fold" in t:
                        derived.add(t.get("as", ["key", "value"])[0] if isinstance(t.get("as"), list) else "key")
                        derived.add(t.get("as", ["key", "value"])[1] if isinstance(t.get("as"), list) and len(t.get("as", [])) > 1 else "value")
        return derived

    # Collect from top-level transforms and from each layer
    transform_fields = _collect_transform_fields(sanitized_spec)
    if is_layered:
        for layer in sanitized_spec.get("layer", []):
            if isinstance(layer, dict):
                transform_fields.update(_collect_transform_fields(layer))
    sample_fields = sample_fields | transform_fields

    def check_enc_field(enc_name: str, enc_dict: Dict[str, Any], context: str = ""):
        enc = enc_dict.get(enc_name)
        if not isinstance(enc, dict):
            errors.append(f"{context}encoding.{enc_name} must be an object with at least 'field' and 'type'.")
            return
        field = enc.get("field")
        if not field or not isinstance(field, str):
            errors.append(f"{context}encoding.{enc_name}.field must be a non-empty string.")
            return
        if sample_row is not None:
            matches = [f for f in sample_fields if f.lower() == field.lower()]
            if not matches:
                # For layered specs, allow derived/computed fields not in preview
                if is_layered:
                    pass  # derived fields expected in layered specs
                else:
                    errors.append(f"{context}encoding.{enc_name}.field '{field}' not found in data preview fields: {sorted(list(sample_fields))}.")
            else:
                actual_field = matches[0]
                if enc_name == "x":
                    t = _get_encoding_field_type(enc)
                    if t and t != "temporal" and actual_field.lower() == "date":
                        errors.append(f"{context}encoding.x.field '{field}' looks like a date but encoding.x.type is '{t}'. Use 'temporal'.")
        if "type" not in enc:
            errors.append(f"{context}encoding.{enc_name}.type is required (e.g., 'temporal', 'quantitative').")

    encoding = sanitized_spec.get("encoding")

    if not is_layered:
        # Flat spec: must have top-level encoding with x, y
        if not isinstance(encoding, dict):
            errors.append("Spec must include an 'encoding' object.")
        else:
            required_enc = VEGA_ALLOWED.get("encoding_required_fields", ["x", "y"])
            for e in required_enc:
                if e not in encoding:
                    errors.append(f"Missing encoding.{e} field.")
            if "x" in encoding:
                check_enc_field("x", encoding)
            if "y" in encoding:
                check_enc_field("y", encoding)
    else:
        # Layered spec: check shared top-level encoding if present
        if isinstance(encoding, dict):
            if "x" in encoding:
                check_enc_field("x", encoding, context="shared ")
            if "y" in encoding:
                check_enc_field("y", encoding, context="shared ")

    # 4) data_preview size limit (backwards compatible with old validator)
    if not isinstance(data_preview, list):
        errors.append("data_preview must be a list of rows.")
    else:
        if len(data_preview) > max_preview:
            errors.append(f"data_preview has {len(data_preview)} rows which exceeds allowed preview limit {max_preview}.")

    # 5) inline data limit (spec.data.values)
    data_block = sanitized_spec.get("data") or {}
    inline_len = 0
    if isinstance(data_block, dict) and "values" in data_block and isinstance(data_block["values"], list):
        inline_len = len(data_block["values"])

    # enforce max_rows if provided (total render rows)
    effective_max_rows = max_rows if max_rows is not None else VEGA_ALLOWED.get("max_render_rows", 5000)
    if inline_len > effective_max_rows:
        errors.append(f"Spec includes {inline_len} data rows which exceeds max allowed {effective_max_rows}.")

    # 6) post-sanitization security check
    post_matches = _find_forbidden_substrings(sanitized_spec)
    if post_matches:
        errors.append(f"Spec still contains forbidden patterns after sanitization: {post_matches}")

    valid = len(errors) == 0
    return {
        "valid": valid,
        "errors": errors,
        "warnings": warnings,
        "sanitization": sanitization_report
    }

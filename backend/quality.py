# backend/quality.py
"""
Data Quality & Sanity Checks for Visualization Agent (MVP)

Provides:
- QualityConfig: thresholds and settings
- run_quality_checks(rows, config) -> dict with ok/errors/warnings/metrics
- attempt_autofix(rows, config) -> (fixed_rows, actions_taken, messages)

Design priorities:
- Deterministic fixes only (no hallucination / interpolation by default)
- Clear and actionable error messages
- Minimal external dependencies (uses pandas)
"""

from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
import math
import pandas as pd
import numpy as np

# Error codes (map to API error responses)
E_TOO_MANY_POINTS = "E_TOO_MANY_POINTS"
E_MISSING_MANY = "E_MISSING_MANY"
E_NON_MONOTONIC_DATES = "E_NON_MONOTONIC_DATES"
E_OUTLIER_DETECTED = "E_OUTLIER_DETECTED"
E_BAD_DATA = "E_BAD_DATA"


@dataclass
class QualityConfig:
    max_render_rows: int = 5000
    preview_row_limit: int = 50
    max_nan_ratio: float = 0.2
    outlier_iqr_multiplier: float = 3.0
    min_rows_for_outlier_detection: int = 6
    allow_autofix_downsample: bool = True
    downsample_method: str = "decimate"  # "decimate" or "aggregate_monthly"
    resample_agg: str = "mean"


def _to_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows).copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def run_quality_checks(rows: List[Dict[str, Any]], config: Optional[QualityConfig] = None) -> Dict[str, Any]:
    """
    Run quality checks and return a report dictionary with:
    {
      "ok": bool,
      "errors": [ { "code": str, "message": str } ],
      "warnings": [ { "code": str, "message": str } ],
      "metrics": { ... }
    }
    """
    if config is None:
        config = QualityConfig()

    report: Dict[str, Any] = {"ok": True, "errors": [], "warnings": [], "metrics": {}}

    if not isinstance(rows, list):
        report["ok"] = False
        report["errors"].append({"code": E_BAD_DATA, "message": "Rows must be a list of dicts."})
        return report

    n_rows = len(rows)
    report["metrics"]["n_rows"] = n_rows

    if n_rows == 0:
        report["ok"] = False
        report["errors"].append({"code": E_BAD_DATA, "message": "No data rows provided."})
        return report

    df = _to_df(rows)

    # 1) Row count check
    if n_rows > config.max_render_rows:
        report["ok"] = False
        report["errors"].append({
            "code": E_TOO_MANY_POINTS,
            "message": f"Data has {n_rows} rows which exceeds the max allowed {config.max_render_rows}."
        })

    # 2) Date monotonicity & duplicates
    if "date" in df.columns:
        if df["date"].isnull().any():
            report["ok"] = False
            report["errors"].append({"code": E_BAD_DATA, "message": "One or more date values could not be parsed (null)."})
        else:
            if not df["date"].is_monotonic_increasing:
                report["warnings"].append({
                    "code": E_NON_MONOTONIC_DATES,
                    "message": "Date column not strictly increasing. Consider sorting rows by date before charting."
                })
    else:
        report["warnings"].append({"code": "W_NO_DATE_COLUMN", "message": "No 'date' column present in data."})

    # 3) Missing values per column
    nan_info: Dict[str, Any] = {}
    for col in df.columns:
        if col == "date":
            continue
        total = len(df)
        na = int(df[col].isna().sum())
        ratio = na / total if total > 0 else 1.0
        nan_info[col] = {"n_na": na, "ratio": ratio}
        if ratio > config.max_nan_ratio:
            report["ok"] = False
            report["errors"].append({
                "code": E_MISSING_MANY,
                "message": f"Column '{col}' has {na}/{total} missing values (ratio {ratio:.2f}) which exceeds allowed {config.max_nan_ratio:.2f}."
            })
        elif ratio > (config.max_nan_ratio / 2):
            report["warnings"].append({
                "code": "W_MISSING_MANY",
                "message": f"Column '{col}' has {na}/{total} missing values (ratio {ratio:.2f})."
            })
    report["metrics"]["nan_info"] = nan_info

    # 4) Outlier detection (IQR-based) for numeric columns
    outliers: Dict[str, Any] = {}
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    report["metrics"]["numeric_cols"] = numeric_cols
    if len(numeric_cols) > 0 and n_rows >= config.min_rows_for_outlier_detection:
        for col in numeric_cols:
            series = df[col].dropna()
            if series.empty:
                continue
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            if iqr == 0:
                continue
            lower = q1 - config.outlier_iqr_multiplier * iqr
            upper = q3 + config.outlier_iqr_multiplier * iqr
            mask = (series < lower) | (series > upper)
            n_out = int(mask.sum())
            if n_out > 0:
                outliers[col] = {"n_outliers": n_out, "lower": float(lower), "upper": float(upper)}
                report["warnings"].append({
                    "code": E_OUTLIER_DETECTED,
                    "message": f"Column '{col}' has {n_out} outlier(s) outside [{lower:.3f}, {upper:.3f}]."
                })
    report["metrics"]["outliers"] = outliers

    # 5) Constant/flat series detection (warn)
    for col in numeric_cols:
        unique_vals = df[col].dropna().unique()
        if len(unique_vals) <= 1:
            report["warnings"].append({"code": "W_FLAT_SERIES", "message": f"Column '{col}' appears constant or near-constant."})

    return report


def attempt_autofix(rows: List[Dict[str, Any]], config: Optional[QualityConfig] = None) -> Tuple[List[Dict[str, Any]], List[str], List[Dict[str, Any]]]:
    """
    Attempt deterministic autofixes (only downsampling implemented for MVP).

    Returns:
      (fixed_rows, actions_taken, messages)

    actions_taken: list of short strings describing what was done
    messages: list of report dicts produced by run_quality_checks after each action
    """
    if config is None:
        config = QualityConfig()

    actions: List[str] = []
    messages: List[Dict[str, Any]] = []

    report = run_quality_checks(rows, config)
    messages.append(report)

    n_rows = len(rows)
    if n_rows <= config.max_render_rows:
        return rows, actions, messages

    if not config.allow_autofix_downsample:
        return rows, actions, messages

    # Try monthly aggregation if configured
    if config.downsample_method == "aggregate_monthly":
        try:
            df = pd.DataFrame(rows).copy()
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df = df.set_index("date")
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                if numeric_cols:
                    agg = config.resample_agg if config.resample_agg in ("mean", "sum", "median", "first", "last") else "mean"
                    res = getattr(df[numeric_cols].resample("MS"), agg)()
                    res = res.reset_index()
                    fixed_rows = []
                    for _, r in res.iterrows():
                        obj: Dict[str, Any] = {"date": r["date"].strftime("%Y-%m-%d")}
                        for c in numeric_cols:
                            v = r[c]
                            obj[c] = None if pd.isna(v) else float(v)
                        fixed_rows.append(obj)
                    actions.append(f"resample_monthly_agg_{agg}")
                    messages.append(run_quality_checks(fixed_rows, config))
                    return fixed_rows, actions, messages
        except Exception:
            pass  # fall through to decimate

    # Default decimation: keep every k-th row deterministically
    k = math.ceil(n_rows / config.max_render_rows)
    if k <= 1:
        return rows, actions, messages

    fixed_rows = [rows[i] for i in range(0, n_rows, k)]
    if len(fixed_rows) > config.max_render_rows:
        fixed_rows = fixed_rows[:config.max_render_rows]

    actions.append(f"decimate_every_{k}_kept_{len(fixed_rows)}_rows")
    messages.append(run_quality_checks(fixed_rows, config))
    return fixed_rows, actions, messages

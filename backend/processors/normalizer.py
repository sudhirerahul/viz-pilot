# backend/processors/normalizer.py
"""
Data Normalizer / Transformer for Visualization Agent (MVP)

Functions:
- apply_transforms(rows, transforms) -> (transformed_rows, applied_transforms)

Transforms supported (deterministic):
- {"op":"moving_average","field":"Close","window":30}
- {"op":"rebased_index","field":"Close","base":100}
- {"op":"resample","freq":"M","agg":"mean"}  # freq examples: "M" (monthly), "D" (daily)
- {"op":"pct_change","field":"Close","periods":1}

Inputs:
- rows: list[dict], each row must contain "date" in "YYYY-MM-DD" format (or parsable)
- transforms: list[dict] in the exact form above

Outputs:
- transformed_rows: list[dict] with same schema (dates as "YYYY-MM-DD")
- applied_transforms: list[str] describing applied transforms
"""

from typing import List, Dict, Any, Tuple
import pandas as pd

# Error codes used by callers/orchestrator
E_BAD_DATA = "E_BAD_DATA"
E_INVALID_TRANSFORM = "E_INVALID_TRANSFORM"


def _to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert list-of-dicts to DataFrame, parse dates, and preserve numeric columns.
    Ensures 'date' column exists and is datetime dtype.
    """
    if not isinstance(rows, list):
        raise ValueError("rows must be a list of dicts")

    if len(rows) == 0:
        df = pd.DataFrame(columns=["date"])
        df["date"] = pd.to_datetime(df["date"])
        return df

    df = pd.DataFrame(rows).copy()

    if "date" not in df.columns:
        raise ValueError(f"Each row must have a 'date' column. {E_BAD_DATA}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isna().all():
        raise ValueError(f"All date values are invalid or could not be parsed. {E_BAD_DATA}")

    # Sort by date ascending — important for rolling/pct computations
    df = df.sort_values("date").reset_index(drop=True)

    # Normalize column names: replace spaces with underscores
    df.columns = [c.replace(" ", "_") if isinstance(c, str) else c for c in df.columns]

    # Convert numeric-like columns to floats, leave others as-is
    for col in df.columns:
        if col == "date":
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _from_dataframe(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Convert DataFrame back to list-of-dicts, with 'date' formatted as 'YYYY-MM-DD'.
    Keeps numeric NaNs as None for JSON friendliness.
    """
    out = []
    for _, row in df.iterrows():
        r = {}
        if pd.isna(row.get("date")):
            r["date"] = None
        else:
            dt = pd.to_datetime(row["date"])
            r["date"] = dt.strftime("%Y-%m-%d")
        for col in df.columns:
            if col == "date":
                continue
            val = row.get(col)
            if pd.isna(val):
                r[col] = None
            elif isinstance(val, (int, float)) and not isinstance(val, bool):
                r[col] = float(val)
            else:
                r[col] = val
        out.append(r)
    return out


def apply_transforms(rows: List[Dict[str, Any]], transforms: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Apply a list of deterministic transforms to the table rows.

    Returns:
      (transformed_rows, applied_transforms)

    Raises:
      ValueError on invalid input or missing fields.
    """
    df = _to_dataframe(rows)
    applied: List[str] = []

    if not transforms:
        return _from_dataframe(df), applied

    for t in transforms:
        if not isinstance(t, dict) or "op" not in t:
            raise ValueError(f"Invalid transform spec: {t}. {E_INVALID_TRANSFORM}")

        op = t["op"]

        if op == "moving_average":
            field = t.get("field")
            window = int(t.get("window", 1))
            if not field or field not in df.columns:
                raise ValueError(f"moving_average requires existing 'field' in rows: {field}. {E_BAD_DATA}")
            if window <= 0:
                raise ValueError("moving_average window must be >0")
            if df[field].dropna().empty:
                raise ValueError(f"Field {field} contains no numeric data. {E_BAD_DATA}")
            ma_col = f"{field}_ma{window}"
            df[ma_col] = df[field].rolling(window=window, min_periods=1).mean()
            df[field] = df[ma_col]
            df.drop(columns=[ma_col], inplace=True)
            applied.append(f"moving_average_{field}_w{window}")

        elif op == "rebased_index":
            field = t.get("field")
            base = float(t.get("base", 100.0))
            if not field or field not in df.columns:
                raise ValueError(f"rebased_index requires existing 'field' in rows: {field}. {E_BAD_DATA}")
            series = df[field].astype(float)
            first_valid = series.dropna().iloc[0] if not series.dropna().empty else None
            if first_valid is None or float(first_valid) == 0:
                raise ValueError(f"Cannot rebase because field {field} has no valid non-zero first value. {E_BAD_DATA}")
            df[field] = (series / float(first_valid)) * float(base)
            applied.append(f"rebased_index_{field}_base{int(base)}")

        elif op == "resample":
            freq = t.get("freq", "M")
            agg = t.get("agg", "mean")
            if df.empty:
                raise ValueError("Cannot resample empty dataset.")
            tmp = df.set_index("date")
            numeric_cols = tmp.select_dtypes(include="number").columns.tolist()
            if not numeric_cols:
                raise ValueError(f"No numeric columns to aggregate for resample. {E_BAD_DATA}")
            freq_map = {"M": "MS", "W": "W-MON", "D": "D"}
            pandas_freq = freq_map.get(freq, freq)
            if agg not in ("mean", "sum", "median", "first", "last"):
                raise ValueError("Unsupported agg for resample. Supported: mean,sum,median,first,last")
            res = getattr(tmp[numeric_cols].resample(pandas_freq), agg)()
            res = res.reset_index()
            df = res
            applied.append(f"resample_{freq}_agg_{agg}")

        elif op == "pct_change":
            field = t.get("field")
            periods = int(t.get("periods", 1))
            if not field or field not in df.columns:
                raise ValueError(f"pct_change requires existing 'field' in rows: {field}. {E_BAD_DATA}")
            series = df[field].astype(float)
            df[field] = series.pct_change(periods=periods)
            applied.append(f"pct_change_{field}_p{periods}")

        else:
            raise ValueError(f"Unsupported transform op: {op}. {E_INVALID_TRANSFORM}")

    transformed_rows = _from_dataframe(df)
    return transformed_rows, applied

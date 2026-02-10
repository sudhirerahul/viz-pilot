# backend/connectors/yfinance_connector.py
import datetime
from typing import Any, Dict

import pandas as pd
import yfinance as yf


def fetch_ticker_data(
    symbol: str,
    period: str = "6mo",
    interval: str = "1d",
    max_rows: int = 50,
) -> Dict[str, Any]:
    """
    Fetch historical data for a ticker symbol via yfinance.

    Returns:
        {
            "table": [{"date": "YYYY-MM-DD", "Close": float, ...}, ...],
            "metadata": {"source": "yfinance", "symbol": str, "fetched_at": ISO},
            "raw_sample": "<first 3 rows as CSV>"
        }
    Raises RuntimeError if yfinance returns empty data.
    """
    df = yf.download(tickers=symbol, period=period, interval=interval, progress=False)

    if df is None or df.empty:
        raise RuntimeError(f"yfinance returned no data for symbol={symbol}")

    df = df.reset_index()

    # Flatten MultiIndex columns if present (yfinance >= 0.2.x sometimes returns them)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if col[1] == "" else col[0] for col in df.columns]

    # Normalise column names
    if "Adj Close" in df.columns:
        df.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)

    # Ensure date is ISO string
    date_col = "Date" if "Date" in df.columns else df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")

    # Keep only known columns
    allowed = [
        c
        for c in [date_col, "Open", "High", "Low", "Close", "Adj_Close", "Volume"]
        if c in df.columns
    ]
    df = df[allowed]

    # Build raw sample (CSV of first 3 rows)
    raw_sample = df.head(3).to_csv(index=False)

    # Convert to list of dicts with normalised "date" key
    rows = df.head(max_rows).to_dict(orient="records")
    normalised_rows = []
    for r in rows:
        nr: Dict[str, Any] = {}
        for k, v in r.items():
            key = "date" if k == date_col else k
            if pd.isna(v):
                nr[key] = None
            elif isinstance(v, (int, float)):
                nr[key] = round(float(v), 4)
            else:
                nr[key] = v
            normalised_rows.append(nr) if key == list(r.keys())[-1] else None
        normalised_rows.append(nr)

    # de-dup (the inline append above may double-add)
    seen: list = []
    deduped: list = []
    for nr in normalised_rows:
        sig = nr.get("date", "")
        if sig not in seen:
            seen.append(sig)
            deduped.append(nr)
    normalised_rows = deduped[:max_rows]

    metadata = {
        "source": "yfinance",
        "symbol": symbol,
        "fetched_at": datetime.datetime.utcnow().isoformat() + "Z",
    }

    return {"table": normalised_rows, "metadata": metadata, "raw_sample": raw_sample}


# Alias used by tests and downstream consumers
fetch_ticker_preview = fetch_ticker_data

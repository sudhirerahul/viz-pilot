# tests/test_yfinance_connector.py
import os
import json
import pandas as pd
import pytest
from datetime import datetime

# point to repo-relative paths
HERE = os.path.dirname(__file__)
SCHEMA_PATH = os.path.join(HERE, "..", "schemas", "time_series_schema.json")

with open(SCHEMA_PATH, "r") as f:
    TIME_SERIES_SCHEMA = json.load(f)

from jsonschema import validate, ValidationError

# import the connector (assumes path backend/connectors/yfinance_connector.py)
from backend.connectors.yfinance_connector import fetch_ticker_preview

# We'll monkeypatch yfinance.download inside the connector module's namespace
import backend.connectors.yfinance_connector as yconn

def make_dummy_df():
    # Build a small DataFrame as yfinance would return
    dates = pd.date_range(start="2025-01-01", periods=5, freq="D")
    df = pd.DataFrame({
        "Open": [100.0, 101.0, 102.0, 103.0, 104.0],
        "High": [101.0, 102.0, 103.0, 104.0, 105.0],
        "Low": [99.5, 100.5, 101.0, 102.5, 103.0],
        "Close": [100.5, 101.5, 102.5, 103.5, 104.5],
        "Adj Close": [100.4, 101.4, 102.4, 103.4, 104.4],
        "Volume": [1000000, 1100000, 900000, 1050000, 980000]
    }, index=dates)
    df.index.name = "Date"
    return df

def fake_download(tickers, period, interval, progress=False):
    return make_dummy_df()

def test_fetch_ticker_preview_monkeypatched(monkeypatch):
    # monkeypatch yfinance.download used inside the connector
    monkeypatch.setattr(yconn.yf, "download", fake_download)

    # Call connector
    res = fetch_ticker_preview(symbol="TSLA", period="5d", interval="1d", max_rows=10)

    # Basic structure checks
    assert "table" in res and isinstance(res["table"], list)
    assert "metadata" in res and isinstance(res["metadata"], dict)
    assert res["metadata"]["source"] == "yfinance"
    assert res["metadata"]["symbol"] == "TSLA"
    assert res["metadata"]["fetched_at"] is not None

    # Validate rows conform to schema shape (we will craft a minimal wrapper object to validate)
    wrapper = {
        "columns": [
            {"name": "date", "type": "temporal"},
            {"name": "Close", "type": "quantitative"}
        ],
        "rows": res["table"],
        "metadata": res["metadata"]
    }

    # JSON Schema validation should not raise
    validate(instance=wrapper, schema=TIME_SERIES_SCHEMA)

    # Pydantic model round-trip
    from backend.schemas import TimeSeriesTable
    ts = TimeSeriesTable(columns=wrapper["columns"], rows=wrapper["rows"], metadata=wrapper["metadata"])
    assert len(ts.rows) > 0
    # Ensure date format is valid ISO in first row
    assert ts.rows[0].date is not None
    # Ensure numeric Close parsed as float
    assert isinstance(ts.rows[0].Close, float)

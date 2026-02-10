# tests/test_quality.py
import pytest
import datetime
from backend.quality import (
    run_quality_checks,
    attempt_autofix,
    QualityConfig,
    E_TOO_MANY_POINTS,
    E_MISSING_MANY,
    E_NON_MONOTONIC_DATES,
)


def make_rows(n=100):
    base = datetime.date(2025, 1, 1)
    rows = []
    for i in range(n):
        d = base + datetime.timedelta(days=i)
        rows.append({"date": d.strftime("%Y-%m-%d"), "Close": 100.0 + i, "Volume": 1000 + i * 10})
    return rows


def test_too_many_points_detected():
    rows = make_rows(12000)
    config = QualityConfig(max_render_rows=5000, allow_autofix_downsample=False)
    report = run_quality_checks(rows, config)
    assert report["ok"] is False
    assert any(e["code"] == E_TOO_MANY_POINTS for e in report["errors"])


def test_missing_values_detected():
    rows = make_rows(10)
    # introduce NaNs in Close (6 out of 10 -> ratio 0.6)
    for i in range(6):
        rows[i]["Close"] = None
    config = QualityConfig(max_nan_ratio=0.5)
    report = run_quality_checks(rows, config)
    assert any(e["code"] == E_MISSING_MANY for e in report["errors"])
    assert report["ok"] is False


def test_non_monotonic_dates_warns():
    rows = [
        {"date": "2025-01-02", "Close": 101},
        {"date": "2025-01-01", "Close": 100},
        {"date": "2025-01-03", "Close": 102},
    ]
    config = QualityConfig()
    report = run_quality_checks(rows, config)
    assert any(w["code"] == E_NON_MONOTONIC_DATES for w in report["warnings"])


def test_outlier_detection_warns():
    rows = make_rows(20)
    # inject an outlier
    rows[10]["Close"] = 100000.0
    config = QualityConfig(min_rows_for_outlier_detection=5, outlier_iqr_multiplier=1.5)
    report = run_quality_checks(rows, config)
    assert any(w["code"] == "E_OUTLIER_DETECTED" for w in report["warnings"])


def test_autofix_decimate_reduces_rows():
    rows = make_rows(10000)
    config = QualityConfig(max_render_rows=1000, allow_autofix_downsample=True, downsample_method="decimate")
    fixed, actions, messages = attempt_autofix(rows, config)
    assert len(fixed) <= config.max_render_rows
    assert any("decimate_every" in a for a in actions)


def test_autofix_resample_monthly():
    # create daily data spanning 180 days (six months)
    rows = []
    base = datetime.date(2025, 1, 1)
    for i in range(180):
        d = base + datetime.timedelta(days=i)
        rows.append({"date": d.strftime("%Y-%m-%d"), "Close": 100.0 + i})
    config = QualityConfig(
        max_render_rows=10,
        allow_autofix_downsample=True,
        downsample_method="aggregate_monthly",
        resample_agg="mean",
    )
    fixed, actions, messages = attempt_autofix(rows, config)
    # monthly resample should reduce points to ~6-7 rows
    assert len(fixed) <= 12
    assert any(a.startswith("resample_monthly") for a in actions)


def test_no_autofix_if_disabled():
    rows = make_rows(10000)
    config = QualityConfig(max_render_rows=1000, allow_autofix_downsample=False)
    fixed, actions, messages = attempt_autofix(rows, config)
    assert len(fixed) == len(rows)
    assert actions == []

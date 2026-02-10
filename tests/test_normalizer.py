# tests/test_normalizer.py
import pytest
from backend.processors.normalizer import apply_transforms

SAMPLE_ROWS = [
    {"date": "2025-01-01", "Close": 100.0, "Volume": 1000},
    {"date": "2025-01-02", "Close": 110.0, "Volume": 1500},
    {"date": "2025-01-03", "Close": 120.0, "Volume": 1300},
    {"date": "2025-01-04", "Close": 130.0, "Volume": 1600},
    {"date": "2025-01-05", "Close": 140.0, "Volume": 1700},
]


def almost_eq(a, b, eps=1e-9):
    return abs(a - b) <= eps


def test_moving_average_short_window():
    rows = SAMPLE_ROWS.copy()
    transforms = [{"op": "moving_average", "field": "Close", "window": 3}]
    transformed, applied = apply_transforms(rows, transforms)
    # After moving average with window 3, values should be rolling mean:
    # Day1: mean([100]) = 100
    # Day2: mean([100,110]) = 105
    # Day3: mean([100,110,120]) = 110
    # Day4: mean([110,120,130]) = 120
    # Day5: mean([120,130,140]) = 130
    expected = [100.0, 105.0, 110.0, 120.0, 130.0]
    got = [r["Close"] for r in transformed]
    for g, e in zip(got, expected):
        assert g is not None
        assert almost_eq(g, e)

    assert "moving_average_Close_w3" in applied


def test_rebased_index():
    rows = SAMPLE_ROWS.copy()
    transforms = [{"op": "rebased_index", "field": "Close", "base": 100}]
    transformed, applied = apply_transforms(rows, transforms)
    # first value becomes 100; subsequent are scaled accordingly
    first = transformed[0]["Close"]
    assert almost_eq(first, 100.0)
    # second: 110 / 100 * 100 = 110
    assert almost_eq(transformed[1]["Close"], 110.0)
    assert "rebased_index_Close_base100" in applied


def test_pct_change():
    rows = SAMPLE_ROWS.copy()
    transforms = [{"op": "pct_change", "field": "Close", "periods": 1}]
    transformed, applied = apply_transforms(rows, transforms)
    # first row: NaN -> None
    assert transformed[0]["Close"] is None
    # second: (110-100)/100 = 0.1
    assert transformed[1]["Close"] == pytest.approx(0.1)
    assert "pct_change_Close_p1" in applied


def test_resample_monthly_mean():
    # construct daily data spanning two months (Jan and Feb)
    rows = [
        {"date": "2025-01-01", "Close": 100.0},
        {"date": "2025-01-15", "Close": 110.0},
        {"date": "2025-01-31", "Close": 120.0},
        {"date": "2025-02-01", "Close": 200.0},
        {"date": "2025-02-15", "Close": 220.0},
    ]
    transforms = [{"op": "resample", "freq": "M", "agg": "mean"}]
    transformed, applied = apply_transforms(rows, transforms)
    # Expect two rows: Jan and Feb (month-start dates)
    assert len(transformed) == 2
    # monthly means: Jan mean = (100+110+120)/3 = 110
    assert transformed[0]["Close"] == pytest.approx(110.0)
    # Feb mean = (200 + 220)/2 = 210
    assert transformed[1]["Close"] == pytest.approx(210.0)
    assert applied[0].startswith("resample_M")


def test_invalid_field_transform_raises():
    rows = SAMPLE_ROWS.copy()
    transforms = [{"op": "moving_average", "field": "NonExistent", "window": 3}]
    with pytest.raises(ValueError):
        apply_transforms(rows, transforms)


def test_empty_rows_no_transforms_returns_sorted():
    rows = [
        {"date": "2025-01-03", "Close": 3.0},
        {"date": "2025-01-01", "Close": 1.0},
        {"date": "2025-01-02", "Close": 2.0},
    ]
    transformed, applied = apply_transforms(rows, [])
    # Should be sorted by date ascending
    dates = [r["date"] for r in transformed]
    assert dates == ["2025-01-01", "2025-01-02", "2025-01-03"]
    assert applied == []

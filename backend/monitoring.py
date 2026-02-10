# backend/monitoring.py
"""
Centralized monitoring: Prometheus metrics, structured JSON logging, optional Sentry.

Env vars:
- PROMETHEUS_ENABLED (default: true)
- SENTRY_DSN (optional)
- LOG_AS_JSON (default: true)
- LOG_LEVEL (default: INFO)
- ENVIRONMENT (default: development)
"""

import os
import logging
import time
from typing import Tuple

from prometheus_client import (
    Counter, Histogram, Gauge,
    generate_latest, CONTENT_TYPE_LATEST, REGISTRY,
)

# Optional imports — degrade gracefully if not installed
try:
    from pythonjsonlogger import jsonlogger
    _HAS_JSON_LOGGER = True
except ImportError:
    _HAS_JSON_LOGGER = False

try:
    import sentry_sdk
    _HAS_SENTRY = True
except ImportError:
    _HAS_SENTRY = False

# --- ENV flags
PROMETHEUS_ENABLED = os.getenv("PROMETHEUS_ENABLED", "true").lower() in ("1", "true", "yes")
SENTRY_DSN = os.getenv("SENTRY_DSN", None)
LOG_AS_JSON = os.getenv("LOG_AS_JSON", "true").lower() in ("1", "true", "yes")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")


# --- Logger setup
def setup_logger(name: str = "viz-agent", level: int = None) -> logging.Logger:
    level = logging.getLevelName(os.getenv("LOG_LEVEL", "INFO")) if level is None else level
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        if LOG_AS_JSON and _HAS_JSON_LOGGER:
            fmt = jsonlogger.JsonFormatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s"
            )
            handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


logger = setup_logger()

# --- Sentry (optional)
if SENTRY_DSN and _HAS_SENTRY:
    sentry_sdk.init(dsn=SENTRY_DSN, environment=ENVIRONMENT)
    logger.info("Sentry initialized")


# --- Prometheus metrics
REQUEST_COUNT = Counter(
    "viz_requests_total",
    "Total /api/viz requests",
    ["method", "endpoint", "status"],
)

INTENT_PARSE_COUNTER = Counter(
    "viz_intent_parse_total",
    "Intent parsing attempts",
    ["outcome"],
)

SPEC_GEN_COUNTER = Counter(
    "viz_spec_gen_total",
    "Spec generation attempts",
    ["outcome"],
)

VALIDATION_FAILURES = Counter(
    "viz_spec_validation_failures_total",
    "Spec validation failures",
    ["reason"],
)

DATA_QUALITY_ERRORS = Counter(
    "viz_data_quality_errors_total",
    "Data quality errors detected",
    ["error_code"],
)

TRANSFORMS_APPLIED = Counter(
    "viz_transforms_applied_total",
    "Transforms applied",
    ["transform"],
)

REQUEST_LATENCY = Histogram(
    "viz_request_latency_seconds",
    "Request latency in seconds",
    ["endpoint"],
)

SPEC_GEN_LATENCY = Histogram(
    "viz_spec_gen_latency_seconds",
    "Spec generation latency",
)

LAST_DATA_ROWS = Gauge(
    "viz_last_data_rows",
    "Rows in last connector response",
)


# --- Helper wrappers (never crash the app)
def observe_request(start_ts: float, endpoint: str, method: str, status: str):
    try:
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(time.time() - start_ts)
        REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status).inc()
    except Exception:
        pass


def observe_spec_gen(start_ts: float, outcome: str):
    try:
        SPEC_GEN_LATENCY.observe(time.time() - start_ts)
        SPEC_GEN_COUNTER.labels(outcome=outcome).inc()
    except Exception:
        pass


def inc_intent_parse(outcome: str):
    try:
        INTENT_PARSE_COUNTER.labels(outcome=outcome).inc()
    except Exception:
        pass


def inc_validation_failure(reason: str):
    try:
        VALIDATION_FAILURES.labels(reason=reason).inc()
    except Exception:
        pass


def inc_data_quality_error(code: str):
    try:
        DATA_QUALITY_ERRORS.labels(error_code=code).inc()
    except Exception:
        pass


def inc_transform(transform: str):
    try:
        TRANSFORMS_APPLIED.labels(transform=transform).inc()
    except Exception:
        pass


def set_last_data_rows(n: int):
    try:
        LAST_DATA_ROWS.set(n)
    except Exception:
        pass


def prometheus_metrics_response() -> Tuple[bytes, str]:
    """Return (body_bytes, content_type) for Prometheus scrape."""
    try:
        return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
    except Exception:
        return b"", CONTENT_TYPE_LATEST

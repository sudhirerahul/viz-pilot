# api/index.py
"""
Vercel Serverless Function adapter.

Vercel's Python runtime looks for a variable named `app` (ASGI) or `handler` (WSGI).
FastAPI is ASGI, so we just re-export it as `app`.
"""
import sys
import os

# Ensure project root is on the Python path so `backend.*` imports resolve.
# On Vercel the layout is /vercel/path0/ (project root).
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Force env vars for serverless context
os.environ.setdefault("MOCK_AUTH", "true")
os.environ.setdefault("PROMETHEUS_ENABLED", "false")

# Use /tmp for SQLite on Vercel (filesystem is read-only except /tmp)
if not os.environ.get("DATABASE_URL"):
    os.environ["DATABASE_URL"] = "sqlite:////tmp/viz_agent.db"

# yfinance needs a writable cache directory — default ~/.cache is read-only on Vercel
os.environ.setdefault("XDG_CACHE_HOME", "/tmp/.cache")

# Load .env if present (Vercel injects env vars natively, but this helps local testing)
from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, ".env"), override=True)

# Import the FastAPI app — Vercel looks for the `app` variable
from backend.app import app  # noqa: F401 — Vercel uses this

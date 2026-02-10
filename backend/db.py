# backend/db.py
import os
import json
import datetime
from typing import Optional, Dict, Any

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.exc import SQLAlchemyError

# Default dev DB — on Vercel api/index.py sets DATABASE_URL to /tmp before import
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./viz_agent.db")

def _make_engine(url: str):
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, connect_args=connect_args)

engine = _make_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Import models dynamically to ensure Base metadata has models
# Models will be in backend.models (see file to create)
# from backend import models  # avoid circular import here

def reconfigure(url: str):
    """Reconfigure the DB engine and session factory at runtime (for tests)."""
    global engine, SessionLocal
    engine = _make_engine(url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    # Create tables if they don't exist
    try:
        # import models lazily
        import backend.models as models  # noqa: F841
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        # If db init fails, surface info in logs; don't crash the app at import time
        print("Warning: DB init failed:", e)

def save_request_record(record: Dict[str, Any]) -> Optional[int]:
    """
    Save a request record dict to DB.
    record should include:
      - request_id (str)
      - prompt (str)
      - status (str)
      - response (dict)   # full response returned to client
      - provenance (dict) # optional
      - timestamp (iso str) optional; if missing set now
    Returns the DB id or None on error.
    """
    try:
        db: Session = SessionLocal()
        from backend.models import RequestRecord
        # Always use a proper datetime object (SQLite requires it)
        ts_raw = record.get("timestamp")
        if isinstance(ts_raw, str):
            # Parse ISO format string, strip trailing 'Z' if present
            ts_raw = ts_raw.rstrip("Z")
            try:
                ts = datetime.datetime.fromisoformat(ts_raw)
            except ValueError:
                ts = datetime.datetime.utcnow()
        elif isinstance(ts_raw, datetime.datetime):
            ts = ts_raw
        else:
            ts = datetime.datetime.utcnow()
        rr = RequestRecord(
            request_id=record.get("request_id"),
            prompt=record.get("prompt"),
            status=record.get("status"),
            timestamp=ts,
            response_json=json.dumps(record.get("response") or {}),
            provenance_json=json.dumps(record.get("provenance") or {})
        )
        db.add(rr)
        db.commit()
        db.refresh(rr)
        db.close()
        return rr.id
    except SQLAlchemyError as e:
        # log in production; return None
        try:
            db.rollback()
            db.close()
        except Exception:
            pass
        print("DB save error:", e)
        return None
    except Exception as e:
        print("DB unexpected error:", e)
        return None

def get_request_by_request_id(request_id: str) -> Optional[Dict[str, Any]]:
    """
    Return the stored record as a dict or None.
    """
    try:
        db: Session = SessionLocal()
        from backend.models import RequestRecord
        rr = db.query(RequestRecord).filter(RequestRecord.request_id == request_id).first()
        db.close()
        if not rr:
            return None
        return {
            "id": rr.id,
            "request_id": rr.request_id,
            "prompt": rr.prompt,
            "status": rr.status,
            "timestamp": rr.timestamp.isoformat() if hasattr(rr.timestamp, "isoformat") else rr.timestamp,
            "response": json.loads(rr.response_json or "{}"),
            "provenance": json.loads(rr.provenance_json or "{}")
        }
    except Exception as e:
        print("DB read error:", e)
        return None

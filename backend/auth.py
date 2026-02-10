# backend/auth.py
"""
API key auth and pluggable rate-limiter.

Env vars:
- MOCK_AUTH (default: true) — bypass auth in dev
- API_KEYS — comma-separated allowed keys
- API_KEYS_FILE — optional path to file with one key per line
- RATE_LIMIT_PER_MINUTE (default: 60)
- REDIS_URL — optional, enables Redis-based distributed limiter
"""

import os
import time
import threading
from typing import Optional, Tuple, Dict, Set

# Optional Redis import
try:
    import redis as _redis_mod
except ImportError:
    _redis_mod = None

# Configuration
MOCK_AUTH = os.getenv("MOCK_AUTH", "true").lower() in ("1", "true", "yes")
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))
API_KEYS_ENV = os.getenv("API_KEYS", "")
API_KEYS_FILE = os.getenv("API_KEYS_FILE", "")
REDIS_URL = os.getenv("REDIS_URL", "")


def _load_api_keys() -> Set[str]:
    keys: Set[str] = set()
    if API_KEYS_ENV:
        for k in API_KEYS_ENV.split(","):
            k = k.strip()
            if k:
                keys.add(k)
    if API_KEYS_FILE and os.path.exists(API_KEYS_FILE):
        try:
            with open(API_KEYS_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    k = line.strip()
                    if k:
                        keys.add(k)
        except Exception:
            pass
    return keys


API_KEYS = _load_api_keys()


class InMemoryFixedWindowLimiter:
    """Thread-safe in-memory fixed-window rate limiter (per-process)."""

    def __init__(self, limit_per_minute: int = 60):
        self.limit = limit_per_minute
        self._store: Dict[str, Tuple[int, int]] = {}  # key -> (window_minute, count)
        self._lock = threading.Lock()

    def allow_request(self, api_key: str) -> Tuple[bool, Optional[int]]:
        now = int(time.time())
        window = now // 60
        with self._lock:
            if api_key not in self._store:
                self._store[api_key] = (window, 1)
                return True, self.limit - 1
            wstart, count = self._store[api_key]
            if wstart == window:
                if count >= self.limit:
                    return False, 0
                self._store[api_key] = (wstart, count + 1)
                return True, self.limit - (count + 1)
            else:
                self._store[api_key] = (window, 1)
                return True, self.limit - 1

    def reset(self):
        """Reset all state (useful for tests)."""
        with self._lock:
            self._store.clear()


class RedisFixedWindowLimiter:
    """Redis fixed-window counter using INCR + EXPIRE."""

    def __init__(self, redis_url: str, limit_per_minute: int = 60):
        if _redis_mod is None:
            raise RuntimeError("redis package not installed")
        self.limit = limit_per_minute
        self._client = _redis_mod.from_url(redis_url, decode_responses=True)

    def allow_request(self, api_key: str) -> Tuple[bool, Optional[int]]:
        now = int(time.time())
        window = now // 60
        key = f"rate:{api_key}:{window}"
        try:
            count = self._client.incr(key)
            if count == 1:
                self._client.expire(key, 120)
            if int(count) > self.limit:
                return False, 0
            return True, self.limit - int(count)
        except Exception:
            # Fail open on Redis errors
            return True, None


# Choose limiter instance
_rate_limiter: InMemoryFixedWindowLimiter
if REDIS_URL and _redis_mod is not None:
    try:
        _rate_limiter = RedisFixedWindowLimiter(REDIS_URL, RATE_LIMIT_PER_MINUTE)
    except Exception:
        _rate_limiter = InMemoryFixedWindowLimiter(RATE_LIMIT_PER_MINUTE)
else:
    _rate_limiter = InMemoryFixedWindowLimiter(RATE_LIMIT_PER_MINUTE)


def is_key_allowed(api_key: Optional[str]) -> bool:
    """Check if API key is valid. If MOCK_AUTH=true, always returns True."""
    if MOCK_AUTH:
        return True
    if not api_key:
        return False
    if not API_KEYS:
        return False
    return api_key in API_KEYS


def check_rate_limit(api_key: str) -> Tuple[bool, Optional[int]]:
    """Check and consume quota. Returns (allowed, remaining)."""
    if MOCK_AUTH:
        return True, None
    if not api_key:
        return False, 0
    return _rate_limiter.allow_request(api_key)


def get_limiter() -> InMemoryFixedWindowLimiter:
    """Return the current limiter instance (for testing)."""
    return _rate_limiter

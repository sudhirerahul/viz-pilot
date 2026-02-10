# backend/llm_wrapper.py
"""
Centralized LLM wrapper. Supports OpenAI and Anthropic backends.
Returns a standardized dict:
{
  "text": "<assistant text>",
  "model": "<model used>",
  "response_id": "<model response id if available>",
  "raw": <raw response object>
}

Configuration (env vars):
  LLM_PROVIDER=openai|anthropic   (default: auto-detect based on available keys)
  OPENAI_API_KEY=...
  ANTHROPIC_API_KEY=...
  SPEC_LLM_MODEL=...              (default: depends on provider)
  INTENT_LLM_MODEL=...            (default: depends on provider)
  MOCK_OPENAI=true                (mock mode for dev/tests)

Usage:
  from backend.llm_wrapper import call_llm
  resp = call_llm(messages=..., model="claude-sonnet-4-20250514", timeout=30)
  text = resp["text"]; model = resp["model"]; rid = resp["response_id"]
"""

import os
import time
from typing import Dict, Any, Optional, List

MOCK_OPENAI = os.getenv("MOCK_OPENAI", "true").lower() in ("1", "true", "yes")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()

# Auto-detect provider: explicit > anthropic if key present > openai
_explicit_provider = os.getenv("LLM_PROVIDER", "").strip().lower()
if _explicit_provider in ("anthropic", "claude"):
    LLM_PROVIDER = "anthropic"
elif _explicit_provider in ("openai", "gpt"):
    LLM_PROVIDER = "openai"
elif ANTHROPIC_API_KEY:
    LLM_PROVIDER = "anthropic"
elif OPENAI_API_KEY:
    LLM_PROVIDER = "openai"
else:
    LLM_PROVIDER = "openai"  # fallback, will use mock anyway

# Default models per provider
_ANTHROPIC_DEFAULT = "claude-sonnet-4-20250514"
_OPENAI_DEFAULT = "gpt-4o-mini"

DEFAULT_MODEL = os.getenv(
    "SPEC_LLM_MODEL",
    os.getenv("INTENT_LLM_MODEL",
              _ANTHROPIC_DEFAULT if LLM_PROVIDER == "anthropic" else _OPENAI_DEFAULT)
)


# ---------------------------------------------------------------------------
# Anthropic backend
# ---------------------------------------------------------------------------
def _real_anthropic_chat(messages: List[Dict[str, str]], model: str,
                         max_tokens: int = 4096, temperature: float = 0.0,
                         timeout: int = 30) -> Dict[str, Any]:
    from anthropic import Anthropic

    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    # Anthropic uses a separate system param, not a system message in messages list
    system_text = ""
    chat_messages = []
    for m in messages:
        if m["role"] == "system":
            system_text += m["content"] + "\n"
        else:
            chat_messages.append({"role": m["role"], "content": m["content"]})

    kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": chat_messages,
    }
    if system_text.strip():
        kwargs["system"] = system_text.strip()

    resp = client.messages.create(**kwargs)

    text = ""
    for block in resp.content:
        if hasattr(block, "text"):
            text += block.text

    rid = getattr(resp, "id", None)
    return {"text": text, "model": model, "response_id": rid, "raw": resp}


# ---------------------------------------------------------------------------
# OpenAI backend
# ---------------------------------------------------------------------------
def _real_openai_chat_completion(messages: List[Dict[str, str]], model: str,
                                  max_tokens: int = 4096, temperature: float = 0.0,
                                  timeout: int = 15) -> Dict[str, Any]:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature
    )
    choices = getattr(resp, "choices", [])
    text = choices[0].message.content if choices else ""
    rid = getattr(resp, "id", None)
    return {"text": text, "model": model, "response_id": rid, "raw": resp}


# ---------------------------------------------------------------------------
# Mock backend
# ---------------------------------------------------------------------------
def _mock_llm(messages: List[Dict[str, str]], model: str, **kwargs) -> Dict[str, Any]:
    """
    Deterministic mock used in dev/tests. Returns the concatenation of user messages as text,
    and a deterministic response_id based on time.
    """
    user_texts = [m["content"] for m in messages if m["role"] == "user"]
    text = ("\n\n").join(user_texts)[:1000]  # truncated
    rid = f"mock-{model}-{int(time.time() * 1000)}"
    return {"text": text, "model": model, "response_id": rid, "raw": {"mock": True}}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def call_llm(messages: List[Dict[str, str]], model: Optional[str] = None,
             max_tokens: int = 4096, temperature: float = 0.0,
             timeout: int = 30) -> Dict[str, Any]:
    """
    messages: list of {role, content}
    model: override model string
    Returns: dict with keys 'text','model','response_id','raw'
    """
    model = model or DEFAULT_MODEL
    if MOCK_OPENAI:
        return _mock_llm(messages, model=model, max_tokens=max_tokens,
                         temperature=temperature, timeout=timeout)
    try:
        if LLM_PROVIDER == "anthropic":
            return _real_anthropic_chat(messages, model=model,
                                        max_tokens=max_tokens,
                                        temperature=temperature,
                                        timeout=timeout)
        else:
            return _real_openai_chat_completion(messages, model=model,
                                                max_tokens=max_tokens,
                                                temperature=temperature,
                                                timeout=timeout)
    except Exception as e:
        raise RuntimeError(f"LLM call failed ({LLM_PROVIDER}): {e}")

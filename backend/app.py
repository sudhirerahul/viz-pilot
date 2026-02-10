# backend/app.py
import os
import time
from typing import Optional, List, Dict, Any

# Load .env BEFORE any backend imports (they read env vars at import time)
from dotenv import load_dotenv
load_dotenv(override=True)

from fastapi import FastAPI, Request, HTTPException, Path
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, Response, PlainTextResponse
from pydantic import BaseModel

from backend.orchestrator import VisualizationOrchestrator
from backend import monitoring
from backend import auth as authmod
from backend import db as dbmod

app = FastAPI(title="Visualization Agent API (Orchestrator)")

# Initialize DB tables on startup
dbmod.init_db()

# instantiate orchestrator once
orchestrator = VisualizationOrchestrator(preview_rows=50, max_render_rows=5000)

# ---------------------------------------------------------------------------
# Serve frontend static files
# ---------------------------------------------------------------------------
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR), name="frontend")

API_KEY_HEADER = "x-api-key"


# ---------------------------------------------------------------------------
# Auth + rate-limit middleware (runs first on /api/* paths)
# ---------------------------------------------------------------------------
@app.middleware("http")
async def api_key_and_rate_limit_middleware(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/api/"):
        return await call_next(request)

    # Extract API key
    api_key = request.headers.get(API_KEY_HEADER)
    if not authmod.is_key_allowed(api_key):
        return JSONResponse(status_code=401, content={"detail": "Missing or invalid API key"})

    # Rate limit check (synchronous limiter — no await needed)
    allowed, remaining = authmod.check_rate_limit(api_key or "")
    if not allowed:
        resp = JSONResponse(
            status_code=429,
            content={
                "request_id": None,
                "status": "error",
                "error_code": "E_RATE_LIMIT",
                "message": "Rate limit exceeded"
            }
        )
        resp.headers["Retry-After"] = "60"
        return resp

    return await call_next(request)


# ---------------------------------------------------------------------------
# Metrics middleware
# ---------------------------------------------------------------------------
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.time()
    endpoint = request.url.path
    method = request.method
    status = "500"
    try:
        response = await call_next(request)
        status = str(response.status_code)
        return response
    except Exception:
        monitoring.logger.exception("Unhandled exception in request", extra={"path": endpoint})
        raise
    finally:
        monitoring.observe_request(start, endpoint, method, status)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------
class VizRequest(BaseModel):
    prompt: str


class AutofixRequest(BaseModel):
    prompt: str
    autofix: Optional[Dict[str, Any]] = None
    transforms: Optional[List[Dict[str, Any]]] = None


class ReplayRequest(BaseModel):
    request_id: str
    override_prompt: Optional[str] = None
    autofix: Optional[Dict[str, Any]] = None
    transforms: Optional[List[Dict[str, Any]]] = None
    model_override: Optional[str] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post("/api/viz")
async def create_viz(req: VizRequest, request: Request):
    """
    POST /api/viz
    Body: { "prompt": "..." }
    """
    monitoring.logger.info("Received /api/viz request", extra={"prompt_preview": (req.prompt[:200] if req.prompt else "")})
    try:
        resp = orchestrator.handle_request(req.prompt)
        return JSONResponse(status_code=200, content=resp)
    except Exception as e:
        monitoring.logger.exception("Unexpected error in /api/viz handler")
        return JSONResponse(
            status_code=500,
            content={
                "request_id": None,
                "status": "error",
                "error_code": "E_INTERNAL",
                "message": "Internal server error",
                "details": {"exception": str(e)},
            },
        )


@app.post("/api/viz/autofix")
async def api_autofix(req: AutofixRequest, request: Request):
    """
    POST /api/viz/autofix
    Body: { "prompt": "...", "autofix": {"method":"decimate"}, "transforms": [...] }
    """
    monitoring.logger.info("Received /api/viz/autofix request", extra={"prompt_preview": (req.prompt[:200] if req.prompt else "")})
    try:
        resp = orchestrator.handle_autofix(
            prompt=req.prompt,
            autofix=req.autofix,
            explicit_transforms=req.transforms
        )
        return JSONResponse(status_code=200, content=resp)
    except Exception as e:
        monitoring.logger.exception("Unexpected error in /api/viz/autofix handler")
        return JSONResponse(
            status_code=500,
            content={
                "request_id": None,
                "status": "error",
                "error_code": "E_INTERNAL",
                "message": "Internal server error",
                "details": {"exception": str(e)},
            },
        )


@app.get("/api/viz/history/{request_id}")
def get_viz_history(request_id: str = Path(..., description="Request ID to fetch")):
    """
    GET /api/viz/history/{request_id}
    Fetch a stored request record by request_id.
    """
    rec = dbmod.get_request_by_request_id(request_id)
    if not rec:
        return JSONResponse(
            status_code=404,
            content={
                "request_id": request_id,
                "status": "error",
                "error_code": "E_NOT_FOUND",
                "message": "Request not found"
            }
        )
    return JSONResponse(status_code=200, content={"request_id": request_id, "status": "success", "record": rec})


@app.post("/api/viz/replay")
async def api_replay(req: ReplayRequest):
    """
    POST /api/viz/replay
    Body: {
      "request_id": "...",
      "override_prompt": "...",     # optional
      "autofix": {"method":"decimate"},  # optional
      "transforms": [ ... ],        # optional explicit transforms
      "model_override": "gpt-4o"    # optional model override
    }
    Returns same contract as /api/viz and persists the replay (audit).
    """
    monitoring.logger.info("Received /api/viz/replay request", extra={"request_id": req.request_id})
    try:
        resp = orchestrator.handle_replay(
            request_id=req.request_id,
            override_prompt=req.override_prompt,
            autofix=req.autofix,
            explicit_transforms=req.transforms,
            model_override=req.model_override
        )
        return JSONResponse(status_code=200, content=resp)
    except Exception as e:
        monitoring.logger.exception("Unexpected error in /api/viz/replay handler")
        return JSONResponse(
            status_code=500,
            content={
                "request_id": None,
                "status": "error",
                "error_code": "E_INTERNAL",
                "message": "Internal server error",
                "details": {"exception": str(e)},
            },
        )


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    if not monitoring.PROMETHEUS_ENABLED:
        return PlainTextResponse("Prometheus disabled", status_code=404)
    payload, content_type = monitoring.prometheus_metrics_response()
    return Response(content=payload, media_type=content_type)

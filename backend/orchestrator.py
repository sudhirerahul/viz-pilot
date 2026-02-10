# backend/orchestrator.py
import uuid
import time
import copy
import datetime
from typing import Dict, Any, Optional, List

# Import modules (not bare functions) so monkeypatching in tests works correctly
import backend.processors.intent_parser as _intent_parser
import backend.connectors.yfinance_connector as _yf_connector
import backend.processors.spec_generator as _spec_generator
import backend.processors.normalizer as _normalizer
import backend.quality as _quality
from backend.validator import validate_vega_spec
from backend import monitoring
from backend import db as dbmod

# Error codes reused from MVP
E_INTENT_PARSE_FAIL = "E_INTENT_PARSE_FAIL"
E_NO_DATA = "E_NO_DATA"
E_BAD_DATA = "E_BAD_DATA"
E_VEGA_INVALID = "E_VEGA_INVALID"


class VisualizationOrchestrator:
    def __init__(self, preview_rows: int = 50, max_render_rows: int = 5000):
        self.preview_rows = preview_rows
        self.max_render_rows = max_render_rows

    def _make_request_id(self) -> str:
        return str(uuid.uuid4())

    def _now_iso(self) -> str:
        return datetime.datetime.utcnow().isoformat() + "Z"

    def _persist_and_return(self, resp: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        """Persist the response to DB (best-effort) and return it."""
        try:
            to_save = {
                "request_id": resp.get("request_id"),
                "prompt": prompt,
                "status": resp.get("status"),
                "response": resp,
                "provenance": resp.get("provenance", {}),
                "timestamp": None
            }
            dbmod.save_request_record(to_save)
        except Exception:
            # Never fail the API if DB write fails
            pass
        return resp

    def _fetch_data(self, task: Dict[str, Any], provenance: Dict[str, Any]) -> tuple:
        """Shared data-fetching logic. Returns (data_preview, error_response_or_None)."""
        data_preview: List[Dict[str, Any]] = []
        request_id = provenance["request_id"]

        if task.get("symbol"):
            symbol = task["symbol"]
            try:
                conn = _yf_connector.fetch_ticker_preview(
                    symbol=symbol, period="6mo", interval="1d", max_rows=self.preview_rows
                )
                data_preview = conn.get("table", [])
                provenance["sources"].append({
                    "source": conn.get("metadata", {}).get("source", "yfinance"),
                    "symbol_or_key": conn.get("metadata", {}).get("symbol"),
                    "fetched_at": conn.get("metadata", {}).get("fetched_at"),
                    "url_or_api_endpoint": None,
                    "http_status": 200,
                    "raw_sample": conn.get("raw", "")[:1000]
                })
            except Exception as e:
                return [], {
                    "request_id": request_id, "status": "error",
                    "error_code": E_NO_DATA,
                    "message": f"No data for symbol {symbol}: {e}",
                    "details": {}
                }
        elif task.get("dataset_key"):
            dataset_key = task["dataset_key"]
            data_preview = [
                {"date": "2010-01-01", "value": 217.0},
                {"date": "2010-02-01", "value": 218.5},
                {"date": "2010-03-01", "value": 219.3}
            ]
            provenance["sources"].append({
                "source": "mock-fred",
                "symbol_or_key": dataset_key,
                "fetched_at": self._now_iso(),
                "url_or_api_endpoint": None,
                "http_status": 200,
                "raw_sample": ""
            })
        else:
            return [], {
                "request_id": request_id, "status": "error",
                "error_code": E_NO_DATA,
                "message": "No symbol or dataset_key found in parsed intent",
                "details": {}
            }

        return data_preview, None

    def _generate_and_validate(self, task: Dict[str, Any], data_preview: List[Dict[str, Any]],
                                provenance: Dict[str, Any]) -> Dict[str, Any]:
        """Shared spec-generation + validation logic. Returns final response dict."""
        request_id = provenance["request_id"]

        if not data_preview or len(data_preview) == 0:
            monitoring.inc_data_quality_error("empty_data")
            return {
                "request_id": request_id, "status": "error",
                "error_code": E_BAD_DATA, "message": "No data available",
                "details": {}
            }

        monitoring.set_last_data_rows(len(data_preview))

        # Spec generation
        start_spec = time.time()
        try:
            spec_payload = _spec_generator.generate_vega_spec(task, data_preview)
            llm_meta = spec_payload.pop("_llm_meta", {})
            provenance["llm_calls"].append({
                "role": "spec_generator",
                "model": llm_meta.get("model", "mock-or-configured"),
                "prompt_hash": llm_meta.get("prompt_hash"),
                "response_id": llm_meta.get("response_id"),
                "timestamp": self._now_iso()
            })
            monitoring.observe_spec_gen(start_spec, "success")
        except Exception as e:
            monitoring.observe_spec_gen(start_spec, "fail")
            return {
                "request_id": request_id, "status": "error",
                "error_code": E_VEGA_INVALID,
                "message": f"Spec generation failed: {e}",
                "details": {}
            }

        vega_spec = spec_payload.get("vega_lite_spec", {})

        # Always inject real data into the spec — LLMs often emit placeholder
        # data references like {"url": "data"} instead of inline values
        if isinstance(vega_spec, dict) and data_preview:
            vega_spec["data"] = {"values": data_preview[:self.preview_rows]}

        caption = spec_payload.get("explanation", "") or spec_payload.get("__caption__", task.get("goal") or "Chart")
        explanation = spec_payload.get("explanation", caption)
        notes = spec_payload.get("__notes__", "")

        # Merge spec-generator provenance metadata (connectors_required, api_keys_required)
        sg_prov = spec_payload.get("provenance", {})
        if isinstance(sg_prov, dict):
            if sg_prov.get("connectors_required"):
                provenance["connectors_required"] = sg_prov["connectors_required"]
            if sg_prov.get("api_keys_required"):
                provenance["api_keys_required"] = sg_prov["api_keys_required"]
            if sg_prov.get("notes"):
                provenance["spec_generator_notes"] = sg_prov["notes"]

        # Validation
        validation = validate_vega_spec(vega_spec, data_preview, max_rows=self.max_render_rows)
        provenance["validator"] = {
            "ok": validation.get("valid", False),
            "errors": validation.get("errors", []),
            "warnings": validation.get("warnings", [])
        }

        if not validation.get("valid", False):
            for err in validation.get("errors", []):
                monitoring.inc_validation_failure(err[:80])
            return {
                "request_id": request_id,
                "status": "error",
                "error_code": E_VEGA_INVALID,
                "message": "Vega-Lite spec failed validation",
                "details": {"validator_errors": validation.get("errors", [])},
                "provenance": provenance
            }

        return {
            "request_id": request_id,
            "status": "success",
            "spec": vega_spec,
            "data_preview": data_preview[:self.preview_rows],
            "provenance": provenance,
            "caption": caption,
            "explanation": explanation,
            "notes": notes
        }

    def handle_request(self, prompt: str) -> Dict[str, Any]:
        """
        Full synchronous flow:
        1. Intent parse (LLM)
        2. Data fetch (connector)
        3. Data quality checks (light)
        4. Spec generation (LLM)
        5. Spec validation
        6. Assemble response with provenance
        """
        request_id = self._make_request_id()
        provenance = {
            "request_id": request_id,
            "sources": [],
            "transforms": [],
            "llm_calls": [],
            "validator": {"ok": True, "errors": []}
        }

        # 1) Intent parsing
        try:
            task = _intent_parser.llm_parse_intent(prompt)
            provenance["llm_calls"].append({
                "role": "intent_parser",
                "model": "mock-or-configured",
                "prompt_hash": None,
                "response_id": None,
                "timestamp": self._now_iso()
            })
            monitoring.inc_intent_parse("success")
        except Exception as e:
            monitoring.inc_intent_parse("fail")
            return self._persist_and_return({
                "request_id": request_id, "status": "error",
                "error_code": E_INTENT_PARSE_FAIL, "message": str(e),
                "details": {}
            }, prompt)

        if task.get("clarify"):
            monitoring.inc_intent_parse("clarify")
            return self._persist_and_return({
                "request_id": request_id,
                "status": "clarify_needed",
                "clarify_question": task["clarify"]["question"]
            }, prompt)

        # 2) Data fetch
        data_preview, fetch_err = self._fetch_data(task, provenance)
        if fetch_err:
            return self._persist_and_return(fetch_err, prompt)

        # 3) Quick quality check
        if not data_preview or len(data_preview) == 0:
            monitoring.inc_data_quality_error("empty_data")
            return self._persist_and_return({
                "request_id": request_id, "status": "error",
                "error_code": E_BAD_DATA,
                "message": "Connector returned empty data",
                "details": {}
            }, prompt)

        monitoring.set_last_data_rows(len(data_preview))

        # 4-6) Generate spec, validate, assemble
        return self._persist_and_return(
            self._generate_and_validate(task, data_preview, provenance), prompt
        )

    def handle_autofix(
        self,
        prompt: str,
        autofix: Optional[Dict[str, Any]] = None,
        explicit_transforms: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Server-side autofix flow: parse intent, fetch data, apply transforms/autofix,
        then generate spec and validate.

        Args:
            prompt: original user prompt
            autofix: {"method": "decimate" | "aggregate_monthly"} or None
            explicit_transforms: list of transform dicts to apply

        Returns same response shape as handle_request.
        """
        request_id = self._make_request_id()
        provenance = {
            "request_id": request_id,
            "sources": [],
            "transforms": [],
            "llm_calls": [],
            "validator": {"ok": True, "errors": []}
        }

        # 1) Parse intent
        try:
            task = _intent_parser.llm_parse_intent(prompt)
            provenance["llm_calls"].append({
                "role": "intent_parser",
                "model": "mock-or-configured",
                "prompt_hash": None,
                "response_id": None,
                "timestamp": self._now_iso()
            })
            monitoring.inc_intent_parse("success")
        except Exception as e:
            monitoring.inc_intent_parse("fail")
            return self._persist_and_return({
                "request_id": request_id, "status": "error",
                "error_code": E_INTENT_PARSE_FAIL, "message": str(e),
                "details": {}
            }, prompt)

        if task.get("clarify"):
            return self._persist_and_return({
                "request_id": request_id,
                "status": "clarify_needed",
                "clarify_question": task["clarify"]["question"]
            }, prompt)

        # 2) Fetch data
        data_preview, fetch_err = self._fetch_data(task, provenance)
        if fetch_err:
            return self._persist_and_return(fetch_err, prompt)

        if not data_preview or len(data_preview) == 0:
            return self._persist_and_return({
                "request_id": request_id, "status": "error",
                "error_code": E_BAD_DATA,
                "message": "Connector returned empty data",
                "details": {}
            }, prompt)

        # 3) Apply explicit transforms if provided
        if explicit_transforms:
            try:
                transformed_rows, applied = _normalizer.apply_transforms(data_preview, explicit_transforms)
                provenance["transforms"].extend(applied)
                data_preview = transformed_rows
                for t in applied:
                    monitoring.inc_transform(t)
            except Exception as e:
                return self._persist_and_return({
                    "request_id": request_id, "status": "error",
                    "error_code": E_BAD_DATA,
                    "message": f"Applying transforms failed: {e}",
                    "details": {}
                }, prompt)

        # 4) If no explicit transforms, attempt quality-based autofix
        if not explicit_transforms and autofix:
            method = autofix.get("method", "decimate") if isinstance(autofix, dict) else "decimate"
            qconf = _quality.QualityConfig(
                max_render_rows=self.max_render_rows,
                allow_autofix_downsample=True,
                downsample_method=method
            )
            try:
                fixed_rows, actions, msgs = _quality.attempt_autofix(data_preview, qconf)
            except Exception as e:
                return self._persist_and_return({
                    "request_id": request_id, "status": "error",
                    "error_code": E_BAD_DATA,
                    "message": f"Autofix failed: {e}",
                    "details": {}
                }, prompt)
            if actions:
                data_preview = fixed_rows
                provenance["transforms"].extend(actions)
                for a in actions:
                    monitoring.inc_transform(a)
                # Check if quality is still bad after fix
                final_report = msgs[-1] if msgs else {}
                if final_report and not final_report.get("ok", True):
                    return self._persist_and_return({
                        "request_id": request_id,
                        "status": "error",
                        "error_code": E_BAD_DATA,
                        "message": "Data failed quality checks after autofix",
                        "details": {"quality_report": final_report},
                        "provenance": provenance
                    }, prompt)

        if not data_preview or len(data_preview) == 0:
            return self._persist_and_return({
                "request_id": request_id, "status": "error",
                "error_code": E_BAD_DATA,
                "message": "No data available after autofix",
                "details": {}
            }, prompt)

        # 5-6) Generate spec, validate, assemble
        return self._persist_and_return(
            self._generate_and_validate(task, data_preview, provenance), prompt
        )

    def handle_replay(
        self,
        request_id: str,
        override_prompt: Optional[str] = None,
        autofix: Optional[Dict[str, Any]] = None,
        explicit_transforms: Optional[List[Dict[str, Any]]] = None,
        model_override: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Replay a previously-stored request identified by request_id.

        Loads stored record, determines base prompt, re-executes pipeline,
        and injects parent_request_id into provenance for audit trail.
        """
        # Load original record
        original = dbmod.get_request_by_request_id(request_id)
        if not original:
            return {
                "request_id": None,
                "status": "error",
                "error_code": "E_NOT_FOUND",
                "message": f"Original request {request_id} not found",
                "details": {}
            }

        base_prompt = override_prompt if override_prompt else original.get("prompt", "")

        # Execute the flow
        if autofix or explicit_transforms:
            resp = self.handle_autofix(
                prompt=base_prompt,
                autofix=autofix,
                explicit_transforms=explicit_transforms
            )
        else:
            resp = self.handle_request(base_prompt)

        # Inject parent_request_id into provenance for audit trail
        try:
            if isinstance(resp, dict):
                prov = resp.setdefault("provenance", {})
                prov["parent_request_id"] = request_id
                # Persist the replay record (best-effort)
                try:
                    dbmod.save_request_record({
                        "request_id": resp.get("request_id"),
                        "prompt": base_prompt,
                        "status": resp.get("status"),
                        "response": resp,
                        "provenance": prov,
                        "timestamp": None
                    })
                except Exception:
                    pass
        except Exception:
            pass

        return resp

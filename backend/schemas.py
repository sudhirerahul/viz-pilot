# backend/schemas.py
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, field_validator, ConfigDict
from datetime import datetime

class ColumnSpec(BaseModel):
    name: str
    type: str  # "temporal" | "quantitative" | "categorical"

class TimeSeriesRow(BaseModel):
    model_config = ConfigDict(extra="allow")

    # allow flexible fields; date is canonical but many series will have other numeric columns
    date: Optional[str] = None
    Open: Optional[float] = None
    High: Optional[float] = None
    Low: Optional[float] = None
    Close: Optional[float] = None
    Adj_Close: Optional[float] = None
    Volume: Optional[float] = None
    value: Optional[float] = None
    # capture any additional columns
    others: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("date")
    @classmethod
    def date_must_be_iso(cls, v):
        if v is None:
            return v
        # basic format check YYYY-MM-DD
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except Exception as e:
            raise ValueError("date must be YYYY-MM-DD") from e
        return v

class TimeSeriesTable(BaseModel):
    columns: List[ColumnSpec]
    rows: List[TimeSeriesRow]
    metadata: Dict[str, Any]

class LLMCallMeta(BaseModel):
    role: str
    model: str
    prompt_hash: Optional[str] = None
    response_id: Optional[str] = None

class Provenance(BaseModel):
    request_id: str
    sources: List[Dict[str, Any]]
    transforms: List[str] = []
    llm_calls: List[LLMCallMeta] = []
    validator: Dict[str, Any]

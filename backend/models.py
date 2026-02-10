# backend/models.py
from sqlalchemy import Column, Integer, String, DateTime, Text
import datetime

from backend.db import Base


class RequestRecord(Base):
    __tablename__ = "request_records"

    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(String(128), unique=True, index=True, nullable=False)
    prompt = Column(Text, nullable=True)
    status = Column(String(32), nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    response_json = Column(Text, nullable=True)
    provenance_json = Column(Text, nullable=True)

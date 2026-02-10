# syntax=docker/dockerfile:1
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 8000

ENV PYTHONUNBUFFERED=1 \
    LOG_LEVEL=INFO \
    PROMETHEUS_ENABLED=true \
    MOCK_OPENAI=true \
    MOCK_SPEC_GENERATOR=true \
    ENVIRONMENT=development

CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]

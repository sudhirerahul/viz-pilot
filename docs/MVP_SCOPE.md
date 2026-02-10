# MVP Scope — Visualization Agent

## 1.1 Big Picture

A minimal web service that accepts a plain-English prompt, fetches public time-series data for a requested symbol or public dataset, returns a validated Vega-Lite spec plus a data preview and provenance, and renders the chart in the browser.

---

## 1.2 Supported Prompt Types (MVP)

The app supports **ticker-based** and **dataset-key** prompts only:

### 1. Ticker time-series prompts
Natural language requests about a stock/time-series identified by ticker (e.g., TSLA, AAPL).

- "Plot TSLA daily close since 2024-01-01 with a 30-day moving average."
- "Show AAPL adjusted close for last 12 months as a line chart."

### 2. Macro dataset prompts (named public series)
Pre-mapped public datasets like FRED series keys.

- "Plot US CPIAUCSL monthly since 2010-01-01."
- "Show unemployment rate (UNRATE) from 2015-01-01 to present."

### 3. Simple comparison prompts (two tickers)
Compare up to 2 tickers on the same axis or by rebasing.

- "Compare AAPL and MSFT daily close for the last 2 years."

### Not supported in MVP
Free web scraping/search, arbitrary website table extraction, user-uploaded files, multi-series (>2) comparisons, categorical survey data.

---

## 1.3 Supported Chart Types (MVP)

| Chart Type | Description |
|------------|-------------|
| `line` | Default for time-series |
| `bar` | For aggregated periods (quarterly/annual) |
| `area` | Same as line with filled area |
| `scatter` | For X vs Y numeric comparisons (limited) |
| `auto` | System chooses best type given task |

Front-end allows switching among these after generation.

---

## 1.4 Supported Data Sources (MVP)

Two deterministic public sources:

### 1. yfinance (via `yfinance` Python package)
- Stock ticker historical prices (daily).
- Fields: `Date`, `Open`, `High`, `Low`, `Close`, `Adj Close`, `Volume`.
- Default interval: `1d`. Support `1d` and `1mo`.

### 2. FRED API (via `fredapi` or direct REST)
- Macroeconomic series.
- Small mapping/config of allowed series keys to friendly names in repo.

No scraping or third-party paid feeds in MVP.

---

## 1.5 Supported Transforms (MVP)

Server-side deterministic transforms only:

| Transform | Description |
|-----------|-------------|
| `moving_average` | Window in days (e.g., 30) |
| `rebased_index` | Set first value = 100 |
| `resample` | Aggregate daily to monthly |
| `pct_change` | Period-over-period percent change |

Transforms are applied by the Extractor/Normalizer (P2) — never by LLM.

---

## 1.6 Intent Parsing (MVP)

- Use OpenAI function-calling to produce strict TASK JSON.
- Required fields: `goal`, `chart_type`, `metrics`, `symbol` or `dataset_key`, `time_range`, `transforms`.
- If ambiguous, parser returns `clarify.question`.
- Malformed JSON: orchestrator retries once; if still malformed -> `E_INTENT_PARSE_FAIL`.

MVP metric mapping (tickers): `["Close", "Adj Close", "Volume", "Open", "High", "Low"]`
FRED datasets use whatever fields the API returns (usually `value` with dates).

---

## 1.7 API: Exact MVP Endpoints

### `POST /api/viz`

**Request:**
```json
{ "prompt": "string" }
```

**Response (success):**
```json
{
  "request_id": "uuid",
  "status": "success",
  "spec": { },
  "data_preview": [ ],
  "provenance": { },
  "caption": "string"
}
```

**Response (clarify_needed):**
```json
{
  "request_id": "uuid",
  "status": "clarify_needed",
  "clarify_question": "string"
}
```

**Response (error):**
```json
{
  "request_id": "uuid",
  "status": "error",
  "error_code": "E_CODE",
  "message": "explanation",
  "details": {}
}
```

---

## 1.8 Hard Limits & Constraints (MVP)

| Constraint | Value |
|------------|-------|
| Max tickers per request | 2 |
| Max rows to Viz LLM preview | 10 |
| Max rows in `data_preview` response | 50 |
| Row cap for rendered chart | 5,000 points |
| Transforms supported | See 1.5 |
| Allowed Vega-Lite marks | `line`, `area`, `bar`, `point` |
| Intent parse LLM timeout | 10s |
| Connector fetch timeout | 20s |
| Spec generation LLM timeout | 10s |
| Total `/api/viz` timeout | 30s |
| Rate limit (dev) | 30 req/min per API key |

---

## 1.9 Error Codes (MVP)

| Code | Meaning |
|------|---------|
| `E_INTENT_PARSE_FAIL` | Intent parser failed after retry |
| `E_CLARIFY_REQUIRED` | Parser returned clarify (return as `clarify_needed`) |
| `E_NO_DATA` | Connector returned no data |
| `E_BAD_DATA` | Normalized table failed (too many NaNs or missing metric) |
| `E_TOO_MANY_POINTS` | Exceeds row cap, cannot auto-aggregate |
| `E_VEGA_INVALID` | Vega-Lite spec invalid after retry |
| `E_RATE_LIMIT` | Connector or LLM rate-limited |
| `E_INTERNAL` | Unexpected error (includes correlation id) |

Every error response includes `request_id` and actionable `details`.

---

## 1.10 Provenance Contents (MVP)

```json
{
  "sources": [
    {
      "source": "yfinance",
      "symbol_or_key": "TSLA",
      "fetched_at": "2026-02-06T14:22:11Z",
      "url_or_api_endpoint": null,
      "http_status": 200,
      "raw_sample": "Date,Close\n2024-01-02,248.42\n2024-01-03,238.45\n2024-01-04,237.93"
    }
  ],
  "transforms": ["date_normalization", "moving_average_30"],
  "llm_calls": [
    { "role": "intent_parser", "model": "gpt-4o-mini", "prompt_hash": "sha256...", "response_id": "..." },
    { "role": "spec_generator", "model": "gpt-4o-mini", "prompt_hash": "sha256...", "response_id": "..." }
  ],
  "validator": { "ok": true, "errors": [] }
}
```

---

## 1.11 UX / Frontend MVP

Single-page UI with:
- Prompt input (textarea) + example prompts
- "Generate" button
- Chart area (`vega-embed`)
- Panel: Data preview (first 50 rows), Provenance (source list + timestamps), Spec JSON (collapsible)
- Buttons: Export CSV (`data_preview`), Export PNG (chart)
- Edit chart type toggle (client-side, re-renders spec with same data)

---

## 1.12 Acceptance Criteria

The MVP is working if **all** hold:

1. **Intent Parsing**: For 20 diverse test prompts in `tests/golden_prompts.json`, the Intent Parser returns valid TASK JSON or a clarifying question.
2. **Data Fetch & Normalize**: For 10 sample tickers and 10 FRED series, connector returns normalized tables with date and requested metric.
3. **Spec Generation & Validation**: For each TASK JSON in acceptance tests, Viz Spec Generator produces a Vega-Lite spec that passes the Spec Validator and renders without runtime errors.
4. **Provenance Present**: Every successful response includes a complete provenance object.
5. **Latency**: 90% of `/api/viz` requests return within 10s under dev load.
6. **Errors**: Failure modes return defined error codes with clear messages.
7. **Security**: No API keys in responses; UI output sanitized for XSS.

---

## 1.13 Minimal Test Cases (MVP)

| # | Prompt | Expected |
|---|--------|----------|
| 1 | Plot TSLA daily close since 2024-01-01 with 30-day moving average | line chart, Close + MA_30 |
| 2 | Show AAPL adjusted close last 12 months as a line chart | line chart, Adj Close |
| 3 | Compare AAPL and MSFT daily close for the last 2 years | multi-line chart |
| 4 | Plot CPIAUCSL monthly since 2010-01-01 | line chart, FRED data |
| 5 | Plot TSLA volume last 6 months as bar chart | bar chart, Volume |
| 6 | Show growth of Apple | `clarify_needed` with question |

Each test asserts the chain: parse -> fetch -> normalize -> transform -> spec -> validate -> renderable.

---

## 1.14 Implementation Notes

- LLM usage limited to intent parse + spec generation; all numeric transforms and data retrieval deterministic in server code.
- No scraping of paywalled sites.
- Cache connector results (TTL default 10 minutes).
- Start with FastAPI backend, single-page static frontend with `vega-embed`.

---

## 1.15 Post-MVP Success Signals

| Metric | Target |
|--------|--------|
| Time-to-first-chart (median) | < 6s |
| User satisfaction (beta) | >= 80% positive |
| Clarifying question rate | < 10% for common prompts |
| Error rate (beta) | < 3% |

# Low-Level Design (LLD) — Autonomous Payment Reconciliation Agent

| Field | Value |
|---|---|
| Document type | Low-Level Design (LLD) |
| Project | Autonomous Payment Reconciliation Agent |
| Version | 1.0 |
| Date | 2026-05-01 |
| Author | Nikhil Pujar |
| Status | Draft for review |
| Source HLD | `doc.md` (Solution Design) |

---

## 1. Purpose & Scope

### 1.1 Purpose
This Low-Level Design (LLD) translates the high-level solution design (`doc.md`) into implementation-ready specifications: module boundaries, class signatures, table-level schemas, API contracts, internal protocols, algorithms, error handling, and instrumentation. It is the single source of truth for engineers writing code in `src/recon/*`.

### 1.2 Scope
In scope:
- Module-by-module design for ingestion, rules engine, LLM agent, PII redaction, ledger I/O, API, observability, and CLI.
- Database schema (DDL, indexes, query plans), Protobuf schemas, REST API contract.
- Algorithms (rules ordering, agent orchestration, redaction, idempotent writes, transactional outbox).
- Configuration, error taxonomy, retry/DLQ semantics, security boundaries.

Out of scope:
- Operational runbooks (covered in `RUNBOOK.md`).
- Marketing/positioning content (covered in `README.md`).
- NPCI's actual file specification (we explicitly simulate it).

### 1.3 Audience
Backend engineers, SRE/observability owners, security reviewers, and AI/ML reviewers of the agent prompts and tools.

### 1.4 Definitions
- **UDIR** — UPI Daily Issuer Report (simulated NPCI settlement file).
- **RRN** — Retrieval Reference Number, 12-digit transaction identifier.
- **UTR** — Unique Transaction Reference, bank-issued identifier.
- **PA/PG** — Payment Aggregator / Payment Gateway.
- **case_uid** — Deterministic UUID derived from `(file_id, line_no)`.
- **SLO** — Service Level Objective.

---

## 2. System Context (recap)

```
NPCI ─▶ SFTP ─▶ Watcher ─▶ Parser ─▶ Rules ─▶ Agent ─▶ FastAPI ─▶ Reviewer
              (Kafka transport between stages; Postgres = source of truth)
```

The system processes ~100k settlement lines per day. Rules deterministically resolve ≥90%; the LLM agent investigates the ~5–10% residue and proposes resolutions for human approval. Every `(file_id, line_no)` produces exactly one `recon_case`.

---

## 3. Module Catalog

| # | Module | Path | Owner of | Stateless? |
|---|---|---|---|---|
| 1 | SFTP Watcher | `src/recon/ingest/sftp_watcher.py` | `processed_files`, emits `FileArrived` | Yes |
| 2 | Parser | `src/recon/ingest/parser.py` | `settlement_lines` | Yes |
| 3 | Rules Engine | `src/recon/rules/engine.py` + `rules/` | Creates `recon_cases` | Yes |
| 4 | Agent Orchestrator | `src/recon/agent/orchestrator.py` | Updates `recon_cases`, writes `agent_traces` | Yes |
| 5 | Agent Tools | `src/recon/agent/tools.py` | Read-only DB tools (via redactor) | Yes |
| 6 | PII Redactor / Tokenizer | `src/recon/pii/{redactor,tokenizer}.py` | `pan_vault` | Yes |
| 7 | Ledger Repo | `src/recon/ledger/repo.py` | DB I/O abstraction | Yes |
| 8 | Outbox Publisher | `src/recon/ledger/outbox.py` | Drains `outbox` → Kafka | Yes |
| 9 | FastAPI Service | `src/recon/api/main.py` | `/cases`, `/healthz`, approval flow | Yes |
| 10 | CLI | `src/recon/cli.py` | Backfill, DLQ replay | Yes |
| 11 | Observability | `src/recon/obs/{otel,metrics}.py` | OTel + Prometheus exporters | Yes |
| 12 | Eval Harness | `src/recon/eval/harness.py` | Golden case execution | Yes |

All modules are stateless processes; Postgres is the only stateful service.

---

## 4. Database Design

### 4.1 Tables (full DDL)

#### 4.1.1 `pg_transactions` — internal ledger
```sql
CREATE TABLE pg_transactions (
  id           BIGSERIAL PRIMARY KEY,
  txn_id       TEXT UNIQUE NOT NULL,
  rrn          TEXT,
  utr          TEXT,
  payer_vpa    TEXT,
  payee_vpa    TEXT,
  amount_paise BIGINT NOT NULL CHECK (amount_paise >= 0),
  status       TEXT   NOT NULL CHECK (status IN ('INITIATED','SUCCESS','FAILED','TIMEOUT','PARTIAL_REVERSED')),
  created_at   TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_pg_txn_rrn         ON pg_transactions(rrn);
CREATE INDEX idx_pg_txn_utr         ON pg_transactions(utr);
CREATE INDEX idx_pg_txn_rrn_utr     ON pg_transactions(rrn, utr);
CREATE INDEX idx_pg_txn_created_at  ON pg_transactions(created_at);
```

#### 4.1.2 `settlement_lines` — parsed file rows
```sql
CREATE TABLE settlement_lines (
  id             BIGSERIAL PRIMARY KEY,
  file_id        TEXT NOT NULL,
  line_no        INT  NOT NULL,
  rrn            TEXT,
  utr            TEXT,
  amount_paise   BIGINT,
  fee_paise      BIGINT,
  net_paise      BIGINT,
  status         TEXT,
  raw            JSONB,
  ingested_at    TIMESTAMPTZ DEFAULT now(),
  UNIQUE (file_id, line_no)
);
CREATE INDEX idx_sl_rrn_utr ON settlement_lines(rrn, utr);
CREATE INDEX idx_sl_file_id ON settlement_lines(file_id);
```

#### 4.1.3 `recon_cases` — resolution state
```sql
CREATE TABLE recon_cases (
  id              BIGSERIAL PRIMARY KEY,
  case_uid        UUID UNIQUE NOT NULL,
  settlement_line BIGINT REFERENCES settlement_lines(id),
  pg_transaction  BIGINT REFERENCES pg_transactions(id),
  match_type      TEXT NOT NULL CHECK (match_type IN
    ('EXACT','TOLERANCE','MISSING_LEG','AMOUNT_MISMATCH','DUPLICATE','UNKNOWN')),
  confidence      NUMERIC(4,3) CHECK (confidence BETWEEN 0 AND 1),
  resolution      TEXT CHECK (resolution IN
    ('PROPOSED','APPROVED','REJECTED','AUTO_RESOLVED','ESCALATE')),
  resolved_by     TEXT,
  notes           JSONB,
  created_at      TIMESTAMPTZ DEFAULT now(),
  resolved_at     TIMESTAMPTZ
);
CREATE INDEX idx_rc_resolution ON recon_cases(resolution);
CREATE INDEX idx_rc_match_type ON recon_cases(match_type);
CREATE INDEX idx_rc_created_at ON recon_cases(created_at);
```

#### 4.1.4 `agent_traces` — LLM call trace store
```sql
CREATE TABLE agent_traces (
  id          BIGSERIAL PRIMARY KEY,
  case_uid    UUID REFERENCES recon_cases(case_uid),
  prompt_hash TEXT,
  model       TEXT,
  tools_used  TEXT[],
  steps       JSONB,
  total_ms    INT,
  cost_usd    NUMERIC(10,6),
  created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_at_case_uid   ON agent_traces(case_uid);
CREATE INDEX idx_at_created_at ON agent_traces(created_at);
-- Partition pruning candidate (monthly) once volume exceeds ~10M rows.
```

#### 4.1.5 `processed_files` — file-level dedup
```sql
CREATE TABLE processed_files (
  file_hash   CHAR(64) PRIMARY KEY,   -- SHA-256 hex
  file_id     TEXT NOT NULL,
  filename    TEXT NOT NULL,
  bytes       BIGINT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);
```

#### 4.1.6 `outbox` — transactional outbox
```sql
CREATE TABLE outbox (
  id           BIGSERIAL PRIMARY KEY,
  topic        TEXT NOT NULL,
  partition_key TEXT,
  payload      BYTEA NOT NULL,         -- Protobuf-serialized
  schema_id    INT  NOT NULL,
  created_at   TIMESTAMPTZ DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox(created_at) WHERE published_at IS NULL;
```

#### 4.1.7 `pan_vault` — encrypted PAN vault
```sql
CREATE TABLE pan_vault (
  token        TEXT PRIMARY KEY,        -- HMAC-SHA256 token
  ciphertext   BYTEA NOT NULL,          -- AES-256-GCM(raw_pan)
  iv           BYTEA NOT NULL,
  tag          BYTEA NOT NULL,
  bin          CHAR(6),
  last4        CHAR(4),
  created_at   TIMESTAMPTZ DEFAULT now()
);
```

### 4.2 Retention
- `agent_traces`: 14 days; nightly cron `DELETE FROM agent_traces WHERE created_at < now() - interval '14 days'`.
- `outbox`: rows deleted post-publish by drainer (or kept 24h then purged for debugging).
- `recon_cases`, `settlement_lines`, `pg_transactions`, `processed_files`: indefinite.

### 4.3 Indexing rationale
- `pg_transactions(rrn, utr)` composite serves the dominant rules-engine lookup `WHERE rrn=$1 AND utr=$2`.
- `recon_cases(resolution)` partial index can be added (`WHERE resolution = 'PROPOSED'`) once review queue size grows.
- `outbox(created_at) WHERE published_at IS NULL` keeps the drainer's scan small.

### 4.4 Transactional integrity
- Idempotent writes use `INSERT ... ON CONFLICT (file_id, line_no) DO NOTHING`.
- `case_uid = uuid5(NAMESPACE_OID, f"{file_id}:{line_no}")` derived without sequence.
- Outbox writes occur in the **same transaction** as the business write; the drainer publishes asynchronously after commit.

---

## 5. Protobuf Schemas

`proto/recon.proto`:
```protobuf
syntax = "proto3";
package recon.v1;

message FileArrived {
  string file_id     = 1;
  string filename    = 2;
  string sha256      = 3;
  int64  bytes       = 4;
  int64  detected_at = 5;  // unix epoch ms
}

message ReconRequest {
  string file_id  = 1;
  int32  line_no  = 2;
  string case_uid = 3;     // hex of UUID
}

message ReconResult {
  string case_uid       = 1;
  string match_type     = 2;
  string resolution     = 3;
  double confidence     = 4;
  string resolved_by    = 5;
  int64  resolved_at_ms = 6;
}
```

Schema evolution: only additive changes; never reuse field numbers; bump `schema_id` in `outbox.schema_id` on breaking change.

---

## 6. Module-Level Design

### 6.1 SFTP Watcher (`ingest/sftp_watcher.py`)

**Responsibility:** detect new files, deduplicate, emit `FileArrived`.

**Public API:**
```python
class SftpWatcher:
    def __init__(self, watch_dir: Path, kafka: KafkaProducer, repo: LedgerRepo): ...
    async def run(self) -> None: ...               # blocking event loop
    async def _on_close_write(self, path: Path) -> None: ...
```

**Algorithm (per file):**
1. Wait for `IN_CLOSE_WRITE` (file fully written).
2. Compute SHA-256 streaming (`hashlib.sha256`, 1 MiB chunks).
3. `INSERT INTO processed_files(file_hash, ...) ON CONFLICT DO NOTHING RETURNING file_id`.
4. If insert returned `None`, log `duplicate_file` metric and skip.
5. Else publish `FileArrived` to Kafka topic `files.new` (key = `file_id`, partitions = 3).

**Failure modes:**
- Watcher crash mid-hash → file remains on disk; on restart `watchdog` re-scans dir and dedup catches reprocessing.
- DB unavailable → retry with exponential backoff (1s → 30s); emit metric `watcher_db_failures_total`.

### 6.2 Parser (`ingest/parser.py`)

**Responsibility:** stream-parse UDIR, persist `settlement_lines`, fan out `ReconRequest` per line.

**Public API:**
```python
class UdirParser:
    HEADER = ["TXN_DATE","RRN","UTR","PAYER_VPA","PAYEE_VPA",
              "AMOUNT","FEE","GST","NET","STATUS","TXN_TYPE","REMARKS"]

    def __init__(self, repo: LedgerRepo, outbox: Outbox): ...
    async def parse(self, file_id: str, path: Path) -> int:
        """Returns number of lines persisted."""
```

**Parsing rules:**
- Pipe-separated, no quoting; reject lines whose column count ≠ `len(HEADER)`.
- Amounts arrive as decimal rupees with up to 2 places; convert to paise via `Decimal(amount) * 100` (never `float`).
- Empty `STATUS` → store as `NULL`; rules treat `NULL` as missing.
- Malformed line: write to `settlement_lines` with `status='MALFORMED'`, raw JSON of the offending fields; agent ignores `MALFORMED` (rules engine routes them to a dedicated `parse_error` case_type).

**Algorithm:**
```text
in transaction T:
    for line in stream(file):
        validated = validate(line)
        INSERT INTO settlement_lines(...) ON CONFLICT (file_id,line_no) DO NOTHING
        case_uid = uuid5(NAMESPACE_OID, f"{file_id}:{line_no}")
        INSERT INTO outbox(topic='recon.requests', payload=ReconRequest(...))
    commit T
```

A single transaction per file keeps outbox-events consistent with line writes.

### 6.3 Rules Engine (`rules/engine.py`)

**Responsibility:** deterministic matching of a settlement line against `pg_transactions`.

**Rule contract:**
```python
@dataclass(frozen=True)
class Match:
    pg_txn_id: int | None
    match_type: Literal['EXACT','TOLERANCE','DUPLICATE','MISSING_LEG','AMOUNT_MISMATCH']
    confidence: float
    notes: dict

Rule = Callable[[SettlementLine, list[PgTxn]], Match | None]
```

**Ordered pipeline:**
```python
RULES: list[Rule] = [
    exact_rrn,        # 1
    utr_amount,       # 2
    tolerance,        # 3
    duplicate,        # 4
    amount_mismatch,  # 5  → routes to agent
    missing_leg,      # 6  → routes to agent
]
```

Pseudocode for `engine.run(line)`:
```python
def run(line: SettlementLine) -> ReconCase:
    candidates = repo.lookup_pg_txns(rrn=line.rrn, utr=line.utr)
    if len(candidates) > 1 and not is_duplicate(candidates):
        return ReconCase.unresolved(line, reason="ambiguous")  # → agent
    for rule in RULES:
        m = rule(line, candidates)
        if m is not None:
            return ReconCase.from_match(line, m)
    return ReconCase.unresolved(line, reason="no_rule_fired")
```

**Determinism guarantees:**
- All rules are pure functions (no DB writes, no time, no random).
- Rule input is the line + a fully materialized list of candidates.
- `repo.lookup_pg_txns` orders by `id ASC` so candidate list is reproducible.
- First-match wins; rules are mutually exclusive by construction (asserted by unit tests).

**Detailed rule specs:**

| Rule | Predicate | Confidence |
|---|---|---|
| `exact_rrn` | exactly one PG txn with `rrn = line.rrn AND amount_paise = line.amount_paise AND status='SUCCESS'` | 1.000 |
| `utr_amount` | exactly one PG txn with `utr = line.utr AND amount_paise = line.amount_paise AND date(created_at) = date(line.txn_date)` | 0.990 |
| `tolerance` | exactly one PG txn with `rrn = line.rrn AND \|amt - line.amt\| <= 1` paise | 0.950 |
| `duplicate` | ≥2 settlement_lines exist with same RRN; pick first by `(file_id,line_no)` | 0.900 |
| `amount_mismatch` | RRN matches a single PG txn but `\|amt - line.amt\| > 1` paise → produces `MISSING_LEG/AMOUNT_MISMATCH` case_type, **routes to agent** | n/a |
| `missing_leg` | no PG txn for RRN/UTR (or vice versa) → routes to agent | n/a |

Tests assert: (i) idempotence (same input → same output), (ii) mutual exclusivity on a curated synthetic set.

### 6.4 LLM Agent (`agent/orchestrator.py`)

**Responsibility:** investigate residual cases, emit exactly one `propose_resolution` call.

**Public API:**
```python
class AgentOrchestrator:
    def __init__(self, anthropic: Anthropic, tools: ToolRegistry,
                 redactor: Redactor, repo: LedgerRepo,
                 model_triage="claude-haiku-4-5",
                 model_invest="claude-sonnet-4-6"): ...

    async def investigate(self, case: ReconCase) -> AgentDecision: ...
```

**State machine:**
```
NEW → TRIAGE_HAIKU
   ├── conf >= 0.9 ──▶ DECISION
   └── conf <  0.9 ──▶ INVESTIGATE_SONNET ──▶ DECISION
DECISION → PROPOSED   (writes recon_cases.resolution='PROPOSED')
DECISION → ESCALATE   (on tool failure / timeout / max steps)
```

**Hard limits (enforced server-side, not via prompt):**
- Max 6 tool calls per case (counter on orchestrator).
- 30 s wall-clock per case (`asyncio.wait_for`).
- Temperature = 0; `top_p = 1`.
- Exactly one terminal `propose_resolution` tool call (multiple → `ESCALATE`).

**Prompt caching layout:**
```
[cache-anchor 1] system_prompt          (~3.5k tokens, cache_control=ephemeral)
[cache-anchor 2] tool_schemas + exemplars (~2k tokens, cache_control=ephemeral)
[per-case]       user message with redacted ReconCase JSON
```

**Conditional update (write guard):**
```sql
UPDATE recon_cases
SET resolution='PROPOSED', resolved_by='agent',
    confidence=$conf, notes=$notes, pg_transaction=$pg_txn_id
WHERE case_uid=$uid AND resolution IS NULL;
```
Returning row count of 0 → another worker raced; agent logs `stale_write` metric and exits without retry.

### 6.5 Agent Tools (`agent/tools.py`)

Each tool is a Python function decorated with `@tool` that:
1. Accepts only redacted/tokenized arguments.
2. Calls `LedgerRepo` read-only methods.
3. Re-redacts the response before returning.
4. Records latency, input hash, output hash on the orchestrator's step buffer.

**Tool catalog:**

| Tool | Inputs (redacted) | Output | Side effect |
|---|---|---|---|
| `search_pg_transactions` | rrn, utr, payer_vpa_token, amount_range, date_range | list of PgTxnRedacted | none |
| `get_settlement_history` | rrn | list of {file_id, line_no, amount_paise, status} | none |
| `get_chargeback_status` | rrn | {chargeback: bool, amount_paise, processed_at} | none |
| `compute_fee_breakdown` | amount_paise, txn_type | {mdr_paise, gst_paise, net_paise} | none |
| `propose_resolution` | case_uid, resolution_type, confidence, rationale, pg_txn_id | ack | **writes** `recon_cases` |

Only `propose_resolution` is permitted to mutate state. The redactor enforces by tool name.

### 6.6 PII Redaction (`pii/redactor.py`, `pii/tokenizer.py`)

**Boundary:** every tool call passes through `RedactionGateway.invoke(tool_name, args)` which:
1. Detokenizes inputs (token → raw) for DB query.
2. Executes the underlying function.
3. Tokenizes outputs (raw → token).
4. On any failure (regex miss, vault error) → raises `RedactionFailure`; orchestrator marks case `ESCALATE`. **Fail closed.**

**PAN tokenization:**
```python
def pan_token(pan: str) -> str:
    bin6  = pan[:6]
    last4 = pan[-4:]
    middle = pan[6:-4]
    h = hmac.new(HMAC_KEY, middle.encode(), 'sha256').hexdigest()[:16]
    return f"PAN_{bin6}_{h}_{last4}"
```
Reversal stored in `pan_vault` keyed by token.

**VPA / phone / email / name:** regex-based masking with a stable per-session salt so the LLM can refer to the same token across messages.

| Type | Regex (illustrative) | Token format |
|---|---|---|
| VPA | `^[\w.\-]+@[\w.\-]+$` | `VPA_<sha1[:10]>` |
| Phone | `^\+?\d{10,12}$` | `PHN_<sha1[:10]>` |
| Email | `^[^@]+@[^@]+\.[^@]+$` | `EML_<sha1[:10]>` |
| Name | NER-based (spaCy `en_core_web_sm`) | `NAM_<sha1[:10]>` |

**Audit:** nightly job `pii_audit.py` greps `agent_traces.steps` for raw patterns (regex same as redactor). Hits page on-call.

### 6.7 Ledger Repo (`ledger/repo.py`)

Thin async data-access layer using `asyncpg`. Connection pool size = 10 per worker.

**Selected methods:**
```python
class LedgerRepo:
    async def insert_settlement_lines(self, rows: list[SettlementLine]) -> int: ...
    async def lookup_pg_txns(self, rrn: str | None, utr: str | None) -> list[PgTxn]: ...
    async def insert_recon_case(self, case: ReconCase) -> bool: ...
    async def update_recon_case_proposed(self, uid: UUID, fields: dict) -> bool: ...
    async def insert_outbox(self, conn, topic: str, payload: bytes, schema_id: int): ...
    async def fetch_proposed_cases(self, limit: int, offset: int) -> list[ReconCase]: ...
```

All write methods accept an optional `conn` for transaction sharing.

### 6.8 Outbox Publisher (`ledger/outbox.py`)

Drains `outbox` rows where `published_at IS NULL` in batches of 200, publishes to Kafka, then `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`. Loop interval 100 ms or whenever notified via `LISTEN/NOTIFY 'outbox_new'`.

Failure handling: producer error → leave row unpublished; next pass retries. Kafka `acks=all`, idempotent producer enabled (`enable.idempotence=true`).

### 6.9 FastAPI Service (`api/main.py`)

Endpoints (full list in §7).

Cross-cutting middleware:
- `X-Request-ID` propagation (UUIDv4 if absent).
- OTel `FastAPIInstrumentor`.
- Auth: bearer token (env `API_BEARER_TOKEN`); only `/healthz` is public.
- Rate limit: 60 rpm per token (in-memory; sufficient for solo deployment).

### 6.10 CLI (`cli.py`)

Sub-commands (built with `typer`):
```
recon backfill --since YYYY-MM-DD [--rules-only]
recon dlq replay --topic recon.investigate [--max N]
recon eval run [--real]
recon seed [--rows 100000]
recon redact-audit
```

### 6.11 Observability (`obs/otel.py`, `obs/metrics.py`)

- OTel SDK initialized in each process at startup; OTLP exporter → Tempo via Collector.
- Prometheus client exposes `/metrics` on port 9100 (parser, rules, agent share separate ports).
- Span naming: `recon.<module>.<op>` (e.g., `recon.rules.engine.run`).
- Mandatory span attributes: `case.uid`, `match.type`, `agent.model`, `agent.cost_usd`.
- Forbidden attributes: anything matching the redactor's regex set; checked in CI by `tests/test_no_pii_in_spans.py`.

### 6.12 Eval Harness (`eval/harness.py`)

- Loads YAML cases under `tests/eval/cases/`.
- For each case: stubs `LedgerRepo` reads, runs orchestrator with mocked `Anthropic` client, asserts `resolution_type`, `must_call_tools ⊆ tools_used`, `cost_usd ≤ max_cost_usd`.
- Reports two accuracy numbers: rules-only (skipping the agent) and rules+agent (full pipeline).
- Regression gate: fails CI if `(accuracy_now < accuracy_prev - 0.02)` or `(cost_now > cost_prev * 1.20)`.

---

## 7. REST API Specification

Base path: `/api/v1`. Authentication: `Authorization: Bearer <token>`.

### 7.1 `GET /cases`
Query params: `status` ∈ {PROPOSED, APPROVED, REJECTED, ESCALATE}, `match_type`, `cursor`, `limit` (≤200).

200 response:
```json
{
  "cases": [{
    "case_uid": "f3a9...",
    "match_type": "AMOUNT_MISMATCH",
    "confidence": 0.96,
    "resolution": "PROPOSED",
    "settlement_line_id": 8821,
    "pg_transaction_id": 7711,
    "notes": {"rationale": "Partial reversal of ₹500 ..."},
    "created_at": "2026-04-30T18:42:11Z"
  }],
  "next_cursor": "eyJpZCI6ODgyMX0="
}
```

### 7.2 `GET /cases/{case_uid}`
404 if not found; 200 returns full case + last `agent_traces` row (without raw PII).

### 7.3 `POST /cases/{case_uid}/approve`
Body: `{ "reviewer_email": "ops@example.com", "comment": "ok" }`.
- 200 on success; updates `resolution='APPROVED'`, `resolved_by='human:ops@example.com'`, `resolved_at=now()`.
- 409 if `resolution != PROPOSED`.
- 422 if body validation fails.

### 7.4 `POST /cases/{case_uid}/reject`
Body identical; sets `resolution='REJECTED'`. Same error semantics.

### 7.5 `GET /healthz`
200 if DB reachable AND Kafka reachable AND Anthropic API key present. Returns `{"db":"ok","kafka":"ok","llm":"configured"}`.

### 7.6 `GET /metrics`
Prometheus exposition format; not auth-gated (scoped to private network).

### 7.7 Error envelope
```json
{ "error": { "code": "STALE_RESOLUTION", "message": "...", "request_id": "..." } }
```
HTTP codes: 400 validation, 401 auth, 403 forbidden, 404 not found, 409 conflict, 429 rate limit, 500 unexpected, 503 dependency down.

---

## 8. Sequence Diagrams

### 8.1 Happy path — rules-resolved line
```
SFTP   Watcher        Kafka       Parser        Rules           DB
 │ file │              │           │              │               │
 │─────▶│ sha256 + dedup           │              │               │
 │      │─────FileArrived─────────▶│              │               │
 │      │              │           │ stream rows ─┼──insert SL───▶│
 │      │              │           │ outbox(ReconRequest)         │
 │      │              │◀── drain ─│              │               │
 │      │              │──ReconReq─▶              │ lookup_pg_txn │◀
 │      │              │           │              │ engine.run    │
 │      │              │           │              │ insert RC     │
 │      │              │           │              │ outbox(ReconResult)
```

### 8.2 Agent investigation
```
Rules ─unmatched─▶ Kafka(recon.investigate) ─▶ Agent
Agent.haiku_triage()
  ├── conf ≥ 0.9 ──▶ propose_resolution
  └── conf <  0.9 ──▶ Sonnet
                       loop ≤ 6:
                         tool = pick_tool()
                         redactor.invoke(tool, args)
                       propose_resolution
Agent.UPDATE recon_cases WHERE resolution IS NULL
Agent.INSERT agent_traces (post-redaction)
```

### 8.3 Human approval
```
Reviewer ──GET /cases?status=PROPOSED──▶ FastAPI ──SELECT──▶ DB
Reviewer ──POST /cases/{uid}/approve──▶ FastAPI
   tx:
     UPDATE recon_cases SET resolution='APPROVED' WHERE uid=$ AND resolution='PROPOSED'
     INSERT outbox(topic='cases.approved', ...)
   commit
```

---

## 9. Algorithms

### 9.1 case_uid derivation
```python
NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # OID
def case_uid(file_id: str, line_no: int) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, f"{file_id}:{line_no}")
```
Properties: deterministic, no central sequence, no collisions across files, easy to log.

### 9.2 Idempotent line write + outbox (single transaction)
```python
async with conn.transaction():
    inserted = await conn.fetchval("""
        INSERT INTO settlement_lines(...) VALUES (...)
        ON CONFLICT (file_id, line_no) DO NOTHING
        RETURNING id
    """, *args)
    if inserted is None:
        return  # duplicate; do not emit outbox event
    await conn.execute("INSERT INTO outbox(topic, payload, schema_id) VALUES ($1,$2,$3)",
                       'recon.requests', payload, 1)
```

### 9.3 Retry policy
- Initial: 1 s; multiplier 5×; max 25 s; cap 3 attempts.
- After cap: produce to `recon.dlq` with original payload + failure metadata header.
- Replay command consumes from DLQ, re-publishes to original topic.

### 9.4 Auto-approve threshold
```python
def can_auto_approve(case: ReconCase) -> bool:
    return (
        case.match_type == 'EXACT' or
        (case.resolution == 'PROPOSED' and case.notes.get('proposed_type') == 'MATCH'
         and case.confidence >= 0.99
         and case.notes.get('source') == 'agent'
         and not feature_flag('disable_auto_approve'))
    )
```
`WRITE_OFF` is **never** auto-approved.

---

## 10. Configuration

All config via env vars validated by `pydantic.BaseSettings`:

| Env | Default | Notes |
|---|---|---|
| `DATABASE_URL` | required | `postgres://...` |
| `KAFKA_BROKERS` | `kafka:9092` | comma-separated |
| `ANTHROPIC_API_KEY` | required | secret |
| `MODEL_TRIAGE` | `claude-haiku-4-5` |  |
| `MODEL_INVEST` | `claude-sonnet-4-6` |  |
| `AGENT_MAX_STEPS` | 6 |  |
| `AGENT_TIMEOUT_S` | 30 |  |
| `RULES_TOLERANCE_PAISE` | 1 |  |
| `RETRY_INITIAL_S` | 1 |  |
| `RETRY_MAX_ATTEMPTS` | 3 |  |
| `PAN_HMAC_KEY` | required | 32 bytes b64 |
| `PAN_AES_KEY` | required | 32 bytes b64 |
| `API_BEARER_TOKEN` | required |  |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` |  |
| `KAFKA_CONSUMER_GROUP_PREFIX` | `recon` |  |

Feature flags via env: `DISABLE_AUTO_APPROVE`, `MOCK_LLM`.

---

## 11. Error Handling & Failure Modes

| Failure | Detection | Action |
|---|---|---|
| Duplicate file | `processed_files` PK conflict | Skip, log, increment counter |
| Malformed line | column count mismatch | Persist with `status='MALFORMED'`, route to `parse_error` rule |
| DB unavailable | `asyncpg.PostgresError` | Exponential backoff; circuit break after 30 s |
| Kafka unavailable | producer raise | Outbox holds rows until broker returns |
| LLM 5xx | `anthropic.APIStatusError` | Retry up to 2 times then `ESCALATE` |
| LLM rate limit | 429 | Token bucket pause; do not retry |
| Tool exception | any `Exception` | Mark case `ESCALATE`; log redacted args |
| Redaction failure | `RedactionFailure` | Abort LLM call; mark `ESCALATE`; alert |
| Stale write race | UPDATE returns 0 rows | Drop write; metric `agent_stale_write_total++` |
| Schema drift | Protobuf decode error | DLQ |

DLQ topic: `recon.dlq`. Headers: `error_class`, `error_message`, `attempts`, `original_topic`.

---

## 12. Security Design

- **Network:** API on Caddy with TLS; Kafka/Postgres bound to Docker network only.
- **Secrets:** loaded from `.env` (dev) or OCI Vault (prod-shape); never logged.
- **PII:** see §6.6. Audit job nightly; alert on any unmasked match.
- **AuthN/Z:** static bearer token for solo demo; structured for swap-in OAuth.
- **Threat model highlights:**
  - Prompt-injection from settlement file → mitigated by redaction (LLM never sees raw text the user controls beyond tokens) and by tool schema validation.
  - Tool over-reach → only `propose_resolution` mutates state; redactor enforces tool allow-list.
  - Replay of approval requests → idempotent on `case_uid` + state guard (`WHERE resolution='PROPOSED'`).

---

## 13. Observability Plan

### 13.1 Metrics (Prometheus)
```
manual_review_queue_size                          # gauge
recon_match_rate                                  # gauge, SLO target ≥ 0.90
recon_cases_total{match_type, resolved_by}        # counter
recon_rules_engine_duration_seconds               # histogram
recon_agent_duration_seconds                      # histogram
llm_cost_usd_total{model}                         # counter
llm_tokens_total{model, kind=input|output|cached} # counter
kafka_consumer_lag{topic, group}                  # gauge
recon_dlq_size                                    # gauge
agent_stale_write_total                           # counter
redaction_failure_total                           # counter
```

### 13.2 Traces
Sampling: 100% for `recon.agent.*`; 10% otherwise. Each span carries `case.uid`. Spans:
`recon.watcher.detect → recon.parser.line → recon.rules.run → recon.agent.triage → recon.agent.invest → recon.tool.<name> → recon.api.approve`.

### 13.3 Logs
JSON; required fields: `ts, level, request_id, case_uid, module, message`. PII forbidden; CI test asserts.

### 13.4 Alerts
| Alert | Rule | Severity |
|---|---|---|
| MatchRateLow | `recon_match_rate < 0.90 for 30m` | page |
| LLMCostSpike | `rate(llm_cost_usd_total[5m]) > 0.10/min` | page |
| KafkaLagHigh | `kafka_consumer_lag > 1000 for 5m` | page |
| ManualQueueGrowing | `delta(manual_review_queue_size[1h]) > 0` | warn |
| DLQNonEmpty | `recon_dlq_size > 0 for 10m` | warn |
| RedactionFailure | `increase(redaction_failure_total[5m]) > 0` | page |

---

## 14. Test Strategy

| Layer | Tooling | Coverage target |
|---|---|---|
| Unit (rules, redactor, tokenizer) | `pytest` | ≥ 95% |
| Integration (parser → DB → outbox) | `pytest` + Docker Postgres | end-to-end happy path + 3 failure modes |
| Eval (agent prompts) | `pytest -m eval` | ≥ 30 golden cases, ≥ 90% accuracy, ≤ $0.01/case |
| Contract (Protobuf, REST) | schemathesis + buf | breaking-change diff in CI |
| Load (rules path) | locust | 1000 lines/s sustained, p99 < 50 ms |
| Security (PII audit) | nightly cron + grep | zero unmasked findings |

Mocks: `Anthropic` client wrapped behind `LLMClient` interface; eval harness injects `MockLLMClient` with deterministic tool-call scripts.

---

## 15. Deployment Specifics

- Target: OCI A1.Flex 4 OCPU / 24 GB ARM64.
- Compose services: `caddy`, `sftp`, `postgres`, `kafka`, `parser`, `rules`, `agent`, `api`, `outbox-drainer`, `prometheus`, `grafana`, `tempo`, `otel-collector`.
- Resource budget: see HLD §Appendix; total ≈ 6.5 GB.
- Image policy: multi-arch builds (`buildx`), `linux/arm64` verified by `docker manifest inspect` in CI.
- CI/CD: GitHub Actions — `lint → unit → integration → eval(mock) → build → deploy on main`.

---

## 16. Traceability to HLD & Invariants

| HLD Invariant | LLD Enforcement |
|---|---|
| 1. Each line processed exactly once | §4.1.2 unique constraint, §9.2 conditional insert, §9.1 deterministic uid |
| 2. No raw PII to LLM | §6.6 redaction gateway, fail-closed, §13 audit alert |
| 3. Rules deterministic | §6.3 pure functions, §14 unit tests asserting determinism |
| 4. Agent reversible & auditable | §6.4 conditional update, §4.1.4 traces, §7.3 approval API |
| 5. Tolerates duplicate ingestion | §6.1 SHA-256 + `processed_files`, §9.2 idempotent writes |
| 6. DB-committed before Kafka emits | §6.8 outbox drainer, §9.2 single transaction |

---

## 17. Open Questions

1. Should `agent_traces` retention be configurable per env (currently 14 days)?
2. Do we need per-tenant partitioning of `pg_transactions` or is single-tenant sufficient for the showcase?
3. Auto-approve threshold — start at 0.99 or 0.995? Will be informed by first 2 weeks of eval data.
4. PII NER model size — `en_core_web_sm` may miss Indian names; evaluate `en_core_web_trf` cost vs accuracy.

---

## 18. Change Log

| Version | Date | Author | Notes |
|---|---|---|---|
| 1.0 | 2026-05-01 | Nikhil Pujar | Initial LLD derived from `doc.md` v1 |

# Low-Level Design — Autonomous Payment Reconciliation Agent

| Field | Value |
|---|---|
| Document type | Low-Level Design (LLD) |
| Project | Autonomous Payment Reconciliation Agent |
| Version | 2.0 |
| Date | 2026-05-24 |
| Author | Nikhil Pujar |
| Status | Reflects actual implementation |

---

## 1. Purpose & Scope

This LLD describes the implemented system — module boundaries, class-level design, database schema, API contracts, algorithms, and deployment. It is derived directly from the source code in `recon-boot/`.

Out of scope: operational runbooks, marketing content, NPCI's actual file specification (simulated).

---

## 2. System Context

```
NPCI SFTP ──▶ SftpWatcher ──▶ UdirParser ──▶ outbox(recon.requests)
                                                        │
                                              OutboxDrainer (500ms poll)
                                                        │
                                              ReconRequestListener ──▶ RulesEngine
                                                        │                    │
                                                        │          AUTO_RESOLVED (≥90%)
                                                        │                    │
                                              outbox(recon.investigate)      │
                                                        │                    │
                                         InvestigateOutboxDrainer            │
                                                        │                    │
                                         ReconInvestigateListener            │
                                                        │                    │
                                         AgentOrchestrator (Groq)            │
                                              │              │                │
                                          PROPOSED       ESCALATE            │
                                              └────────────┴──────────▶ REST API ──▶ Human Reviewer
```

**Transport:** No Kafka. Workers communicate through the `outbox` Postgres table using `FOR UPDATE SKIP LOCKED`. Spring `ApplicationEventPublisher` carries events within a single JVM.

**Scale:** ~100k settlement lines/day. Rules auto-resolve ≥90%; the LLM agent handles the residual ~10%.

---

## 3. Module Catalog

| # | Module | Key classes | Responsibility |
|---|---|---|---|
| 1 | `recon-common` | `AppConfig`, `CaseUid`, `ProtoCodec`, models, events | Shared types, config, Protobuf codec |
| 2 | `recon-ingest` | `SftpWatcher`, `UdirParser` | SFTP polling, file parsing, outbox write |
| 3 | `recon-ledger` | `*Repository`, `OutboxDrainer` | All DB I/O; outbox drain for `recon.requests` |
| 4 | `recon-rules` | `RulesEngine`, 6 `Rule` impls, `ReconRequestListener` | Deterministic case resolution |
| 5 | `recon-agent` | `AgentOrchestrator`, `LangChainAgentConfig`, 5 tools, `InvestigateOutboxDrainer`, `ReconInvestigateListener` | LLM investigation via Groq + LangChain4j |
| 6 | `recon-pii` | `PiiRedactor`, `HmacTokenizer`, `PanVault`, `RedactionGateway`, `PiiAuditJob` | PII masking, PAN tokenization, nightly audit |
| 7 | `recon-api` | `ReconApplication`, `CasesController`, `HealthController`, `BearerAuthFilter`, `RequestIdFilter` | REST API, auth, request tracing |
| 8 | `recon-obs` | `OtelConfig`, `ReconMetrics` | OpenTelemetry tracing, Prometheus metrics |
| 9 | `recon-eval` | `EvalHarnessTest`, `ScriptedAnthropicClient` | Golden-case regression harness |

All modules are stateless processes; Postgres is the only stateful component.

---

## 4. Database Design

### 4.1 Tables

#### `pg_transactions` — internal PG ledger
```sql
CREATE TABLE pg_transactions (
    id           BIGSERIAL PRIMARY KEY,
    txn_id       TEXT UNIQUE NOT NULL,
    rrn          TEXT,
    utr          TEXT,
    payer_vpa    TEXT,
    payee_vpa    TEXT,
    amount_paise BIGINT NOT NULL CHECK (amount_paise >= 0),
    status       TEXT   NOT NULL CHECK (status IN
                 ('INITIATED','SUCCESS','FAILED','TIMEOUT','PARTIAL_REVERSED')),
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_pg_txn_rrn        ON pg_transactions(rrn);
CREATE INDEX idx_pg_txn_utr        ON pg_transactions(utr);
CREATE INDEX idx_pg_txn_rrn_utr    ON pg_transactions(rrn, utr);
CREATE INDEX idx_pg_txn_created_at ON pg_transactions(created_at);
```

#### `settlement_lines` — parsed UDIR rows
```sql
CREATE TABLE settlement_lines (
    id           BIGSERIAL PRIMARY KEY,
    file_id      TEXT   NOT NULL,
    line_no      INT    NOT NULL,
    rrn          TEXT,
    utr          TEXT,
    amount_paise BIGINT,
    fee_paise    BIGINT,
    net_paise    BIGINT,
    status       TEXT,
    raw          JSONB,
    ingested_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (file_id, line_no)
);
CREATE INDEX idx_sl_rrn_utr ON settlement_lines(rrn, utr);
CREATE INDEX idx_sl_file_id ON settlement_lines(file_id);
```

#### `recon_cases` — resolution state machine
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
                    ('PENDING','PROPOSED','APPROVED','REJECTED','AUTO_RESOLVED','ESCALATE')),
    resolved_by     TEXT,
    notes           JSONB,
    created_at      TIMESTAMPTZ DEFAULT now(),
    resolved_at     TIMESTAMPTZ
);
CREATE INDEX idx_rc_resolution ON recon_cases(resolution);
CREATE INDEX idx_rc_match_type ON recon_cases(match_type);
CREATE INDEX idx_rc_created_at ON recon_cases(created_at);
```

`PENDING` (added in V4 migration) is the initial state for cases routed to the agent. The write guard in `proposeResolution()` checks `WHERE resolution = 'PENDING'` — ensuring a case can only be resolved once.

#### `agent_traces` — LLM audit log
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
```

#### `processed_files` — file-level dedup
```sql
CREATE TABLE processed_files (
    file_hash   CHAR(64) PRIMARY KEY,   -- SHA-256 hex
    file_id     TEXT NOT NULL,
    filename    TEXT NOT NULL,
    bytes       BIGINT,
    ingested_at TIMESTAMPTZ DEFAULT now()
);
```

#### `outbox` — transactional outbox (replaces Kafka)
```sql
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    topic         TEXT  NOT NULL,
    partition_key TEXT,
    payload       BYTEA NOT NULL,        -- Protobuf-encoded
    schema_id     INT   NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox(created_at) WHERE published_at IS NULL;
```

Topics in use: `recon.requests` (schema_id=1, ingest→rules) and `recon.investigate` (schema_id=3, rules→agent).

#### `pan_vault` — encrypted PAN storage
```sql
CREATE TABLE pan_vault (
    token      TEXT PRIMARY KEY,   -- HMAC-SHA256 token
    ciphertext BYTEA NOT NULL,     -- AES-256-GCM(pan)
    iv         BYTEA NOT NULL,
    tag        BYTEA NOT NULL,
    bin        CHAR(6),
    last4      CHAR(4),
    created_at TIMESTAMPTZ DEFAULT now()
);
```

### 4.2 Indexing rationale

- `pg_transactions(rrn, utr)` composite: serves the dominant rules lookup `WHERE rrn=$1 AND utr=$2` with one index scan.
- `outbox(created_at) WHERE published_at IS NULL`: keeps drainer poll scans small — only undelivered rows are in the index.
- `recon_cases(resolution)`: fast queue-size queries for the review dashboard.

### 4.3 Retention

- `agent_traces`: 14-day nightly delete.
- `outbox`: rows soft-deleted by drainer (published_at set); purge after 24h.
- All other tables: indefinite.

---

## 5. Protobuf Schemas (`recon-common/src/main/proto/recon.proto`)

```protobuf
syntax = "proto3";
package recon.v1;

message FileArrived {       // not yet used in outbox; wired for future use
  string file_id     = 1;
  string filename    = 2;
  string sha256      = 3;
  int64  bytes       = 4;
  int64  detected_at = 5;
}

message ReconRequest {      // outbox topic: recon.requests, schema_id=1
  string file_id  = 1;
  int32  line_no  = 2;
  string case_uid = 3;
}

message ReconInvestigate {  // outbox topic: recon.investigate, schema_id=3
  string case_uid = 1;
}

message ReconResult {       // reserved for future downstream consumers
  string case_uid       = 1;
  string match_type     = 2;
  string resolution     = 3;
  double confidence     = 4;
  string resolved_by    = 5;
  int64  resolved_at_ms = 6;
}
```

Schema evolution rule: only additive changes; never reuse field numbers.

---

## 6. Module-Level Design

### 6.1 SftpWatcher (`recon-ingest`, `@Profile("watcher")`)

Polls the SFTP server every 5 seconds via JSch. Only active when Spring profile `watcher` is set.

**Algorithm per file:**
1. JSch connects to `${SFTP_HOST}:${SFTP_PORT}` with password auth.
2. Lists `*.txt` files in `${recon.sftp.remote-dir}`.
3. Downloads each file into a `ByteArrayOutputStream`.
4. Computes SHA-256 → `FILE_<first12hex>` as `file_id`.
5. `processedFiles.markSeen(sha256, fileId, filename, bytes)` — `INSERT ... ON CONFLICT DO NOTHING RETURNING file_id`. If nothing returned → already processed → skip.
6. `UdirParser.parse(fileId, inputStream)` → `ParseResult(lines, errors)`.
7. For each `SettlementLine`: `settlementLines.insertIgnoreDuplicate(line, protoPayload, caseUid)` which atomically inserts the line and a `recon.requests` outbox row.

**Failure:** JSch exception → log error, skip poll cycle, retry next 5s tick.

### 6.2 UdirParser (`recon-ingest`)

Pipe-delimited stream parser. Stateless `@Component` — no DB access.

```
Format:
  HDR|<file_id>|...
  <rrn>|<utr>|<amount_paise>|<fee_paise>|<net_paise>|<status>
  TRL|<count>
```

- Amounts arrive as paise integers (no decimal conversion needed).
- Lenient by column index: missing columns default to `0` or `"UNKNOWN"`.
- Trailer count mismatch: logged as warning, not an error — parsing continues.
- Returns `ParseResult(List<SettlementLine>, List<String> errors, String headerFileId)`.

### 6.3 Outbox Transport (replaces Kafka)

Two drainers poll the `outbox` table every 500ms using `FOR UPDATE SKIP LOCKED` to safely support concurrent workers:

**`OutboxDrainer`** (in `recon-ledger`, runs in every JVM):
- Polls `topic='recon.requests'`, batch 100.
- Decodes `ReconRequest` Protobuf → publishes `ReconRequestEvent` via `ApplicationEventPublisher`.
- Marks row `published_at = now()` after successful dispatch.

**`InvestigateOutboxDrainer`** (in `recon-agent`, runs in every JVM):
- Polls `topic='recon.investigate'`, **batch 1** — throttles Groq's 12k TPM free tier.
- Decodes `ReconInvestigate` Protobuf → publishes `ReconInvestigateEvent`.

Both drainers are `@Scheduled(fixedDelay=500)` and `@Transactional`. `FOR UPDATE SKIP LOCKED` prevents two JVMs from processing the same row; only one wins the lock per row.

### 6.4 RulesEngine (`recon-rules`)

**Event entry point:** `ReconRequestListener` is `@Async @TransactionalEventListener(AFTER_COMMIT)`. It receives `ReconRequestEvent`, fetches the `SettlementLine`, calls `evaluate()`, and writes the result.

**Rule contract:**
```java
interface Rule {
    RuleMatch evaluate(SettlementLine line, Set<String> seenRrns);
}
// RuleMatch: matched(), matchType(), confidence(), pgTransactionId()
```

**Ordered pipeline (first-match wins):**

| # | Rule | Predicate | Confidence | Resolution |
|---|---|---|---|---|
| 1 | `ExactRrnRule` | Single PG txn with matching RRN + exact amount + `status=SUCCESS` | 1.000 | AUTO_RESOLVED |
| 2 | `UtrAmountRule` | Single PG txn with matching UTR + exact amount + same date | 0.990 | AUTO_RESOLVED |
| 3 | `ToleranceRule` | Single PG txn with matching RRN, `|amount_diff| ≤ tolerancePaise (default 1)` | 0.950 | AUTO_RESOLVED |
| 4 | `DuplicateRule` | RRN already seen in `seenRrns` set (within the same file) | 0.900 | AUTO_RESOLVED |
| 5 | `AmountMismatchRule` | RRN matches a single txn but amount differs by >tolerancePaise | — | PENDING → Agent |
| 6 | `MissingLegRule` | No PG txn found by RRN or UTR | — | PENDING → Agent |
| — | fallthrough | No rule fired | — | PENDING (UNKNOWN) → Agent |

**Determinism guarantees:** All rules are pure functions — no DB writes, no time calls, no randomness. Same `SettlementLine` + same candidates always yields the same `RuleMatch`. Unit-tested for idempotence and mutual exclusivity.

**`AUTO_RESOLVED` path:** `caseRepo.upsertAutoResolved()` — `INSERT ON CONFLICT DO NOTHING` + publishes `CaseApprovedEvent`.

**`PENDING` path:** `caseRepo.upsertPending()` — inserts case with `resolution='PENDING'` and **atomically** inserts a `recon.investigate` outbox row in the same transaction (only if `inserted > 0` — idempotent on replay).

### 6.5 AgentOrchestrator (`recon-agent`)

**Entry point:** `ReconInvestigateListener` receives `ReconInvestigateEvent` → calls `orchestrator.investigate(caseUid)`.

**LLM stack:** Groq via LangChain4j `AiServices`. `LangChainAgentConfig` wires:
```java
OpenAiChatModel.builder()
    .baseUrl("https://api.groq.com/openai/v1")
    .apiKey(GROQ_API_KEY)
    .modelName(config.agent().modelInvest())  // llama-3.3-70b-versatile
    .maxTokens(1024)
    .maxRetries(3)
    .build();

AiServices.builder(ReconInvestigateAgent.class)
    .chatLanguageModel(chatLanguageModel)
    .tools(searchTool, chargebackTool, settlementTool, feeTool, proposeTool)
    .build();
```

`@ConditionalOnProperty(mock-llm=true)` wires a no-op mock model that returns an escalation string — zero Groq calls during tests.

**Timeout enforcement:**
```java
Future<?> future = executor.submit(() ->
    investigateAgent.investigate("Investigate reconciliation case: " + caseUid));
future.get(config.agent().timeoutSeconds(), TimeUnit.SECONDS);  // default 30s
// TimeoutException → escalate()
```

**Escalation** writes `resolution='ESCALATE'` via `caseRepo.proposeResolution()` with a JSON notes field containing the reason.

After successful investigation: inserts an `agent_trace` row with `prompt_hash`, model name, elapsed ms, and cost (Groq free tier → `$0.00`).

### 6.6 Agent Tools (`recon-agent`)

All tools are `@Component` beans annotated with LangChain4j's `@Tool`. LangChain4j's `AiServices` discovers them automatically and generates the tool schemas for the LLM.

| Tool | `@Tool` description | DB access | Mutates? |
|---|---|---|---|
| `SearchPgTransactionsTool` | Search internal PG ledger by RRN/UTR | `PgTransactionRepository` | No |
| `GetSettlementHistoryTool` | Get all settlement lines for an RRN | `SettlementLineRepository` | No |
| `GetChargebackStatusTool` | Check if an RRN has been charged back | `ReconCaseRepository` | No |
| `ComputeFeeBreakdownTool` | Compute MDR + GST + net given amount + txn type | Pure calculation | No |
| `ProposeResolutionTool` | **Terminal tool** — write the agent's decision | `ReconCaseRepository.proposeResolution()` | **Yes** |

**`ProposeResolutionTool` write guard:**
```sql
UPDATE recon_cases
SET resolution = :resolution, confidence = :confidence,
    resolved_by = 'agent', notes = CAST(:notes AS jsonb), resolved_at = now()
WHERE case_uid = :uid AND resolution = 'PENDING'
```
Returns 0 rows → stale write (another worker already resolved it) → returns `{"status": "STALE"}`. Not retried.

**PII in rationale:** Before persisting the LLM-written `rationale` to `notes`, it is passed through `PiiRedactor.redact()`. The LLM may echo VPA/phone/PAN in its reasoning; redaction ensures nothing raw reaches the DB.

### 6.7 PII Subsystem (`recon-pii`)

**`PiiRedactor`** — regex masking over free-form text:

| Pattern | Regex | Token format |
|---|---|---|
| Email | `[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}` | `EML_<hmac[:7]>` |
| VPA | `[a-z0-9._%+\-]+@[a-z0-9.\-]+` | `VPA_<hmac[:9]>` |
| Phone | `\b[6-9]\d{9}\b` (Indian mobile) | `PHN_<hmac[:8]>` |
| PAN | `[A-Z]{5}[0-9]{4}[A-Z]` | `PAN_<hmac[:8]>` |

Email is matched before VPA because the email pattern is more specific (has a TLD segment).

**`HmacTokenizer`** — HMAC-SHA256 with `PAN_HMAC_KEY` (32-byte base64 env var). Used as the hash function for all token suffixes. Deterministic: same input + same key = same token.

**`PanVault.tokenize(pan)`** — produces `PAN_<bin6>_<hmac[:8]>_<last4>`. In-memory `ConcurrentHashMap` cache ensures the same PAN always maps to the same token within a JVM lifetime.

**`PanVault.encrypt(pan)` / `decrypt(envelope)`** — AES-256-GCM via BouncyCastle:
- Random 12-byte IV per encryption — same PAN produces different ciphertext each time (no frequency leakage).
- 128-bit GCM authentication tag — detects tampering; decryption throws rather than returning corrupt data.
- Envelope format: `iv (12 bytes) || ciphertext+tag` → Base64.

**`PiiAuditJob`** — `@Scheduled(cron="0 0 2 * * *")`:
- Scans `pg_transactions(payer_vpa, payee_vpa)`, `settlement_lines(rrn, utr)`, `agent_traces(steps)`.
- For each value: `PiiRedactor.containsPii(value)`.
- Any hit: increments `recon_pii_audit_findings_total` counter + logs at `ERROR` level.
- Prometheus alert `RedactionFailure` fires on any counter increment → pages on-call.

### 6.8 Repositories (`recon-ledger`)

All repositories use Spring's `JdbcClient` (Spring 6.1+). No ORM — all SQL is explicit.

Key methods:

```java
// SettlementLineRepository
void insertIgnoreDuplicate(SettlementLine line, byte[] outboxPayload, String caseUid)
// Atomically inserts settlement_lines row + outbox(recon.requests) row in one transaction.
// ON CONFLICT (file_id, line_no) DO NOTHING — idempotent on file replay.

// ReconCaseRepository
void upsertAutoResolved(caseUid, settlementLineId, pgTxnId, matchType, confidence)
void upsertPending(caseUid, settlementLineId, matchType)
// upsertPending atomically writes the recon.investigate outbox row only when inserted > 0.

boolean proposeResolution(caseUid, resolution, confidence, resolvedBy, pgTxnId, notes)
// WHERE resolution = 'PENDING' — write guard.

boolean approve(caseUid, reviewer, comment)
// WHERE resolution IN ('PENDING', 'PROPOSED')

boolean reject(caseUid, reviewer, comment)
// WHERE resolution = 'PROPOSED'
```

### 6.9 REST API (`recon-api`)

Base path: `/api/v1`. Auth: `Authorization: Bearer <API_BEARER_TOKEN>`. Only `/healthz` is public.

**`RequestIdFilter`:** Reads `X-Request-ID` header; generates `UUIDv4` if absent. Sets as `MDC` + `@RequestAttribute("requestId")`.

**`BearerAuthFilter`:** Validates `Authorization: Bearer` header against `${recon.api.bearer-token}`. Returns 401 on mismatch. Skips `/healthz`.

**`GlobalExceptionHandler`:** Catches `IllegalArgumentException` (400), unhandled exceptions (500). Returns `ErrorResponse(code, message, requestId)`.

#### Endpoints

**`GET /api/v1/cases`**

Query params: `resolution`, `matchType`, `cursor` (default 0), `limit` (default 50, max 200). Cursor-based pagination by `id`.

200 response:
```json
{
  "cases": [{"case_uid": "...", "match_type": "AMOUNT_MISMATCH", "resolution": "PROPOSED", ...}],
  "next_cursor": 8821
}
```

**`GET /api/v1/cases/{caseUid}`**

200: full case + last `agent_trace` row (model, tools_used, total_ms, cost_usd).
404: case not found.

**`POST /api/v1/cases/{caseUid}/approve`**

Body: `{"reviewer_email": "ops@example.com", "comment": "ok"}`.

- `caseRepo.approve()` — `WHERE resolution IN ('PENDING', 'PROPOSED')`. 409 if 0 rows updated.
- On success: publishes `CaseApprovedEvent` (available for downstream listeners).

**`POST /api/v1/cases/{caseUid}/reject`**

Body identical. `WHERE resolution = 'PROPOSED'`. 409 if not in PROPOSED state.

**`GET /healthz`**

Returns 200 `{"status": "ok"}` if DB reachable.

---

## 7. Sequence Diagrams

### 7.1 Happy path — rules-resolved line

```
SftpWatcher          outbox(recon.requests)      OutboxDrainer    ReconRequestListener    DB
     │ poll SFTP              │                        │                  │                │
     │ download .txt          │                        │                  │                │
     │ SHA-256 + dedup ───────────────────────────────────────────────────────────────────▶│
     │ parse lines            │                        │                  │                │
     │ insertIgnoreDuplicate ─────────────────────────────────────────────────────────────▶│
     │  (line + outbox row atomically)                 │                  │                │
     │                        │ ◀── row inserted       │                  │                │
     │                        │       500ms            │                  │                │
     │                        │──────────────────────▶ │                  │                │
     │                        │  FOR UPDATE SKIP LOCKED│                  │                │
     │                        │                        │ ReconRequestEvent│                │
     │                        │                        │─────────────────▶│                │
     │                        │                        │                  │ evaluate rules │
     │                        │                        │                  │ upsertAutoResolved
     │                        │                        │                  │───────────────▶│
     │                        │                        │ published_at=now │                │
```

### 7.2 Agent investigation path

```
ReconRequestListener         outbox(recon.investigate)    InvestigateOutboxDrainer    AgentOrchestrator
          │ upsertPending             │                             │                        │
          │  (case + outbox atomically)                            │                        │
          │───────────────────────────▶                            │                        │
          │                           │         500ms              │                        │
          │                           │───────────────────────────▶│                        │
          │                           │   FOR UPDATE SKIP LOCKED   │                        │
          │                           │   batch size = 1           │                        │
          │                           │                            │ ReconInvestigateEvent   │
          │                           │                            │────────────────────────▶│
          │                           │                            │                        │ LangChain4j tool loop
          │                           │                            │                        │  search_pg_transactions
          │                           │                            │                        │  get_chargeback_status
          │                           │                            │                        │  propose_resolution
          │                           │                            │                        │   └─ WHERE resolution='PENDING'
          │                           │                            │                        │ insert agent_trace
```

### 7.3 Human approval path

```
Reviewer ──GET /cases?resolution=PROPOSED──▶ CasesController ──SELECT──▶ DB
Reviewer ──POST /cases/{uid}/approve──▶ CasesController
   UPDATE recon_cases SET resolution='APPROVED'
   WHERE case_uid=$ AND resolution IN ('PENDING','PROPOSED')
   → publishEvent(CaseApprovedEvent)
   ← 200 {"status":"APPROVED"}
```

---

## 8. Algorithms

### 8.1 case_uid derivation

```java
// com.recon.common.util.CaseUid
UUID_NAMESPACE = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  // OID namespace

public static String of(String fileId, int lineNo) {
    return UUID.nameUUIDFromBytes(...)  // uuid5(NAMESPACE, "fileId:lineNo")
}
```

Properties: deterministic (no central sequence), collision-free across files, same file replay → same UUID → `ON CONFLICT DO NOTHING` is safe.

### 8.2 Idempotent line write + atomic outbox

```java
// SettlementLineRepository.insertIgnoreDuplicate()
// One transaction:
INSERT INTO settlement_lines(...) ON CONFLICT (file_id, line_no) DO NOTHING RETURNING id
// if id returned:
INSERT INTO outbox(topic, partition_key, payload, schema_id)
VALUES ('recon.requests', caseUid, :protoBytes, 1)
```

If the parser crashes after commit, the outbox row exists and the drainer will re-deliver. If it crashes before commit, neither row exists and the watcher replays the file (SHA-256 dedup is idempotent).

### 8.3 Agent write guard

```sql
UPDATE recon_cases
SET resolution = :resolution, ...
WHERE case_uid = :uid AND resolution = 'PENDING'
```

`rows == 0` → case already resolved → `ProposeResolutionTool` returns `{"status":"STALE"}`. The agent does not retry. This prevents double-resolution when two worker JVMs race.

### 8.4 Outbox drain with concurrent safety

```sql
SELECT id, topic, partition_key, payload
FROM outbox
WHERE topic = 'recon.requests' AND published_at IS NULL
ORDER BY id
LIMIT 100
FOR UPDATE SKIP LOCKED
```

`SKIP LOCKED` makes this safe to run in multiple concurrent JVMs — each grabs a disjoint set of rows. No distributed lock needed.

---

## 9. Configuration (`application.yml` / env vars)

| Env var | Default | Used by |
|---|---|---|
| `DATABASE_URL` | `jdbc:postgresql://localhost:5432/recon` | All |
| `GROQ_API_KEY` | (required for real LLM) | `LangChainAgentConfig` |
| `MOCK_LLM` | `true` | Agent — set `false` in prod |
| `MODEL_INVEST` | `llama-3.3-70b-versatile` | AgentOrchestrator |
| `MODEL_TRIAGE` | `llama-3.1-8b-instant` | AppConfig (reserved) |
| `AGENT_MAX_STEPS` | `6` | AppConfig |
| `AGENT_TIMEOUT_S` | `30` | AgentOrchestrator |
| `RULES_TOLERANCE_PAISE` | `1` | ToleranceRule |
| `PAN_HMAC_KEY` | (required — 32-byte base64) | HmacTokenizer |
| `PAN_AES_KEY` | (required — 32-byte base64) | PanVault |
| `API_BEARER_TOKEN` | `changeme` | BearerAuthFilter |
| `SFTP_HOST` | `localhost` | SftpWatcher |
| `SFTP_PORT` | `2222` | SftpWatcher |
| `SFTP_REMOTE_DIR` | `/config/home/recon/upload` | SftpWatcher |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | OtelConfig |
| `RETRY_INITIAL_S` | `1.0` | AppConfig |
| `RETRY_MAX_ATTEMPTS` | `3` | AppConfig |

Spring profile `watcher` must be active to start `SftpWatcher`.

---

## 10. Error Handling

| Failure | Detection | Action |
|---|---|---|
| Duplicate SFTP file | `processed_files` SHA-256 conflict | Skip, log debug |
| Malformed settlement line | Parse exception in `UdirParser` | Add to `errors` list, skip line |
| Trailer count mismatch | `trailerCount != lines.size()` | Log warning, continue |
| DB unavailable | `DataAccessException` | Outbox holds rows; drainer retries next 500ms tick |
| Agent timeout | `Future.get(30s)` throws `TimeoutException` | `escalate()` → `resolution='ESCALATE'` |
| Agent exception | Any `Exception` in `Future` | `escalate()` → `resolution='ESCALATE'` |
| PII redaction miss | `containsPii()` returns true in `PiiAuditJob` | Metric increment + ERROR log + alert |
| Stale agent write | `proposeResolution()` returns `false` | Log warning, no retry |
| Approve on wrong state | `approve()` returns `false` | 409 Conflict response |

---

## 11. Security Design

- **Network:** Caddy reverse proxy handles TLS termination; Postgres and SFTP are not exposed outside the Docker network.
- **Auth:** Static bearer token for the REST API (`API_BEARER_TOKEN` env var). `/healthz` is public.
- **Secrets:** Loaded from `.env` (dev) or OCI Vault (prod); never logged or embedded in code.
- **PII boundary:** `PiiRedactor` masks VPA/phone/email/PAN before any value reaches the LLM. `ProposeResolutionTool` redacts LLM-generated rationale before DB write. `PiiAuditJob` provides nightly verification.
- **Prompt injection:** Settlement file content reaches the LLM only as a `case_uid` string in the prompt. Actual values are returned by tools (DB lookups), not embedded in the prompt from user-controlled input.
- **Write guard:** Only `ProposeResolutionTool` mutates `recon_cases`. The `WHERE resolution = 'PENDING'` guard prevents replay attacks and concurrent double-writes.

---

## 12. Observability

### Metrics (Prometheus via Micrometer — `ReconMetrics`)

| Metric | Type | Description |
|---|---|---|
| `manual_review_queue_size` | Gauge | Cases awaiting human review |
| `recon_match_rate` | Gauge | Fraction resolved by rules (SLO ≥ 0.90) |
| `recon_dlq_size` | Gauge | Unprocessed DLQ messages |
| `recon_cases_total{match_type, resolved_by}` | Counter | Cases processed by outcome |
| `llm_cost_usd_total{model}` | Counter | Cumulative Groq spend |
| `llm_tokens_total{model, kind}` | Counter | Input / output / cached tokens |
| `agent_stale_write_total` | Counter | Concurrent resolution races |
| `redaction_failure_total` | Counter | PII gateway failures |
| `recon_rules_engine_duration_seconds` | Timer (p50/p95/p99) | Per-case rules latency |
| `recon_agent_duration_seconds` | Timer (p50/p95/p99) | Per-case agent latency |
| `recon_pii_audit_findings_total` | Counter | Nightly unmasked PII hits |

### Traces (OpenTelemetry → Tempo)

OTel SDK initialised in `OtelConfig`; OTLP gRPC export to `otel-collector:4317`. Spans: `recon.watcher`, `recon.parser`, `recon.rules`, `recon.agent`, `recon.tool.*`, `recon.api.*`. Every span carries `case.uid`.

### Alerts

| Alert | Condition | Severity |
|---|---|---|
| `MatchRateLow` | `recon_match_rate < 0.90 for 30m` | page |
| `LLMCostSpike` | `rate(llm_cost_usd_total[5m]) > 0.10/min` | page |
| `RedactionFailure` | `increase(redaction_failure_total[5m]) > 0` | page |
| `ManualQueueGrowing` | `delta(manual_review_queue_size[1h]) > 0` | warn |

---

## 13. Test Strategy

| Layer | Tooling | What is tested |
|---|---|---|
| Unit | JUnit 5 | `CaseUidTest`, `UdirParserTest`, `RulesMutualExclusivityTest`, `PiiRedactorTest`, `ReconMetricsTest` |
| Eval (agent) | `EvalHarnessTest` + YAML cases | 6 golden cases; `ScriptedAnthropicClient` replays canned tool sequences; asserts `resolution`, `tools_used`, `cost_usd ≤ max_cost_usd` |
| Integration | `ReconPipelineIT` + Testcontainers | 4 end-to-end scenarios: happy path, file dedup, agent write guard, outbox idempotency |
| Load | Locust (`eval/locustfile.py`) | 100 users, 60s; p99 < 50ms, <1% error rate |
| PII audit | `PiiAuditJob` nightly | Zero unmasked findings in monitored columns |

Mock LLM: `@ConditionalOnProperty(mock-llm=true)` wires a `ChatLanguageModel` lambda that returns a fixed escalation string — no Groq calls in unit or integration tests.

---

## 14. Deployment

### Docker Compose services

| Service | Image | Role | Memory |
|---|---|---|---|
| `postgres` | `postgres:16` | Database | 512M |
| `sftp` | `linuxserver/openssh-server` | SFTP drop zone | 64M |
| `api` | recon fat JAR | Web server (port 8080), Flyway migrations | 512M |
| `worker` | recon fat JAR | All pipeline beans: SftpWatcher, OutboxDrainer, RulesEngine, AgentOrchestrator | 768M |
| `caddy` | `caddy:2-alpine` | TLS reverse proxy | 64M |
| `otel-collector` | otel/opentelemetry-collector-contrib | OTLP → Tempo | 256M |
| `prometheus` | `prom/prometheus:v2.52.0` | Metrics store | 512M |
| `grafana` | `grafana/grafana:10.4.2` | Dashboards | 256M |
| `tempo` | `grafana/tempo:2.4.2` | Trace store | 512M |

**One fat JAR, two app containers.** `api` and `worker` run the same `recon-api-*.jar` (`scanBasePackages="com.recon"` picks up all modules). The only difference:

- `api`: web server ON, Flyway ON, no profile flag.
- `worker`: web server OFF (`web-application-type=none`), Flyway OFF, `--spring.profiles.active=watcher` (activates `SftpWatcher`).

`SftpWatcher` is the only bean gated by `@Profile("watcher")`. All other beans load in both containers; `FOR UPDATE SKIP LOCKED` on the outbox prevents duplicate processing if both happen to drain the same topic concurrently.

### Key JVM flags (both containers)
```
-XX:+UseG1GC
-XX:MaxRAMPercentage=65.0
-XX:InitialRAMPercentage=30.0
-Dspring.main.lazy-initialization=true
```

---

## 15. Traceability — Core Invariants

| Invariant | Enforcement |
|---|---|
| Each settlement line processed exactly once | `UNIQUE(file_id, line_no)` + `ON CONFLICT DO NOTHING` + SHA-256 file dedup |
| One `recon_case` per line | `case_uid = uuid5(file_id:line_no)`, `ON CONFLICT (case_uid) DO NOTHING` |
| No raw PII to LLM | `PiiRedactor` on all tool outputs + rationale; `PiiAuditJob` verifies nightly |
| Rules deterministic | Pure functions, no DB writes, unit-tested for idempotence |
| Agent decision auditable | `agent_traces` row per investigation; human approval required for PROPOSED |
| DB committed before event dispatched | Outbox row written in same transaction as business write; drainer fires after commit |
| Agent cannot double-resolve | Write guard `WHERE resolution = 'PENDING'`; 0 rows returned → STALE, not retried |

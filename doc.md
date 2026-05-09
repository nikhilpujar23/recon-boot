# Autonomous Payment Reconciliation Agent — Solution Design

> **Ingest** NPCI UPI settlement files via SFTP → **Rules engine** resolves >90% of lines deterministically → **LLM agent** investigates the residual ~5–10% via tool calls and proposes resolutions for human approval.
>
> Simulates NPCI UPI settlement formats (not an exact NPCI spec implementation). Built as a learning project and fintech/AI resume showcase.

**System invariant:** Each `(file_id, line_no)` produces exactly one `recon_case` — idempotent by design.

**SLO:** ≥90% auto-match rate · <1% incorrect auto-resolutions · <$0.01 per agent case

---

## System Guarantees & Invariants

1. **Each settlement line is processed exactly once.** `ON CONFLICT (file_id, line_no) DO NOTHING` + `case_uid = hash(file_id + line_no)` makes every write idempotent.
2. **No raw PII is ever exposed to the LLM.** Redaction wraps every tool boundary; if redaction fails, the LLM call is aborted.
3. **The rules engine is deterministic.** Pure functions, no side effects, strictly ordered. Same input always produces the same match.
4. **Agent decisions are auditable and reversible.** The agent only proposes; a human (or a high-confidence auto-approve) confirms. Every step is logged to `agent_traces`.
5. **The system tolerates duplicate file ingestion.** SHA-256 deduplication at the watcher level; idempotent writes below it.
6. **All side effects are DB-committed before Kafka emits.** The transactional outbox ensures no event is published for a write that didn't commit.

---

## What this solves

Every Indian PA/PG ops team manually reconciles NPCI UPI settlement files against their internal ledger before the next-day cutoff. Each day NPCI drops a UDIR file on your SFTP. Your job: match each line to your internal ledger, post the matched ones, and investigate the rest.

This project automates that loop. The rules engine handles deterministic matches. The LLM agent handles the hard residue — late credits, partial reversals, fee mismatches, deduplicated retries.

## How it works

1. File lands on SFTP → `watchdog` detects it → emits `FileArrived` event
2. Parser streams line-by-line → writes to `settlement_lines`
3. Rules engine runs each line → writes to `recon_cases` (>90% resolved here)
4. Unmatched cases go to the agent → updates `recon_cases` with a proposed resolution
5. Human approves/rejects via the API; high-confidence `MATCH` cases can be auto-approved

## Why it's interesting

- Hybrid rules + LLM: you can talk about *when not to use an LLM*, which is rarer than the reverse
- Eval harness with cost + accuracy gating in CI: catches prompt regressions before they hit production
- PII tokenization before any LLM call: the agent never sees raw identifiers
- Explicit reliability guarantees: at-least-once + idempotent writes + transactional outbox

---

## Architecture

**Two layers:**

**Core** (where all interesting logic lives) — Parser, Rules Engine, LLM Agent, FastAPI

**Infra** (additive) — Kafka (event bus), Postgres (source of truth), Prometheus + Grafana + OTel

```
NPCI          ┌─── Core ─────────────────────────────────────────────┐
(simulator) ──▶  SFTP Watcher                                         │
                     ↓                                                │
                  Parser ──────────────────▶ settlement_lines (PG)   │
                     ↓                                                │
               Rules Engine ──────────────▶ recon_cases (PG)         │
                     │ unmatched                                      │
                     ↓                                                │
               LLM Agent ──tools──▶ DB ──▶ updates recon_cases (PG)  │
                     ↓                                                │
               FastAPI /cases                                         │
               └──────────────────────────────────────────────────┘

              ┌─── Infra ──────────────────────────────────────────┐
              │  Kafka (transport between services — not truth)     │
              │  Prometheus → Grafana · OTel Collector → Tempo      │
              └────────────────────────────────────────────────────┘
```

**Ownership:**
- Parser owns `settlement_lines`
- Rules engine owns `recon_cases` (creates rows)
- Agent owns `recon_cases` (updates rows, conditional on `status = PROPOSED`)

**Key architectural properties:**
- **DB is source of truth; Kafka is transport only.** If Kafka is unavailable, no data is lost — it's all in Postgres.
- **Stateless services:** parser, rules engine, agent. Any instance can handle any message.
- **Stateful service:** Postgres only.
- **Can run without Kafka for local dev / MVP.** Wire parser → rules → agent directly in-process. Kafka is additive, not a prerequisite. Use `make dev` to start the in-memory pipeline.

---

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Language | Python 3.12 + FastAPI | Best LLM/tool-calling ecosystem |
| Async runtime | `asyncio` + `aiokafka` | Non-blocking Kafka consumers |
| Schemas | Protobuf 3 | Versioning + size; real-world pattern |
| Broker | Kafka KRaft (Bitnami) | Real Kafka API; KRaft drops ZooKeeper |
| Storage | Postgres 16 | Ledger + recon_cases + agent_traces + outbox |
| LLM | `claude-haiku-4-5` triage → `claude-sonnet-4-6` investigation | Cheap-then-smart routing |
| Eval | pytest harness + golden YAML cases | Cost + accuracy gating in CI |
| Observability | OTel + Prometheus + Grafana | Standard one-stack setup |
| Deploy | OCI ARM VM + docker-compose + Caddy | Always free |

---

## Domain model

### NPCI UPI settlement files (simulated)

We simulate two file types:

- **UDIR** (UPI Daily Issuer Report) — pipe-separated, per-RRN view of txns, fees, MDR, net settlement
- **Dispute/chargeback file** — adjustments that retroactively change yesterday's net

A simplified UDIR row:
```
TXN_DATE|RRN|UTR|PAYER_VPA|PAYEE_VPA|AMOUNT|FEE|GST|NET|STATUS|TXN_TYPE|REMARKS
2026-04-30|412345678901|HDFC0123456789|alice@okhdfc|merchant@okicici|1500.00|2.00|0.36|1497.64|SUCCESS|P2M|
```

### Internal ledger (Postgres)

```sql
CREATE TABLE pg_transactions (
  id           BIGSERIAL PRIMARY KEY,
  txn_id       TEXT UNIQUE NOT NULL,
  rrn          TEXT,
  utr          TEXT,
  payer_vpa    TEXT,
  payee_vpa    TEXT,
  amount_paise BIGINT NOT NULL,   -- always paise, never floats
  status       TEXT NOT NULL,     -- INITIATED|SUCCESS|FAILED|TIMEOUT
  created_at   TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON pg_transactions(rrn);
CREATE INDEX ON pg_transactions(utr);
-- Composite index for the common lookup pattern in rules engine
CREATE INDEX ON pg_transactions(rrn, utr);

-- One row per file line. ~100k rows/day.
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
  raw            JSONB,           -- full row stored for backfill / reprocessing
  UNIQUE (file_id, line_no)
);
CREATE INDEX ON settlement_lines(rrn, utr);

-- Output of reconciliation. ~10k rows/day (rules resolve the rest without creating cases).
CREATE TABLE recon_cases (
  id              BIGSERIAL PRIMARY KEY,
  case_uid        UUID UNIQUE NOT NULL,  -- derived: hash(file_id || line_no)
  settlement_line BIGINT REFERENCES settlement_lines(id),
  pg_transaction  BIGINT REFERENCES pg_transactions(id),
  match_type      TEXT NOT NULL,  -- EXACT|TOLERANCE|MISSING_LEG|AMOUNT_MISMATCH|DUPLICATE|UNKNOWN
  confidence      NUMERIC(4,3),
  resolution      TEXT,           -- PROPOSED|APPROVED|REJECTED|AUTO_RESOLVED
  resolved_by     TEXT,           -- 'rules' | 'agent' | 'human:<email>'
  notes           JSONB,
  created_at      TIMESTAMPTZ DEFAULT now(),
  resolved_at     TIMESTAMPTZ
);

-- LLM agent traces. Retained 7–14 days (cost control; purge via cron).
CREATE TABLE agent_traces (
  id          BIGSERIAL PRIMARY KEY,
  case_uid    UUID REFERENCES recon_cases(case_uid),
  prompt_hash TEXT,
  model       TEXT,
  tools_used  TEXT[],
  steps       JSONB,   -- [{tool, input_redacted, output, latency_ms}]
  total_ms    INT,
  cost_usd    NUMERIC(10,6),
  created_at  TIMESTAMPTZ DEFAULT now()
);
```

**All amounts in paise (`BIGINT`). Never floats.**

**Cardinality:** ~100k `settlement_lines`/day, ~10k `recon_cases`/day (unmatched residue). `agent_traces` retained 7–14 days then purged.

---

## Ingestion & reliability

1. `atmoz/sftp` exposes port 22. Files land in `/data/upload/`.
2. `watchdog` detects `IN_CLOSE_WRITE`, computes SHA-256, deduplicates via `processed_files(file_hash)`. Emits `FileArrived` Protobuf to Kafka `files.new`.
3. Parser consumes `files.new`, streams line-by-line into `settlement_lines` (`ON CONFLICT (file_id, line_no) DO NOTHING`). Emits one `ReconRequest` per line.
4. Rules worker resolves cases and writes `recon_cases`. Unmatched go to `recon.investigate`.
5. Agent worker investigates, updates `recon_cases`.

**Idempotency key:** `case_uid = SHA-256(file_id + ":" + line_no)` — deterministic, collision-free, derived without a sequence.

**Retry policy:** 3 retries with exponential backoff (1s → 5s → 25s). After 3 failures the message is published to `recon.dlq`. A CLI command `recon dlq replay --topic recon.investigate` replays DLQ messages back to the main topic after the underlying issue is fixed.

**Ordering:** No ordering guarantee across partitions. Rules must be order-independent (and are, since each is a pure function over a single line + its matching PG txns).

**Reliability pattern:** at-least-once delivery + idempotent writes + transactional outbox. Kafka's native EOS covers the broker side; the outbox closes the Kafka↔Postgres gap. Manual offset commit after DB write ensures no offset advances until the idempotent write succeeds.

**Backfill / reprocessing:** `settlement_lines.raw` stores the full row as JSONB. `recon backfill --since 2026-04-01` re-runs rules over historical lines without re-parsing files.

---

## Rules engine

A rule is a pure function: `(SettlementLine, list[PgTxn]) -> Optional[Match]`. Rules are strictly ordered; first match wins, no tie-breaking needed.

| Priority | Rule | Match condition | Type | Confidence |
|---|---|---|---|---|
| 1 | `exact_rrn` | RRN + amount + status SUCCESS | EXACT | 1.0 |
| 2 | `utr_amount` | UTR + amount + same-day | EXACT | 0.99 |
| 3 | `tolerance` | RRN matches, amount ±1 paise | TOLERANCE | 0.95 |
| 4 | `duplicate` | Two file lines for same RRN | DUPLICATE | 0.90 |
| 5 | `amount_mismatch` | RRN matches, amount differs >1 paise | → agent | — |
| 6 | `missing_leg` | RRN in file, no PG txn (or vice versa) | → agent | — |

**Conflict handling:** if multiple PG txns match a single settlement line (e.g. a retried payment), no rule resolves it — the case goes to the agent. Rules never guess in ambiguous situations.

**Extensibility:** rules are pure and side-effect-free. Adding a new rule is one file + one entry in the ordered list. Easy to unit test in isolation and easy to reorder without cross-rule dependencies.

---

## LLM agent

The agent handles cases where `match_type ∈ {MISSING_LEG, AMOUNT_MISMATCH, DUPLICATE}` — the residual ~5–10% that rules can't resolve deterministically.

### Contract

- **Input:** a `ReconCase` with all identifiers redacted
- **Output:** exactly one `propose_resolution(...)` call — nothing else
- **Hard limits:** max 6 tool calls · 30s timeout · temperature = 0 (reproducibility)
- **On timeout or tool failure:** mark case `ESCALATE` automatically
- **Write guard:** agent updates are conditional on `recon_cases.resolution = PROPOSED`; stale writes are rejected
- **The agent never executes a resolution** — it only proposes one

### Routing

1. **Haiku triage** — classifies the case type. If confidence ≥ 0.9, calls `propose_resolution` directly. ~500 tokens in / ~150 out. Cost: cents per case.
2. **Sonnet investigation** — only when Haiku is low-confidence. Full tool access + few-shot exemplars. Max 6 tool calls.

**Prompt caching** — system prompt + tool schemas + exemplars are marked `cache_control: ephemeral`. Per-case delta is the only billed portion. Expected: ~90% savings on repeated sections.

### Tools

```python
search_pg_transactions(rrn, utr, payer_vpa_masked, amount_range, date_range)
get_settlement_history(rrn)
get_chargeback_status(rrn)
compute_fee_breakdown(amount_paise, txn_type)
propose_resolution(case_uid, resolution_type, confidence, rationale, pg_txn_id)
# resolution_type: "MATCH" | "WRITE_OFF" | "ESCALATE" | "RETRY_PG_FETCH"
```

---

## Example walkthrough

**Input:** UDIR line — RRN `412345678901`, amount ₹1500, status `SUCCESS`. PG ledger — same RRN, `status = PARTIAL_REVERSED`, `reversal_paise = 50000`.

**Rules engine:** `exact_rrn` fires but flags `AMOUNT_MISMATCH` (net ≠ full amount). → sent to agent.

**Agent (Haiku):** classifies as `partial_reversal`, confidence 0.72 → routes to Sonnet.

**Agent (Sonnet):**
1. `get_chargeback_status("412345678901")` → confirms ₹500 chargeback processed same day
2. `compute_fee_breakdown(100000, "P2M")` → confirms net after reversal matches settlement line
3. `propose_resolution(case_uid, "MATCH", 0.96, "Partial reversal of ₹500 confirmed; net matches.", pg_txn_id=8821)`

**Output:** case `PROPOSED`, resolved_by `agent`. Human approves via `POST /cases/{uid}/approve`. Cost: ~$0.004.

---

## PII redaction (PCI-inspired)

**Guarantee: no raw PII leaves the DB boundary.** The redaction layer wraps every tool function. If redaction fails for any reason, the LLM call is aborted — fail closed, not open.

```
LLM ── tool call ──▶ redactor (token→raw) ──▶ DB
LLM ◀── result ───── redactor (raw→token) ◀── DB
```

**What is treated as PII:** PAN, account number, VPA, mobile number, email, name.

**What is NOT PII:** RRN and UTR are transaction identifiers issued by NPCI/banks — they are passed to the LLM unmasked, as they are needed for tool lookups and carry no cardholder data.

**PAN tokenization:** keep first 6 (BIN) + last 4 per PCI-DSS 3.4; HMAC-SHA256 the middle digits with a boot-time secret key. Same PAN → same token (cross-message identity without raw value). Tokens are reversible only inside `pan_vault` (AES-256-GCM encrypted).

**VPA / account / phone:** regex-masked inline. No vault needed.

**Auditability:** every prompt and tool result is logged to `agent_traces.steps` post-redaction. A nightly SQL scan checks for unmasked patterns and pages on hit.

---

## Evaluation harness

Golden cases live in `tests/eval/cases/*.yaml`. Each defines the input, expected `resolution_type`, required tool calls, and a cost ceiling.

```yaml
id: partial_reversal_001
scenario:
  settlement_line: { rrn: "412345678901", amount_paise: 150000, status: SUCCESS }
  pg_txn: { rrn: "412345678901", amount_paise: 150000, status: PARTIAL_REVERSED, reversal_paise: 50000 }
expected:
  resolution_type: MATCH
  must_call_tools: [get_chargeback_status, propose_resolution]
  max_cost_usd: 0.005
  min_confidence: 0.85
```

**CI behavior:** `pytest -m eval` runs on every PR against a **mocked Anthropic client** (no real API calls in CI; real calls run manually before merging prompt changes). Haiku is used for cost when real calls are needed.

**Regression gate:** fail the build if accuracy drops >2% OR cost increases >20% versus the last passing run.

**Baseline comparison:** the harness reports rules-only accuracy vs rules+agent accuracy, so the agent's marginal value is always visible.

**Targets:** ≥30 golden cases · ≥90% accuracy · <$0.01/case average. Cases cover: partial reversals, fee/MDR mismatch, late credits, dedup retries, missing-leg both directions, chargeback-affected, malformed rows.

---

## Observability

**Tracing (OTel):** 100% sampling for agent flows; 10% for rules-only flows. Every log line and span is keyed by `case_uid` for correlation.

A full case trace spans file detection → parser → rules → agent → resolution:
```
trace 8a2c...
├─ sftp_watcher.detect_file         12 ms
├─ parser.consume                   18 ms
├─ rules.engine                     45 ms
└─ agent.investigate              1240 ms
   ├─ agent.haiku_triage            320 ms  (cached_input_tokens=1842)
   ├─ tool.get_chargeback_status     12 ms
   └─ agent.sonnet_investigate      880 ms
```

Span attributes: `case.uid`, `match.type`, `agent.model`, `agent.cost_usd`, `agent.cached_tokens`. Never PII in span attributes.

**Key Prometheus metrics:**

```
# Business
manual_review_queue_size              # cases awaiting human approval
recon_match_rate                      # gauge — SLO target >90%
recon_cases_total{match_type, resolved_by}

# Performance
recon_rules_engine_duration_seconds
recon_agent_duration_seconds
llm_cost_usd_total{model}
llm_tokens_total{model, kind="input|output|cached"}

# Health
kafka_consumer_lag{topic, group}
recon_dlq_size
```

**Alerts:**
- `MatchRateLow` — match rate <0.90 for 30m
- `LLMCostSpike` — cost rate >$0.10/5min (likely prompt regression)
- `KafkaLagHigh` — consumer lag >1000 for 5m
- `ManualQueueGrowing` — `manual_review_queue_size` growing for >1h

---

## Tradeoffs

**Why not pure LLM?** Cost and latency. At $0.001/call on 100k daily txns = $100/day. Rules on the deterministic majority brings that to ~$1–2/day. Also: LLM non-determinism is a liability for exact financial matching — mitigated by temperature=0 and the eval harness, but rules are simply more reliable for the easy cases.

**Why Kafka?** Decouples parser throughput from agent throughput (agent is ~1s/case; parser is milliseconds). Enables replay, DLQ, and backpressure. **Downside:** operational overhead — topic management, consumer group coordination, KRaft config. For a solo developer, this is the highest-friction part of the stack. The in-process pipeline mode exists for exactly this reason.

**Why Postgres over a message store?** The transactional outbox pattern means Kafka events are produced atomically with the DB write — no external transaction coordinator. Postgres doubles as ledger, case store, and outbox with a single ACID boundary.

**Why ARM (OCI A1)?** Genuinely free forever. 4 cores + 24 GB is enough for the full LGTM stack. Only cost is the Anthropic API key.

**Why not auto-resolve everything?** `WRITE_OFF` is never auto-approved — financial risk is asymmetric. `MATCH` above a confidence threshold can be auto-approved once eval shows >99% precision.

---

## Repo layout & entry points

```
recon-agent/
├── proto/
│   └── recon.proto              # FileArrived, ReconRequest, ReconResult
├── src/recon/
│   ├── ingest/
│   │   ├── sftp_watcher.py
│   │   └── parser.py
│   ├── rules/
│   │   ├── engine.py            # ordered pipeline, pure functions
│   │   └── rules/               # exact_rrn, utr_amount, tolerance, duplicate
│   ├── agent/
│   │   ├── orchestrator.py      # triage → investigate
│   │   ├── tools.py
│   │   ├── prompts/
│   │   └── router.py
│   ├── pii/
│   │   ├── redactor.py
│   │   └── tokenizer.py
│   ├── ledger/
│   │   ├── repo.py
│   │   └── outbox.py
│   ├── eval/
│   │   ├── harness.py
│   │   └── cases/               # YAML golden cases
│   ├── obs/
│   │   ├── otel.py
│   │   └── metrics.py
│   ├── api/
│   │   └── main.py              # GET /cases, POST /cases/{uid}/approve, GET /healthz
│   ├── cli.py                   # recon backfill, recon dlq replay
│   └── config.py
├── scripts/
│   ├── seed_pg_txns.py
│   └── gen_settlement_file.py
├── docker/
│   └── docker-compose.yml
├── Makefile
└── pyproject.toml
```

**Entry points:**

```bash
make dev          # in-process pipeline, no Kafka, hot-reload — start here
make stack        # docker compose up -d (full Kafka + Postgres + observability)
make seed         # seed PG ledger + generate synthetic UDIR file
make recon        # run reconciliation on /tmp/udir.txt
make eval         # run golden case harness (mocked LLM)
make eval-real    # run eval against real Anthropic API (costs money)
```

**Day 1 target:**
```bash
$ make seed && make recon
matched: 902 (90.2%)
unmatched: 98 (9.8%)
```

Everything else stacks on this. Don't touch Kafka, Docker, or the agent until this loop works in-process.

---

## Milestones (4–6 weekends)

| Week | Deliverable | Done when |
|---|---|---|
| 1 | Schema, file generator, parser, rules engine | `pytest` green; >90% match on synthetic data |
| 2 | Kafka, Protobuf, idempotent consumers, SFTP watcher | Drop file via SFTP → flows end-to-end |
| 3 | LLM agent + tools + routing + prompt caching | First investigation resolved; cost <$0.01 |
| 4 | Eval harness, 30 golden cases, CI gating | CI fails on accuracy or cost regression |
| 5 | Docker, OCI deploy, Caddy + TLS | Public URL; file → resolution on dashboard |
| 6 | PII redaction, OTel + Grafana, alerts, README + video | Metrics flowing; redaction audit passes |

---

## Resume bullets

- *"Hybrid reconciliation — rules engine for deterministic match rate, LLM agent for residual investigation; eval harness gates every prompt change in CI."*
- *"LLM eval harness with golden cases, cost + accuracy regression gates, and baseline comparison of rules-only vs rules+agent accuracy."*
- *"PII tokenization (PCI-inspired) — LLM never sees raw identifiers; deterministic HMAC tokens enable cross-message reasoning without raw values."*
- *"Prompt caching + model routing (Haiku triage → Sonnet investigation) to control inference costs below $0.01/case."*
- *"At-least-once + idempotent writes + transactional outbox; 3-retry exponential backoff with DLQ replay CLI for operational recovery."*
- *"End-to-end OTel tracing from file ingest to agent resolution; 100% sampling on agent flows with per-case cost and cache-hit metrics."*

---

## Appendix: Deployment

Runs on **OCI Always Free** — `VM.Standard.A1.Flex`, 4 OCPUs, 24 GB RAM, ARM64.

### Memory budget

| Process | Allocation |
|---|---|
| Kafka KRaft | 1.5 GB |
| Postgres | 2 GB |
| App workers (×4) | ~800 MB |
| Prometheus + Grafana + Tempo | ~1.7 GB |
| OTel Collector + Caddy | ~300 MB |
| **Total** | **~6.5 GB** (<30% of 24 GB) |

### Setup (summary)

1. Sign up at cloud.oracle.com → Always Free. Region `ap-mumbai-1` or `ap-hyderabad-1`. ARM A1 capacity can be constrained — retry across Availability Domains.
2. Create VM: `VM.Standard.A1.Flex`, 4 OCPUs, 24 GB, Ubuntu 22.04 aarch64, 100 GB boot volume.
3. Open ingress: 22 (SSH, your IP), 80+443 (Caddy), 2222 (SFTP). **OCI gotcha:** also update host-level iptables inside the VM — Ubuntu on OCI has restrictive rules that override the security list.
4. `sudo apt install -y docker.io docker-compose-plugin git && sudo usermod -aG docker $USER`
5. `git clone ... && cp .env.example .env` — fill `ANTHROPIC_API_KEY`, `SFTP_PASS`, `POSTGRES_PASSWORD`, `PAN_TOKEN_HMAC_KEY`, `PAN_VAULT_AES_KEY`.
6. `make stack` — first ARM build ~10 min; subsequent builds are fast.
7. Point DNS → VM IP; Caddy auto-fetches Let's Encrypt certs.

**ARM check:** run `docker manifest inspect <image>` for each image and confirm `linux/arm64` before first deploy.

### CI/CD

GitHub Actions: `lint+test` on push → `eval` (mocked LLM) on PR → `deploy` on main (SSH `git pull && make stack`).

### Open risks

- **OCI capacity:** ARM shapes sometimes unavailable at signup. Retry across ADs; usually resolves within a day.
- **NPCI format realism:** the spec is closely held — label as "simulated format" in the README and cite public RBI/NPCI docs.
- **ARM wheels:** `grpcio` and older `cryptography` may lack `aarch64` wheels — pin to versions with `manylinux2014_aarch64` support. `aiokafka` is pure-Python and sidesteps this.
- **Key custody:** for production-shape credentials, use OCI Vault free tier + instance principal instead of `.env`.

### Video walkthrough

Non-negotiable for the resume. Recruiters won't `git clone` — they'll watch a 90-second screen recording. Show: file drop via SFTP → Grafana metrics updating → one case resolved by the agent in the UI.

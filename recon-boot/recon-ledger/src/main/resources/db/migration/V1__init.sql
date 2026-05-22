-- Phase 1 schema — run once: psql $DATABASE_URL -f migrations/001_init.sql

-- ── pg_transactions: internal ledger ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pg_transactions (
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
CREATE INDEX IF NOT EXISTS idx_pg_txn_rrn        ON pg_transactions(rrn);
CREATE INDEX IF NOT EXISTS idx_pg_txn_utr        ON pg_transactions(utr);
CREATE INDEX IF NOT EXISTS idx_pg_txn_rrn_utr    ON pg_transactions(rrn, utr);
CREATE INDEX IF NOT EXISTS idx_pg_txn_created_at ON pg_transactions(created_at);

-- ── settlement_lines: parsed UDIR rows ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS settlement_lines (
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
CREATE INDEX IF NOT EXISTS idx_sl_rrn_utr ON settlement_lines(rrn, utr);
CREATE INDEX IF NOT EXISTS idx_sl_file_id ON settlement_lines(file_id);

-- ── recon_cases: resolution state ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS recon_cases (
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
CREATE INDEX IF NOT EXISTS idx_rc_resolution ON recon_cases(resolution);
CREATE INDEX IF NOT EXISTS idx_rc_match_type ON recon_cases(match_type);
CREATE INDEX IF NOT EXISTS idx_rc_created_at ON recon_cases(created_at);

-- ── agent_traces: LLM call trace store ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS agent_traces (
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
CREATE INDEX IF NOT EXISTS idx_at_case_uid   ON agent_traces(case_uid);
CREATE INDEX IF NOT EXISTS idx_at_created_at ON agent_traces(created_at);

-- ── processed_files: file-level dedup ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS processed_files (
    file_hash   CHAR(64) PRIMARY KEY,
    file_id     TEXT NOT NULL,
    filename    TEXT NOT NULL,
    bytes       BIGINT,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

-- ── outbox: transactional outbox ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL PRIMARY KEY,
    topic         TEXT  NOT NULL,
    partition_key TEXT,
    payload       BYTEA NOT NULL,
    schema_id     INT   NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(created_at) WHERE published_at IS NULL;

-- ── pan_vault: encrypted PAN vault ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pan_vault (
    token      TEXT PRIMARY KEY,
    ciphertext BYTEA NOT NULL,
    iv         BYTEA NOT NULL,
    tag        BYTEA NOT NULL,
    bin        CHAR(6),
    last4      CHAR(4),
    created_at TIMESTAMPTZ DEFAULT now()
);

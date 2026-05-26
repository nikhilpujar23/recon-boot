-- ── refunds: merchant-initiated refunds against original transactions ─────────
CREATE TABLE IF NOT EXISTS refunds (
    id             BIGSERIAL PRIMARY KEY,
    original_rrn   TEXT        NOT NULL,
    refund_id      TEXT UNIQUE NOT NULL,
    amount_paise   BIGINT      NOT NULL CHECK (amount_paise > 0),
    status         TEXT        NOT NULL CHECK (status IN ('INITIATED', 'SUCCESS', 'FAILED')),
    reason         TEXT,
    initiated_at   TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_refunds_rrn ON refunds(original_rrn);

-- ── merchant_fee_config: per-merchant fee slabs ───────────────────────────────
-- effective_to NULL means the config is currently active.
-- Multiple rows per merchant are allowed for historical slabs.
CREATE TABLE IF NOT EXISTS merchant_fee_config (
    id              BIGSERIAL PRIMARY KEY,
    merchant_vpa    TEXT           NOT NULL,
    txn_type        TEXT           NOT NULL CHECK (txn_type IN ('P2P', 'P2M')),
    mdr_rate        NUMERIC(6, 4)  NOT NULL,
    gst_applicable  BOOLEAN        NOT NULL DEFAULT true,
    effective_from  DATE           NOT NULL,
    effective_to    DATE,
    UNIQUE (merchant_vpa, txn_type, effective_from)
);
CREATE INDEX IF NOT EXISTS idx_mfc_merchant_vpa ON merchant_fee_config(merchant_vpa);

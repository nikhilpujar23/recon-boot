-- ── chargeback_events: chargebacks filed against settled transactions ──────────
CREATE TABLE IF NOT EXISTS chargeback_events (
    id            BIGSERIAL PRIMARY KEY,
    rrn           TEXT        NOT NULL,
    amount_paise  BIGINT      NOT NULL CHECK (amount_paise > 0),
    status        TEXT        NOT NULL CHECK (status IN ('OPEN', 'ACCEPTED', 'REJECTED')),
    reason        TEXT,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cb_rrn ON chargeback_events(rrn);

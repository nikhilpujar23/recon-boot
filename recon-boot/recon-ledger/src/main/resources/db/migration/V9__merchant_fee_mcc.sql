-- Replace merchant_vpa with mcc on merchant_fee_config.
-- Fee slabs in practice are per MCC category, not per individual VPA.
ALTER TABLE merchant_fee_config
    ADD COLUMN IF NOT EXISTS mcc TEXT;

ALTER TABLE merchant_fee_config
    DROP COLUMN IF EXISTS merchant_vpa;

DROP INDEX IF EXISTS idx_mfc_merchant_vpa;
CREATE INDEX IF NOT EXISTS idx_mfc_mcc ON merchant_fee_config(mcc);

ALTER TABLE merchant_fee_config
    ADD CONSTRAINT IF NOT EXISTS uq_mfc_mcc_txn_type_from
    UNIQUE (mcc, txn_type, effective_from);

-- Add mcc to pg_transactions so the tool can resolve it without exposing any VPA.
ALTER TABLE pg_transactions
    ADD COLUMN IF NOT EXISTS mcc TEXT;

-- V9 used ADD CONSTRAINT IF NOT EXISTS which is not valid PostgreSQL syntax.
-- The column-level DDL in V9 committed, but the constraint was never created.
-- Add it here without the invalid IF NOT EXISTS clause.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_mfc_mcc_txn_type_from'
          AND conrelid = 'merchant_fee_config'::regclass
    ) THEN
        ALTER TABLE merchant_fee_config
            ADD CONSTRAINT uq_mfc_mcc_txn_type_from
            UNIQUE (mcc, txn_type, effective_from);
    END IF;
END $$;

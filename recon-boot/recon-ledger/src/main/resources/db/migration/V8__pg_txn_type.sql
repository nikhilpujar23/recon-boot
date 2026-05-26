ALTER TABLE pg_transactions
    ADD COLUMN IF NOT EXISTS txn_type TEXT
        CHECK (txn_type IN ('P2P', 'P2M'));

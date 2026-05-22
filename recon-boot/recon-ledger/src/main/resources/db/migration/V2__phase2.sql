-- Phase 2 schema additions — run once after 001_init.sql:
--   psql $DATABASE_URL -f migrations/002_phase2.sql

-- ── Postgres NOTIFY trigger on outbox ────────────────────────────────────────
-- Fires NOTIFY 'outbox_new' after every INSERT to outbox.
-- The OutboxPublisher listens on this channel and wakes its drain loop
-- immediately instead of waiting for the 100ms poll interval.

CREATE OR REPLACE FUNCTION notify_outbox_new()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('outbox_new', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_outbox_new ON outbox;

CREATE TRIGGER trg_outbox_new
  AFTER INSERT ON outbox
  FOR EACH ROW
  EXECUTE FUNCTION notify_outbox_new();

-- ── Index: outbox batch drain (already exists via 001, but made explicit) ────
-- idx_outbox_unpublished already created in 001_init.sql via:
--   CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(created_at)
--   WHERE published_at IS NULL;
-- No action needed; documented here for clarity.

-- ── Index: recon_cases partial — open review queue ───────────────────────────
-- Speeds up the agent/API query for PROPOSED cases.
CREATE INDEX IF NOT EXISTS idx_rc_proposed
    ON recon_cases(created_at)
    WHERE resolution = 'PROPOSED';

-- ── Index: recon_cases for agent investigation ────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_rc_null_resolution
    ON recon_cases(created_at)
    WHERE resolution IS NULL;

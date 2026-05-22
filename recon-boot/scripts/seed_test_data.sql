-- Local test seed: inserts pg_transactions, settlement_lines, and recon_cases
-- directly (bypasses Kafka / protobuf pipeline).
--
-- Usage:
--   docker compose exec postgres psql -U recon recon -f /dev/stdin < scripts/seed_test_data.sql
-- Or from the host if psql is installed:
--   psql postgresql://recon:recon@localhost:5432/recon -f scripts/seed_test_data.sql
--
-- Covers all 5 match types so you can exercise every API path.

BEGIN;

-- ── Clean slate (idempotent re-run) ──────────────────────────────────────────
DELETE FROM agent_traces   WHERE case_uid IN (SELECT case_uid FROM recon_cases WHERE notes->>'source' = 'seed');
DELETE FROM recon_cases    WHERE notes->>'source' = 'seed';
DELETE FROM settlement_lines WHERE file_id = 'SEED_FILE_001';
DELETE FROM pg_transactions  WHERE txn_id LIKE 'SEED_%';

-- ── 1. pg_transactions ────────────────────────────────────────────────────────

INSERT INTO pg_transactions (txn_id, rrn, utr, payer_vpa, payee_vpa, amount_paise, status, created_at, updated_at) VALUES
  -- EXACT match (line 1)
  ('SEED_TXN_001', 'RRN000000000001', 'UTR001', 'alice@upi', 'merchant@upi', 50000, 'SUCCESS', now(), now()),
  -- TOLERANCE match (line 2): PG has 10000, settlement has 10001 — within 1 paise tolerance
  ('SEED_TXN_002', 'RRN000000000002', 'UTR002', 'bob@upi',   'merchant@upi', 10000, 'SUCCESS', now(), now()),
  -- AMOUNT MISMATCH (line 3): PG has 75000, settlement has 99999 — too far apart
  ('SEED_TXN_003', 'RRN000000000003', 'UTR003', 'carol@upi', 'merchant@upi', 75000, 'SUCCESS', now(), now()),
  -- DUPLICATE (lines 4 + 5): same RRN/amount appears twice in settlement
  ('SEED_TXN_004', 'RRN000000000004', 'UTR004', 'dave@upi',  'merchant@upi', 20000, 'SUCCESS', now(), now());
  -- MISSING LEG (line 6): no PG txn for this RRN at all

-- ── 2. settlement_lines ───────────────────────────────────────────────────────

INSERT INTO settlement_lines (file_id, line_no, rrn, utr, amount_paise, fee_paise, net_paise, status, raw) VALUES
  ('SEED_FILE_001', 1, 'RRN000000000001', 'UTR001', 50000, 450, 49550, 'SUCCESS',
   '{"TXN_DATE":"2026-05-10","REMARKS":"exact match"}'),
  ('SEED_FILE_001', 2, 'RRN000000000002', 'UTR002', 10001, 90,  9911,  'SUCCESS',
   '{"TXN_DATE":"2026-05-10","REMARKS":"within tolerance"}'),
  ('SEED_FILE_001', 3, 'RRN000000000003', 'UTR003', 99999, 900, 99099, 'SUCCESS',
   '{"TXN_DATE":"2026-05-10","REMARKS":"amount mismatch"}'),
  ('SEED_FILE_001', 4, 'RRN000000000004', 'UTR004', 20000, 180, 19820, 'SUCCESS',
   '{"TXN_DATE":"2026-05-10","REMARKS":"first occurrence"}'),
  ('SEED_FILE_001', 5, 'RRN000000000004', 'UTR004', 20000, 180, 19820, 'SUCCESS',
   '{"TXN_DATE":"2026-05-10","REMARKS":"duplicate line"}'),
  ('SEED_FILE_001', 6, 'RRN000000000099', null,     30000, 270, 29730, 'SUCCESS',
   '{"TXN_DATE":"2026-05-10","REMARKS":"no pg txn exists"}');

-- ── 3. recon_cases (bypass Kafka — insert outcomes directly) ─────────────────

WITH lines AS (
  SELECT id, line_no FROM settlement_lines WHERE file_id = 'SEED_FILE_001'
),
txns AS (
  SELECT id, txn_id FROM pg_transactions WHERE txn_id LIKE 'SEED_%'
)
INSERT INTO recon_cases (case_uid, settlement_line, pg_transaction, match_type, confidence, resolution, resolved_by, notes)
SELECT
  gen_random_uuid(),
  l.id,
  t.id,
  v.match_type::text,
  v.confidence,
  v.resolution::text,
  v.resolved_by,
  jsonb_build_object('source', 'seed', 'remarks', v.remarks)
FROM (VALUES
  (1, 'SEED_TXN_001', 'EXACT',          1.0,  'AUTO_RESOLVED', 'rules',  'exact rrn + amount match'),
  (2, 'SEED_TXN_002', 'TOLERANCE',      0.95, 'AUTO_RESOLVED', 'rules',  '1 paise within tolerance'),
  (3, 'SEED_TXN_003', 'AMOUNT_MISMATCH',0.6,  'PROPOSED',      'agent',  'pg=75000 settlement=99999'),
  (4, 'SEED_TXN_004', 'DUPLICATE',      0.99, 'AUTO_RESOLVED', 'rules',  'first occurrence'),
  (5, 'SEED_TXN_004', 'DUPLICATE',      0.99, 'AUTO_RESOLVED', 'rules',  'duplicate settlement line'),
  (6, null,           'MISSING_LEG',    0.0,  'PROPOSED',      'agent',  'no pg txn found')
) AS v(line_no, txn_id, match_type, confidence, resolution, resolved_by, remarks)
JOIN lines l ON l.line_no = v.line_no
LEFT JOIN txns t ON t.txn_id = v.txn_id;

COMMIT;

-- ── Verify ────────────────────────────────────────────────────────────────────
SELECT match_type, resolution, COUNT(*) AS n
FROM recon_cases
WHERE notes->>'source' = 'seed'
GROUP BY 1, 2
ORDER BY 1;

#!/usr/bin/env bash
# End-to-end pipeline smoke test (no Kafka).
#
# Flow under test:
#   SFTP file drop → SftpWatcher → settlement_lines + outbox
#   → OutboxDrainer (500ms) → ReconRequestEvent
#   → ReconRequestListener → rules engine → recon_cases
#   → REST API assertions
#
# Prerequisites:
#   docker compose up -d       (postgres + sftp + the Spring app)
#
# Usage:
#   ./scripts/test_pipeline.sh [--base-url http://localhost:8080] [--token changeme]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8080}"
TOKEN="${API_BEARER_TOKEN:-changeme}"
MAX_WAIT=300       # seconds to wait for pipeline
POLL_INTERVAL=5    # DB poll cadence

# ── helpers ───────────────────────────────────────────────────────────────────
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*"; FAILURES=$((FAILURES + 1)); }
sep()  { echo; printf '\033[1m── %s\033[0m\n' "$*"; }
FAILURES=0

db() {
  docker compose exec -T postgres psql -U recon recon -tAq -c "$1"
}

api() {
  local method="${1:-GET}" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -s -X "$method" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "$body" \
      "$BASE_URL$path"
  else
    curl -s -X "$method" \
      -H "Authorization: Bearer $TOKEN" \
      "$BASE_URL$path"
  fi
}

api_code() {
  local method="${1:-GET}" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -s -o /dev/null -w '%{http_code}' -X "$method" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "$body" \
      "$BASE_URL$path"
  else
    curl -s -o /dev/null -w '%{http_code}' -X "$method" \
      -H "Authorization: Bearer $TOKEN" \
      "$BASE_URL$path"
  fi
}

# ── 0. Prerequisites ──────────────────────────────────────────────────────────
sep "0. Checking prerequisites"

for cmd in docker curl python3; do
  command -v "$cmd" >/dev/null 2>&1 && ok "$cmd found" || { fail "$cmd not found"; exit 1; }
done

docker compose ps --format '{{.Name}} {{.Status}}' | grep -q "postgres.*healthy" \
  && ok "postgres container healthy" \
  || { fail "postgres not healthy — run: docker compose up -d"; exit 1; }

docker compose ps --format '{{.Name}} {{.State}}' | grep -q "sftp.*running" \
  && ok "sftp container running" \
  || { fail "sftp not running — run: docker compose up -d"; exit 1; }

docker compose ps --format '{{.Name}} {{.State}}' | grep -q "api.*running" \
  && ok "api container running" \
  || { fail "api not running — run: docker compose up -d api"; exit 1; }

# ── 1. Health check ───────────────────────────────────────────────────────────
sep "1. Health check (waiting up to 300s for app to start)"

for i in $(seq 1 60); do
  HEALTH=$(curl -sf "$BASE_URL/healthz" 2>/dev/null || true)
  if [[ -n "$HEALTH" ]]; then
    DB_STATUS=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('db','?'))")
    [[ "$DB_STATUS" == "ok" ]] && ok "app healthy, db=ok" && break
  fi
  [[ $i -eq 60 ]] && { fail "app did not start within 300s"; exit 1; }
  echo "  waiting... ($((i*5))s)"
  sleep 5
done

# ── 2. Seed pg_transactions ───────────────────────────────────────────────────
sep "2. Seeding pg_transactions for expected matches"

# Generate deterministic RRNs/UTRs so we can assert on them later
TS=$(date +%s)
RRN_EXACT="RRN_EXACT_$TS"
RRN_TOLE="RRN_TOLE_$TS"
RRN_MISS="RRN_MISS_$TS"
UTR_EXACT="UTR_EXACT_$TS"
UTR_TOLE="UTR_TOLE_$TS"

AMOUNT_EXACT=50000   # paise = ₹500.00
AMOUNT_TOLE=10000    # PG has 10000; settlement will have 10001 (1p tolerance)
# No pg_transaction for RRN_MISS → triggers MISSING_LEG

db "
INSERT INTO pg_transactions (txn_id, rrn, utr, amount_paise, status, created_at, updated_at)
VALUES
  ('TXN_EXACT_$TS', '$RRN_EXACT', '$UTR_EXACT', $AMOUNT_EXACT, 'SUCCESS', now(), now()),
  ('TXN_TOLE_$TS',  '$RRN_TOLE',  '$UTR_TOLE',  $AMOUNT_TOLE,  'SUCCESS', now(), now())
ON CONFLICT DO NOTHING;
" > /dev/null
ok "seeded 2 pg_transactions (EXACT + TOLERANCE scenarios)"

# ── 3. Generate UDIR settlement file ─────────────────────────────────────────
sep "3. Generating UDIR settlement file"

TMPDIR_LOCAL=$(mktemp -d)
FILE="$TMPDIR_LOCAL/udir_test_$TS.txt"
FEE_EXACT=$((AMOUNT_EXACT / 100))
NET_EXACT=$((AMOUNT_EXACT - FEE_EXACT))
AMOUNT_TOLE_SETTLE=$((AMOUNT_TOLE + 1))   # 1p over → TOLERANCE match
FEE_TOLE=$((AMOUNT_TOLE / 100))
NET_TOLE=$((AMOUNT_TOLE - FEE_TOLE))
AMOUNT_MISS=25000
FEE_MISS=$((AMOUNT_MISS / 100))
NET_MISS=$((AMOUNT_MISS - FEE_MISS))

cat > "$FILE" <<EOF
HDR|UDIR_$TS|$(date +%Y%m%d)|3
$RRN_EXACT|$UTR_EXACT|$AMOUNT_EXACT|$FEE_EXACT|$NET_EXACT|SUCCESS
$RRN_TOLE|$UTR_TOLE|$AMOUNT_TOLE_SETTLE|$FEE_TOLE|$NET_TOLE|SUCCESS
$RRN_MISS|UTR_MISS_$TS|$AMOUNT_MISS|$FEE_MISS|$NET_MISS|SUCCESS
TRL|3
EOF

FILENAME=$(basename "$FILE")
ok "generated $FILE (3 lines: EXACT, TOLERANCE, MISSING_LEG)"

# ── 4. Drop file to SFTP ──────────────────────────────────────────────────────
sep "4. Dropping file into SFTP upload directory"

UPLOAD_DIR="/config/home/recon/upload"
docker compose cp "$FILE" "sftp:$UPLOAD_DIR/$FILENAME"
ok "copied $FILENAME → sftp:$UPLOAD_DIR"
rm -rf "$TMPDIR_LOCAL"

# ── 5. Wait: settlement_lines written ────────────────────────────────────────
sep "5. Waiting for SftpWatcher to ingest file (fixedDelay=5s)"

ELAPSED=0
FOUND_LINES=0
while [[ $ELAPSED -lt $MAX_WAIT ]]; do
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))

  FOUND_LINES=$(db "SELECT COUNT(*) FROM settlement_lines WHERE rrn IN ('$RRN_EXACT','$RRN_TOLE','$RRN_MISS')")
  echo "  [${ELAPSED}s] settlement_lines found: $FOUND_LINES/3"
  [[ "$FOUND_LINES" -eq 3 ]] && break
done

[[ "$FOUND_LINES" -eq 3 ]] && ok "all 3 settlement lines ingested" \
  || { fail "timeout: only $FOUND_LINES/3 lines ingested after ${MAX_WAIT}s"; exit 1; }

# ── 6. Wait: outbox drained ───────────────────────────────────────────────────
sep "6. Waiting for OutboxDrainer to dispatch events (fixedDelay=500ms)"

ELAPSED=0
OUTBOX_PENDING=3
while [[ $ELAPSED -lt 30 ]]; do
  sleep 2
  ELAPSED=$((ELAPSED + 2))
  OUTBOX_PENDING=$(db "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL AND partition_key IN ('$RRN_EXACT','$RRN_TOLE','$RRN_MISS')")
  echo "  [${ELAPSED}s] unpublished outbox rows: $OUTBOX_PENDING"
  [[ "$OUTBOX_PENDING" -eq 0 ]] && break
done

[[ "$OUTBOX_PENDING" -eq 0 ]] && ok "outbox drained (all events dispatched)" \
  || fail "outbox still has $OUTBOX_PENDING unpublished rows"

# ── 7. Wait: recon_cases created ─────────────────────────────────────────────
sep "7. Waiting for ReconRequestListener to create cases"

ELAPSED=0
FOUND_CASES=0
while [[ $ELAPSED -lt 30 ]]; do
  sleep 2
  ELAPSED=$((ELAPSED + 2))
  FOUND_CASES=$(db "SELECT COUNT(*) FROM recon_cases c JOIN settlement_lines s ON s.id = c.settlement_line WHERE s.rrn IN ('$RRN_EXACT','$RRN_TOLE','$RRN_MISS')")
  echo "  [${ELAPSED}s] recon_cases created: $FOUND_CASES/3"
  [[ "$FOUND_CASES" -eq 3 ]] && break
done

[[ "$FOUND_CASES" -eq 3 ]] && ok "all 3 cases created" \
  || { fail "timeout: only $FOUND_CASES/3 cases created after ${MAX_WAIT}s"; exit 1; }

# ── 8. Assert match types ─────────────────────────────────────────────────────
sep "8. Asserting match types"

check_case() {
  local rrn="$1" expected_match="$2" expected_res="$3"
  local row
  row=$(db "
    SELECT c.match_type, c.resolution
    FROM   recon_cases c
    JOIN   settlement_lines s ON s.id = c.settlement_line
    WHERE  s.rrn = '$rrn'
    LIMIT  1
  ")
  local match res
  match=$(echo "$row" | cut -d'|' -f1 | tr -d ' ')
  res=$(echo "$row"   | cut -d'|' -f2 | tr -d ' ')

  if [[ "$match" == "$expected_match" && "$res" == "$expected_res" ]]; then
    ok "$rrn → match=$match resolution=$res"
  else
    fail "$rrn → expected match=$expected_match res=$expected_res, got match=$match res=$res"
  fi
}

check_case "$RRN_EXACT" "EXACT"       "AUTO_RESOLVED"
check_case "$RRN_TOLE"  "TOLERANCE"   "AUTO_RESOLVED"
check_case "$RRN_MISS"  "MISSING_LEG" "PENDING"

# ── 9. REST API: health ───────────────────────────────────────────────────────
sep "9. REST: GET /healthz"

HEALTH=$(curl -sf "$BASE_URL/healthz")
DB_OK=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('db','?'))")
[[ "$DB_OK" == "ok" ]] && ok "db=ok" || fail "db status: $DB_OK"

# ── 10. REST API: 401 without token ──────────────────────────────────────────
sep "10. REST: 401 without token"
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/api/v1/cases")
[[ "$CODE" == "401" ]] && ok "got 401 as expected" || fail "expected 401, got $CODE"

# ── 11. REST API: list cases ──────────────────────────────────────────────────
sep "11. REST: GET /api/v1/cases"
ALL=$(api GET "/api/v1/cases?limit=50")
TOTAL=$(echo "$ALL" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['cases']))")
[[ "$TOTAL" -ge 3 ]] && ok "listed $TOTAL cases (≥3)" || fail "expected ≥3 cases, got $TOTAL"

# ── 12. REST API: filter AUTO_RESOLVED ───────────────────────────────────────
sep "12. REST: filter by resolution=AUTO_RESOLVED"
AUTO=$(api GET "/api/v1/cases?resolution=AUTO_RESOLVED&limit=50")
AUTO_COUNT=$(echo "$AUTO" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['cases']))")
[[ "$AUTO_COUNT" -ge 2 ]] && ok "$AUTO_COUNT AUTO_RESOLVED cases (≥2)" || fail "expected ≥2, got $AUTO_COUNT"

# ── 13. REST API: get single case ─────────────────────────────────────────────
sep "13. REST: GET /api/v1/cases/:caseUid"
PENDING_CASE_UID=$(api GET "/api/v1/cases?resolution=PENDING&limit=1" \
  | python3 -c "import sys,json; cases=json.load(sys.stdin)['cases']; print(cases[0]['caseUid'] if cases else '')")

if [[ -n "$PENDING_CASE_UID" ]]; then
  DETAIL=$(api GET "/api/v1/cases/$PENDING_CASE_UID")
  GOT_UID=$(echo "$DETAIL" | python3 -c "import sys,json; print(json.load(sys.stdin)['case']['caseUid'])")
  [[ "$GOT_UID" == "$PENDING_CASE_UID" ]] && ok "fetched case $PENDING_CASE_UID" \
    || fail "caseUid mismatch: expected $PENDING_CASE_UID got $GOT_UID"
else
  ok "no PENDING cases to fetch (all auto-resolved)"
fi

# ── 14. REST API: 404 for unknown case ───────────────────────────────────────
sep "14. REST: 404 for unknown caseUid"
CODE=$(api_code GET "/api/v1/cases/00000000-0000-0000-0000-000000000000")
[[ "$CODE" == "404" ]] && ok "got 404 as expected" || fail "expected 404, got $CODE"

# ── 15. REST API: approve a PENDING case ────────────────────────────────────
sep "15. REST: approve a PENDING case"
if [[ -n "$PENDING_CASE_UID" ]]; then
  APPROVE=$(api POST "/api/v1/cases/$PENDING_CASE_UID/approve" \
    '{"reviewer_email":"tester@recon.local","comment":"verified correct"}')
  STATUS=$(echo "$APPROVE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))")
  [[ "$STATUS" == "APPROVED" ]] && ok "case $PENDING_CASE_UID approved" \
    || fail "expected APPROVED, got $STATUS"

  sep "15b. Stale write guard (expect 409)"
  CODE=$(api_code POST "/api/v1/cases/$PENDING_CASE_UID/approve" \
    '{"reviewer_email":"tester@recon.local","comment":"duplicate"}')
  [[ "$CODE" == "409" ]] && ok "got 409 on stale approve" || fail "expected 409, got $CODE"
else
  ok "skipped (no PENDING cases)"
fi

# ── 16. Outbox idempotency check ─────────────────────────────────────────────
sep "16. Outbox idempotency: exactly 1 outbox row per settlement line"

BAD=$(db "
  SELECT COUNT(*)
  FROM (
    SELECT partition_key
    FROM   outbox
    WHERE  partition_key IN ('$RRN_EXACT','$RRN_TOLE','$RRN_MISS')
    GROUP  BY partition_key
    HAVING COUNT(*) > 1
  ) t
")
[[ "$BAD" -eq 0 ]] && ok "no duplicate outbox rows" \
  || fail "$BAD partition_keys have >1 outbox row"

# ── Summary ───────────────────────────────────────────────────────────────────
sep "Summary"

echo
echo "  DB state:"
db "
  SELECT match_type, resolution, COUNT(*) AS n
  FROM   recon_cases
  WHERE  created_at > now() - interval '5 minutes'
  GROUP  BY 1, 2
  ORDER  BY 1
" | while IFS='|' read -r mt res n; do
  printf '    %-20s %-15s %s cases\n' "$mt" "$res" "$n"
done

echo
if [[ $FAILURES -eq 0 ]]; then
  printf '\033[32m  All checks passed.\033[0m\n'
else
  printf '\033[31m  %d check(s) failed.\033[0m\n' "$FAILURES"
  exit 1
fi

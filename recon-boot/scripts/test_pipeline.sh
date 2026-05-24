#!/usr/bin/env bash
# End-to-end pipeline smoke test (no Kafka).
#
# ─── DECISION FLOW ───────────────────────────────────────────────────────────
#
#   SFTP drop → SftpWatcher → settlement_lines + outbox
#     → OutboxDrainer → ReconRequestEvent
#     → ReconRequestListener (RULE ENGINE)
#          │
#          ├─ ExactRrnRule    (RRN + exact amount + SUCCESS)  → EXACT      → AUTO_RESOLVED
#          ├─ UtrAmountRule   (UTR + exact amount + same day) → EXACT      → AUTO_RESOLVED
#          ├─ ToleranceRule   (RRN + ≤1p gap + SUCCESS)       → TOLERANCE  → AUTO_RESOLVED
#          ├─ DuplicateRule   (same RRN seen earlier in file) → DUPLICATE  → AUTO_RESOLVED
#          ├─ AmountMismatch  (RRN found, amount gap > tol)   → AMT_MISMATCH → PENDING ─┐
#          └─ MissingLegRule  (no RRN or UTR in pg_txns)      → MISSING_LEG  → PENDING ─┤
#                                                                                        ↓
#                                                                          LLM AGENT (Groq)
#                                                                          tools available:
#                                                                            search_pg_transactions
#                                                                            get_settlement_history
#                                                                            get_chargeback_status
#                                                                            compute_fee_breakdown
#                                                                            propose_resolution  ← terminal
#
# ─── SCENARIOS IN THIS SCRIPT ────────────────────────────────────────────────
#
#   Scenario A  [RULE ENGINE]  RRN exact match               → EXACT / AUTO_RESOLVED
#   Scenario B  [RULE ENGINE]  RRN + 1p tolerance            → TOLERANCE / AUTO_RESOLVED
#   Scenario C  [RULE ENGINE]  No RRN, UTR+amount same day   → EXACT / AUTO_RESOLVED
#   Scenario D  [LLM AGENT]    RRN match, 200p amount gap    → AMOUNT_MISMATCH / agent decides
#   Scenario E  [LLM AGENT]    No PG txn at all              → MISSING_LEG / agent decides
#
# Prerequisites:
#   docker compose up -d
#
# Usage:
#   ./scripts/test_pipeline.sh [--base-url http://localhost:8080] [--token changeme]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8080}"
TOKEN="${API_BEARER_TOKEN:-changeme}"
MAX_WAIT=300
POLL_INTERVAL=5

# ── helpers ───────────────────────────────────────────────────────────────────
ok()      { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail()    { printf '  \033[31m✗\033[0m %s\n' "$*"; FAILURES=$((FAILURES + 1)); }
sep()     { echo; printf '\033[1m── %s\033[0m\n' "$*"; }
rule_tag(){ printf '  \033[36m[RULE ENGINE]\033[0m %s\n' "$*"; }
llm_tag() { printf '  \033[35m[LLM AGENT  ]\033[0m %s\n' "$*"; }
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
sep "2. Seeding pg_transactions for all 5 scenarios"

TS=$(date +%s)

# Scenario A — EXACT via RRN  (Rule 1: ExactRrnRule)
RRN_EXACT="RRN_EXACT_$TS"  ; UTR_EXACT="UTR_EXACT_$TS"   ; AMOUNT_EXACT=50000

# Scenario B — TOLERANCE      (Rule 3: ToleranceRule, 1 paise over)
RRN_TOLE="RRN_TOLE_$TS"   ; UTR_TOLE="UTR_TOLE_$TS"     ; AMOUNT_TOLE=10000

# Scenario C — EXACT via UTR  (Rule 2: UtrAmountRule — pg_txn has different RRN)
#              Settlement line will have RRN_NOPG (no hit in ExactRrnRule)
#              but UTR_UTR matches the pg_txn → UtrAmountRule fires
RRN_UTR_PG="RRN_UTRPG_$TS" ; UTR_UTR="UTR_UTR_$TS"      ; AMOUNT_UTR=35000

# Scenario D — AMOUNT_MISMATCH (Rule 5: AmountMismatchRule → routed to LLM)
#              PG shows 80000, settlement shows 80200 (200p gap, > 1p tolerance)
RRN_MISM="RRN_MISM_$TS"   ; UTR_MISM="UTR_MISM_$TS"     ; AMOUNT_MISM_PG=80000 ; AMOUNT_MISM_SETTLE=80200

# Scenario E — MISSING_LEG   (Rule 6: MissingLegRule → routed to LLM)
#              No pg_txn for this RRN or UTR
RRN_MISS="RRN_MISS_$TS"

db "
INSERT INTO pg_transactions (txn_id, rrn, utr, amount_paise, status, created_at, updated_at)
VALUES
  ('TXN_EXACT_$TS', '$RRN_EXACT',  '$UTR_EXACT', $AMOUNT_EXACT,    'SUCCESS', now(), now()),
  ('TXN_TOLE_$TS',  '$RRN_TOLE',   '$UTR_TOLE',  $AMOUNT_TOLE,     'SUCCESS', now(), now()),
  ('TXN_UTR_$TS',   '$RRN_UTR_PG', '$UTR_UTR',   $AMOUNT_UTR,      'SUCCESS', now(), now()),
  ('TXN_MISM_$TS',  '$RRN_MISM',   '$UTR_MISM',  $AMOUNT_MISM_PG,  'SUCCESS', now(), now())
ON CONFLICT DO NOTHING;
" > /dev/null
ok "seeded 4 pg_transactions"
echo
rule_tag "Scenario A → TXN_EXACT_$TS   RRN=$RRN_EXACT   amt=${AMOUNT_EXACT}p  (will match ExactRrnRule)"
rule_tag "Scenario B → TXN_TOLE_$TS    RRN=$RRN_TOLE    amt=${AMOUNT_TOLE}p   (ToleranceRule, 1p tolerance)"
rule_tag "Scenario C → TXN_UTR_$TS     UTR=$UTR_UTR     amt=${AMOUNT_UTR}p    (UtrAmountRule, no RRN in settlement)"
llm_tag  "Scenario D → TXN_MISM_$TS    RRN=$RRN_MISM    pg=${AMOUNT_MISM_PG}p settle=${AMOUNT_MISM_SETTLE}p  (AmountMismatchRule → LLM)"
llm_tag  "Scenario E → (no pg_txn)     RRN=$RRN_MISS                          (MissingLegRule → LLM)"

# ── 3. Generate UDIR settlement file ─────────────────────────────────────────
sep "3. Generating UDIR settlement file (5 lines)"

TMPDIR_LOCAL=$(mktemp -d)
FILE="$TMPDIR_LOCAL/udir_test_$TS.txt"

# fee/net helpers
fee()  { echo $(( $1 / 100 )); }
net()  { echo $(( $1 - $1 / 100 )); }

AMOUNT_TOLE_SETTLE=$(( AMOUNT_TOLE + 1 ))        # 1p over → TOLERANCE

cat > "$FILE" <<EOF
HDR|UDIR_$TS|$(date +%Y%m%d)|5
$RRN_EXACT|$UTR_EXACT|$AMOUNT_EXACT|$(fee $AMOUNT_EXACT)|$(net $AMOUNT_EXACT)|SUCCESS
$RRN_TOLE|$UTR_TOLE|$AMOUNT_TOLE_SETTLE|$(fee $AMOUNT_TOLE)|$(net $AMOUNT_TOLE)|SUCCESS
RRN_NOPG_$TS|$UTR_UTR|$AMOUNT_UTR|$(fee $AMOUNT_UTR)|$(net $AMOUNT_UTR)|SUCCESS
$RRN_MISM|$UTR_MISM|$AMOUNT_MISM_SETTLE|$(fee $AMOUNT_MISM_SETTLE)|$(net $AMOUNT_MISM_SETTLE)|SUCCESS
$RRN_MISS|UTR_MISS_$TS|25000|250|24750|SUCCESS
TRL|5
EOF

FILENAME=$(basename "$FILE")
ok "generated $FILENAME (5 data lines)"

# ── 4. Drop file to SFTP ──────────────────────────────────────────────────────
sep "4. Dropping file into SFTP upload directory"

UPLOAD_DIR="/config/home/recon/upload"
docker compose cp "$FILE" "sftp:$UPLOAD_DIR/$FILENAME"
ok "copied $FILENAME → sftp:$UPLOAD_DIR"
rm -rf "$TMPDIR_LOCAL"
TEST_START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# ── 5. Wait: settlement_lines written ────────────────────────────────────────
sep "5. Waiting for SftpWatcher to ingest file (fixedDelay=5s)"

ELAPSED=0
FOUND_LINES=0
ALL_RRNS="'$RRN_EXACT','$RRN_TOLE','RRN_NOPG_$TS','$RRN_MISM','$RRN_MISS'"
while [[ $ELAPSED -lt $MAX_WAIT ]]; do
  sleep $POLL_INTERVAL
  ELAPSED=$((ELAPSED + POLL_INTERVAL))
  FOUND_LINES=$(db "SELECT COUNT(*) FROM settlement_lines WHERE rrn IN ($ALL_RRNS)")
  echo "  [${ELAPSED}s] settlement_lines found: $FOUND_LINES/5"
  [[ "$FOUND_LINES" -eq 5 ]] && break
done

[[ "$FOUND_LINES" -eq 5 ]] && ok "all 5 settlement lines ingested" \
  || { fail "timeout: only $FOUND_LINES/5 lines ingested after ${MAX_WAIT}s"; exit 1; }

# ── 6. Wait: outbox drained ───────────────────────────────────────────────────
sep "6. Waiting for OutboxDrainer to dispatch events (fixedDelay=500ms)"

ELAPSED=0
while [[ $ELAPSED -lt 30 ]]; do
  sleep 2; ELAPSED=$((ELAPSED + 2))
  PENDING=$(db "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL AND partition_key IN ($ALL_RRNS)")
  echo "  [${ELAPSED}s] unpublished outbox rows: $PENDING"
  [[ "$PENDING" -eq 0 ]] && break
done
[[ "$PENDING" -eq 0 ]] && ok "outbox drained" || fail "outbox still has $PENDING unpublished rows"

# ── 7. Wait: recon_cases created ─────────────────────────────────────────────
sep "7. Waiting for ReconRequestListener to create cases"

ELAPSED=0
FOUND_CASES=0
while [[ $ELAPSED -lt 30 ]]; do
  sleep 2; ELAPSED=$((ELAPSED + 2))
  FOUND_CASES=$(db "SELECT COUNT(*) FROM recon_cases c JOIN settlement_lines s ON s.id = c.settlement_line WHERE s.rrn IN ($ALL_RRNS)")
  echo "  [${ELAPSED}s] recon_cases created: $FOUND_CASES/5"
  [[ "$FOUND_CASES" -eq 5 ]] && break
done
[[ "$FOUND_CASES" -eq 5 ]] && ok "all 5 cases created" \
  || { fail "timeout: only $FOUND_CASES/5 cases created after ${MAX_WAIT}s"; exit 1; }

# ── 8. Rule engine assertions ─────────────────────────────────────────────────
sep "8. Rule engine assertions (cases auto-resolved without LLM)"

check_case() {
  local rrn="$1" expected_match="$2" expected_res="$3" label="$4"
  local row
  row=$(db "
    SELECT c.match_type, c.resolution, c.confidence
    FROM   recon_cases c
    JOIN   settlement_lines s ON s.id = c.settlement_line
    WHERE  s.rrn = '$rrn'
    LIMIT  1
  ")
  local match res conf
  match=$(echo "$row" | cut -d'|' -f1 | tr -d ' ')
  res=$(echo "$row"   | cut -d'|' -f2 | tr -d ' ')
  conf=$(echo "$row"  | cut -d'|' -f3 | tr -d ' ')

  if [[ "$match" == "$expected_match" && "$res" == "$expected_res" ]]; then
    rule_tag "OK  $label → match=$match  resolution=$res  conf=$conf"
    ok "$rrn"
  else
    fail "$label ($rrn) → expected match=$expected_match res=$expected_res, got match=$match res=$res"
  fi
}

echo
echo "  Rule 1 — ExactRrnRule  : RRN + exact amount + SUCCESS"
check_case "$RRN_EXACT"       "EXACT"       "AUTO_RESOLVED" "Scenario A"

echo
echo "  Rule 3 — ToleranceRule : RRN + amount within ${RULES_TOLERANCE_PAISE:-1}p tolerance"
check_case "$RRN_TOLE"        "TOLERANCE"   "AUTO_RESOLVED" "Scenario B"

echo
echo "  Rule 2 — UtrAmountRule : no RRN in settlement, UTR+amount match same calendar day"
check_case "RRN_NOPG_$TS"    "EXACT"       "AUTO_RESOLVED" "Scenario C"

# ── 9. Cases routed to LLM agent ─────────────────────────────────────────────
sep "9. Cases routed to LLM agent (resolution=PENDING before investigation)"

check_pending() {
  local rrn="$1" expected_match="$2" label="$3"
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

  if [[ "$match" == "$expected_match" ]]; then
    llm_tag "OK  $label → match=$match  initial_resolution=$res  (queued for agent)"
    ok "$rrn"
  else
    fail "$label ($rrn) → expected match=$expected_match, got $match"
  fi
}

echo
echo "  Rule 5 — AmountMismatchRule : RRN found but 200p amount gap (>${RULES_TOLERANCE_PAISE:-1}p tolerance)"
check_pending "$RRN_MISM" "AMOUNT_MISMATCH" "Scenario D"

echo
echo "  Rule 6 — MissingLegRule     : no pg_txn for this RRN or UTR"
check_pending "$RRN_MISS" "MISSING_LEG"     "Scenario E"

# ── 10. Wait: LLM agent completes investigation ───────────────────────────────
sep "10. Waiting for LLM agent to investigate PENDING cases (MOCK_LLM=${MOCK_LLM:-?})"

LLM_RRN_LIST="'$RRN_MISM','$RRN_MISS'"
ELAPSED=0
LLM_RESOLVED=0
LLM_WAIT=120   # agent timeout is 30s; allow headroom for both cases

echo "  Agent will call: search_pg_transactions → get_settlement_history / get_chargeback_status"
echo "                   → compute_fee_breakdown → propose_resolution (terminal)"
echo

while [[ $ELAPSED -lt $LLM_WAIT ]]; do
  sleep 3; ELAPSED=$((ELAPSED + 3))
  LLM_RESOLVED=$(db "
    SELECT COUNT(*)
    FROM   recon_cases c
    JOIN   settlement_lines s ON s.id = c.settlement_line
    WHERE  s.rrn IN ($LLM_RRN_LIST)
    AND    c.resolution != 'PENDING'
  ")
  echo "  [${ELAPSED}s] agent-resolved cases: $LLM_RESOLVED/2"
  [[ "$LLM_RESOLVED" -eq 2 ]] && break
done

[[ "$LLM_RESOLVED" -eq 2 ]] && ok "both LLM cases resolved within ${ELAPSED}s" \
  || fail "timeout: only $LLM_RESOLVED/2 LLM cases resolved after ${LLM_WAIT}s"

# ── 11. LLM agent investigation details ──────────────────────────────────────
sep "11. LLM agent investigation trace"

show_llm_trace() {
  local rrn="$1" label="$2" scenario="$3"
  echo
  llm_tag "━━━  $label ($scenario)  ━━━"

  # Case summary from recon_cases
  local row
  row=$(db "
    SELECT c.case_uid, c.match_type, c.resolution, c.confidence,
           c.notes, c.resolved_by, c.resolved_at
    FROM   recon_cases c
    JOIN   settlement_lines s ON s.id = c.settlement_line
    WHERE  s.rrn = '$rrn'
    LIMIT  1
  ")
  local case_uid match res conf notes resolved_by resolved_at
  case_uid=$(echo "$row"    | cut -d'|' -f1 | tr -d ' ')
  match=$(echo "$row"       | cut -d'|' -f2 | tr -d ' ')
  res=$(echo "$row"         | cut -d'|' -f3 | tr -d ' ')
  conf=$(echo "$row"        | cut -d'|' -f4 | tr -d ' ')
  notes=$(echo "$row"       | cut -d'|' -f5)
  resolved_by=$(echo "$row" | cut -d'|' -f6 | tr -d ' ')
  resolved_at=$(echo "$row" | cut -d'|' -f7 | tr -d ' ')

  llm_tag "  case_uid    : $case_uid"
  llm_tag "  rule_result : $match  (confidence before agent: rule-assigned)"
  llm_tag "  resolution  : $res"
  llm_tag "  confidence  : $conf"
  llm_tag "  resolved_by : $resolved_by"
  llm_tag "  resolved_at : $resolved_at"

  # Rationale from notes JSON
  if [[ -n "$notes" && "$notes" != "" ]]; then
    local rationale
    rationale=$(echo "$notes" | python3 -c "
import sys, json
try:
    d = json.loads(sys.stdin.read())
    print(d.get('rationale', d.get('escalation_reason', '(no rationale)')))
except:
    print('(unparseable notes)')
" 2>/dev/null || echo "(notes parse error)")
    llm_tag "  rationale   : $rationale"
  fi

  # Agent trace (model + timing)
  local trace
  trace=$(db "
    SELECT model, total_ms, cost_usd, tools_used
    FROM   agent_traces
    WHERE  case_uid = '$case_uid'
    LIMIT  1
  " 2>/dev/null || true)

  if [[ -n "$trace" ]]; then
    local model total_ms cost tools
    model=$(echo "$trace"    | cut -d'|' -f1 | tr -d ' ')
    total_ms=$(echo "$trace" | cut -d'|' -f2 | tr -d ' ')
    cost=$(echo "$trace"     | cut -d'|' -f3 | tr -d ' ')
    tools=$(echo "$trace"    | cut -d'|' -f4)
    llm_tag "  llm_model   : $model"
    llm_tag "  llm_time_ms : $total_ms"
    llm_tag "  llm_cost_usd: $cost"
    [[ -n "$tools" && "$tools" != "{}" ]] && llm_tag "  tools_used  : $tools"
  fi

  # Key log lines from the app for this case (shows tool call sequence)
  echo
  llm_tag "  ── app log trace for case_uid=$case_uid ──"
  docker compose logs api --no-log-prefix --since=1h 2>/dev/null \
    | grep -F "$case_uid" \
    | grep -E "ROUTED_TO_AGENT|investigate|tool_use|Resolution proposed|Agent timeout|Agent failure" \
    | sed 's/^/    /' \
    || llm_tag "  (no matching log lines — check docker compose logs api)"

  # Outcome verdict
  echo
  if [[ "$res" == "PENDING" || -z "$res" ]]; then
    fail "$label still PENDING — agent did not call propose_resolution"
  elif [[ "$res" == "PROPOSED" ]]; then
    ok "$label resolved by LLM → PROPOSED (awaiting reviewer approval)"
  elif [[ "$res" == "ESCALATE" ]]; then
    ok "$label escalated by LLM → ESCALATE (needs human review)"
  else
    ok "$label resolved by LLM → $res"
  fi
}

show_llm_trace "$RRN_MISM" "Scenario D" "AMOUNT_MISMATCH — PG ₹$(( AMOUNT_MISM_PG / 100 )) vs settlement ₹$(( AMOUNT_MISM_SETTLE / 100 ))"
show_llm_trace "$RRN_MISS" "Scenario E" "MISSING_LEG — no PG transaction found"

# ── 12. REST API: health ──────────────────────────────────────────────────────
sep "12. REST: GET /healthz"

HEALTH=$(curl -sf "$BASE_URL/healthz")
DB_OK=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('db','?'))")
[[ "$DB_OK" == "ok" ]] && ok "db=ok" || fail "db status: $DB_OK"

# ── 13. REST API: 401 without token ──────────────────────────────────────────
sep "13. REST: 401 without token"
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/api/v1/cases")
[[ "$CODE" == "401" ]] && ok "got 401 as expected" || fail "expected 401, got $CODE"

# ── 14. REST API: list cases ──────────────────────────────────────────────────
sep "14. REST: GET /api/v1/cases"
ALL=$(api GET "/api/v1/cases?limit=50")
TOTAL=$(echo "$ALL" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['cases']))")
[[ "$TOTAL" -ge 5 ]] && ok "listed $TOTAL cases (≥5)" || fail "expected ≥5 cases, got $TOTAL"

# ── 15. REST API: filter AUTO_RESOLVED ───────────────────────────────────────
sep "15. REST: filter by resolution=AUTO_RESOLVED"
AUTO=$(api GET "/api/v1/cases?resolution=AUTO_RESOLVED&limit=50")
AUTO_COUNT=$(echo "$AUTO" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['cases']))")
[[ "$AUTO_COUNT" -ge 3 ]] && ok "$AUTO_COUNT AUTO_RESOLVED cases (≥3 from rule engine)" \
  || fail "expected ≥3, got $AUTO_COUNT"

# ── 16. REST API: get single case ─────────────────────────────────────────────
sep "16. REST: GET /api/v1/cases/:caseUid"
PENDING_CASE_UID=$(api GET "/api/v1/cases?resolution=PENDING&limit=1" \
  | python3 -c "import sys,json; cases=json.load(sys.stdin)['cases']; print(cases[0]['caseUid'] if cases else '')")

if [[ -n "$PENDING_CASE_UID" ]]; then
  DETAIL=$(api GET "/api/v1/cases/$PENDING_CASE_UID")
  GOT_UID=$(echo "$DETAIL" | python3 -c "import sys,json; print(json.load(sys.stdin)['case']['caseUid'])")
  [[ "$GOT_UID" == "$PENDING_CASE_UID" ]] && ok "fetched case $PENDING_CASE_UID" \
    || fail "caseUid mismatch: expected $PENDING_CASE_UID got $GOT_UID"
else
  ok "no PENDING cases to fetch (all resolved by agent)"
fi

# ── 17. REST API: 404 for unknown case ───────────────────────────────────────
sep "17. REST: 404 for unknown caseUid"
CODE=$(api_code GET "/api/v1/cases/00000000-0000-0000-0000-000000000000")
[[ "$CODE" == "404" ]] && ok "got 404 as expected" || fail "expected 404, got $CODE"

# ── 18. REST API: approve a case ─────────────────────────────────────────────
sep "18. REST: approve a PROPOSED/PENDING case"
APPROVABLE_UID=$(api GET "/api/v1/cases?resolution=PROPOSED&limit=1" \
  | python3 -c "import sys,json; cases=json.load(sys.stdin)['cases']; print(cases[0]['caseUid'] if cases else '')")
[[ -z "$APPROVABLE_UID" ]] && APPROVABLE_UID="$PENDING_CASE_UID"

if [[ -n "$APPROVABLE_UID" ]]; then
  APPROVE=$(api POST "/api/v1/cases/$APPROVABLE_UID/approve" \
    '{"reviewer_email":"tester@recon.local","comment":"verified correct"}')
  STATUS=$(echo "$APPROVE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))")
  [[ "$STATUS" == "APPROVED" ]] && ok "case $APPROVABLE_UID approved" \
    || fail "expected APPROVED, got $STATUS"

  sep "18b. Stale write guard (expect 409)"
  CODE=$(api_code POST "/api/v1/cases/$APPROVABLE_UID/approve" \
    '{"reviewer_email":"tester@recon.local","comment":"duplicate"}')
  [[ "$CODE" == "409" ]] && ok "got 409 on stale approve" || fail "expected 409, got $CODE"
else
  ok "skipped (no PROPOSED/PENDING cases to approve)"
fi

# ── 19. Outbox idempotency ────────────────────────────────────────────────────
sep "19. Outbox idempotency: exactly 1 outbox row per settlement line"

BAD=$(db "
  SELECT COUNT(*)
  FROM (
    SELECT partition_key
    FROM   outbox
    WHERE  partition_key IN ($ALL_RRNS)
    GROUP  BY partition_key
    HAVING COUNT(*) > 1
  ) t
")
[[ "$BAD" -eq 0 ]] && ok "no duplicate outbox rows" \
  || fail "$BAD partition_keys have >1 outbox row"

# ── Summary ───────────────────────────────────────────────────────────────────
sep "Summary"

echo
echo "  ┌─────────────────────────────────────────────────────────────────────┐"
echo "  │  SCENARIO        RULE / PATH           MATCH_TYPE    RESOLUTION     │"
echo "  ├─────────────────────────────────────────────────────────────────────┤"

print_summary_row() {
  local rrn="$1" scenario="$2" path="$3"
  local row
  row=$(db "
    SELECT c.match_type, c.resolution
    FROM   recon_cases c JOIN settlement_lines s ON s.id = c.settlement_line
    WHERE  s.rrn = '$rrn' LIMIT 1
  " 2>/dev/null || echo "|")
  local mt res
  mt=$(echo "$row" | cut -d'|' -f1 | tr -d ' ')
  res=$(echo "$row" | cut -d'|' -f2 | tr -d ' ')
  printf "  │  %-16s  %-22s  %-12s  %-15s │\n" "$scenario" "$path" "${mt:-(none)}" "${res:-(none)}"
}

print_summary_row "$RRN_EXACT"    "Scenario A" "Rule 1: ExactRrnRule"
print_summary_row "$RRN_TOLE"     "Scenario B" "Rule 3: ToleranceRule"
print_summary_row "RRN_NOPG_$TS" "Scenario C" "Rule 2: UtrAmountRule"
print_summary_row "$RRN_MISM"     "Scenario D" "Rule 5 → LLM agent"
print_summary_row "$RRN_MISS"     "Scenario E" "Rule 6 → LLM agent"
echo "  └─────────────────────────────────────────────────────────────────────┘"

echo
echo "  DB breakdown (last 5 min):"
db "
  SELECT match_type, resolution, COUNT(*) AS n
  FROM   recon_cases
  WHERE  created_at > now() - interval '5 minutes'
  GROUP  BY 1, 2
  ORDER  BY 1
" | while IFS='|' read -r mt res n; do
  printf '    %-20s %-18s %s case(s)\n' "$mt" "$res" "$n"
done

echo
if [[ $FAILURES -eq 0 ]]; then
  printf '\033[32m  All checks passed.\033[0m\n'
else
  printf '\033[31m  %d check(s) failed.\033[0m\n' "$FAILURES"
  exit 1
fi

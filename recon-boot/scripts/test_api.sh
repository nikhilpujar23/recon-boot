#!/usr/bin/env bash
# Manual API smoke-test script.
# Run after: psql ... -f scripts/seed_test_data.sql
#
# Usage:
#   chmod +x scripts/test_api.sh
#   ./scripts/test_api.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE="http://localhost:8080"
TOKEN="${API_BEARER_TOKEN:-changeme}"
HDR=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")

ok()  { echo "  ✓ $*"; }
err() { echo "  ✗ $*"; }
sep() { echo; echo "── $* ──────────────────────────────────"; }

# ── 0. Seed DB ────────────────────────────────────────────────────────────────
sep "0. Seeding test data"
docker compose exec -T postgres psql -U recon recon < "$SCRIPT_DIR/seed_test_data.sql"
ok "seed complete"

# ── 1. Health ─────────────────────────────────────────────────────────────────
sep "1. Health check"
HEALTH=$(curl -sf "$BASE/healthz")
echo "$HEALTH" | python3 -m json.tool
[[ $(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['db'])") == "ok" ]] \
  && ok "db healthy" || err "db unhealthy"

# ── 2. List all cases ─────────────────────────────────────────────────────────
sep "2. List all cases"
ALL=$(curl -sf "${HDR[@]}" "$BASE/api/v1/cases?limit=20")
echo "$ALL" | python3 -m json.tool
COUNT=$(echo "$ALL" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['cases']))")
ok "got $COUNT cases"

# ── 3. Filter by resolution ───────────────────────────────────────────────────
sep "3. Filter: AUTO_RESOLVED cases"
curl -sf "${HDR[@]}" "$BASE/api/v1/cases?resolution=AUTO_RESOLVED" | python3 -m json.tool

sep "3b. Filter: PROPOSED cases (need human review)"
PROPOSED=$(curl -sf "${HDR[@]}" "$BASE/api/v1/cases?resolution=PROPOSED")
echo "$PROPOSED" | python3 -m json.tool

# ── 4. Filter by match type ───────────────────────────────────────────────────
sep "4. Filter: MISSING_LEG cases"
curl -sf "${HDR[@]}" "$BASE/api/v1/cases?matchType=MISSING_LEG" | python3 -m json.tool

# ── 5. Get a single PROPOSED case and approve it ─────────────────────────────
sep "5. Approve a PROPOSED case"
CASE_UID=$(echo "$PROPOSED" | python3 -c "
import sys, json
cases = json.load(sys.stdin)['cases']
if cases:
    print(cases[0]['caseUid'])
")

if [[ -z "$CASE_UID" ]]; then
  echo "  No PROPOSED cases to approve — skipping"
else
  ok "approving caseUid=$CASE_UID"
  curl -sf -X POST "${HDR[@]}" \
    -d '{"reviewer_email":"test@recon.local","comment":"looks correct"}' \
    "$BASE/api/v1/cases/$CASE_UID/approve" | python3 -m json.tool

  sep "5b. Confirm case is now APPROVED"
  curl -sf "${HDR[@]}" "$BASE/api/v1/cases/$CASE_UID" | python3 -m json.tool
fi

# ── 6. Get another PROPOSED case and reject it ────────────────────────────────
sep "6. Reject another PROPOSED case"
CASE_UID2=$(echo "$PROPOSED" | python3 -c "
import sys, json
cases = json.load(sys.stdin)['cases']
if len(cases) > 1:
    print(cases[1]['caseUid'])
elif len(cases) == 1:
    print('')  # only one, already approved
")

if [[ -z "$CASE_UID2" ]]; then
  echo "  No second PROPOSED case — skipping"
else
  ok "rejecting caseUid=$CASE_UID2"
  curl -sf -X POST "${HDR[@]}" \
    -d '{"reviewer_email":"test@recon.local","comment":"amounts do not match"}' \
    "$BASE/api/v1/cases/$CASE_UID2/reject" | python3 -m json.tool
fi

# ── 7. Stale write guard: approve an already-approved case ───────────────────
sep "7. Stale write guard (expect 409)"
if [[ -n "$CASE_UID" ]]; then
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${HDR[@]}" \
    -d '{"reviewer_email":"test@recon.local","comment":"duplicate"}' \
    "$BASE/api/v1/cases/$CASE_UID/approve")
  [[ "$HTTP" == "409" ]] && ok "got 409 as expected" || err "expected 409, got $HTTP"
fi

# ── 8. 404 for unknown case ───────────────────────────────────────────────────
sep "8. 404 for unknown caseUid"
HTTP=$(curl -s -o /dev/null -w "%{http_code}" "${HDR[@]}" \
  "$BASE/api/v1/cases/00000000-0000-0000-0000-000000000000")
[[ "$HTTP" == "404" ]] && ok "got 404 as expected" || err "expected 404, got $HTTP"

# ── 9. 401 without token ──────────────────────────────────────────────────────
sep "9. 401 without auth token"
HTTP=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/api/v1/cases")
[[ "$HTTP" == "401" ]] && ok "got 401 as expected" || err "expected 401, got $HTTP"

echo
echo "Done."

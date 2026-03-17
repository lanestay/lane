#!/usr/bin/env bash
# Test: Email code login (sad paths)
# Requires: BASE_URL, API_KEY, MAILPIT_URL, SMTP configured on server
set -euo pipefail
source "$(dirname "$0")/lib.sh"

: "${API_KEY:?}"

# Check prerequisites
if ! curl -s -o /dev/null -w "%{http_code}" "${MAILPIT_URL}/api/v1/messages" | grep -q "200"; then
  skip "Mailpit not reachable at ${MAILPIT_URL}"
  print_summary
  exit 0
fi

noauth_get "/api/auth/status"
if ! echo "$HTTP_BODY" | grep -q '"smtp_configured":true'; then
  skip "SMTP not configured — skipping email code sad-path tests"
  print_summary
  exit 0
fi

echo "  Email code login (sad paths)"

# Create fresh users so we never hit the per-email hourly rate limit
SAD_USER="emailsad-$(date +%s)@integration.test"
RATE_USER="emailrate-$(date +%s)@integration.test"
api_post "/api/lane/admin/users" "{\"email\": \"${SAD_USER}\", \"is_admin\": false}"
assert_status "Create temp user for sad-path tests" "201" "$HTTP_STATUS"
api_post "/api/lane/admin/users" "{\"email\": \"${RATE_USER}\", \"is_admin\": false}"
assert_status "Create temp user for rate-limit test" "201" "$HTTP_STATUS"

cleanup() {
  api_delete "/api/lane/admin/users/${SAD_USER}" > /dev/null 2>&1 || true
  api_delete "/api/lane/admin/users/${RATE_USER}" > /dev/null 2>&1 || true
}
trap cleanup EXIT

# ── 1. Send code to nonexistent email — should still return 200 (anti-enumeration)
noauth_post "/api/auth/email-code/send" '{"email": "nonexistent-user-xyz@nowhere.invalid"}'
assert_status "Send code to nonexistent email returns 200" "200" "$HTTP_STATUS"
assert_body_contains "Response indicates success (anti-enum)" "success" "$HTTP_BODY"

# ── 2. Send code, then verify with wrong code — should be 401
mailpit_delete_all
noauth_post "/api/auth/email-code/send" "{\"email\": \"${SAD_USER}\"}"
assert_status "Send code for wrong-code test" "200" "$HTTP_STATUS"
sleep 2

noauth_post "/api/auth/email-code/verify" "{\"email\": \"${SAD_USER}\", \"code\": \"000000\"}"
assert_status "Verify with wrong code" "401" "$HTTP_STATUS"

# ── 3. Send code, exhaust attempts (3 wrong), then try correct code
mailpit_delete_all
noauth_post "/api/auth/email-code/send" "{\"email\": \"${SAD_USER}\"}"
sleep 2

# Get the real code from Mailpit
MESSAGES=$(curl -s "${MAILPIT_URL}/api/v1/messages?limit=5")
MSG_ID=$(echo "$MESSAGES" | grep -o '"ID":"[^"]*"' | head -1 | cut -d'"' -f4 || true)

if [ -z "$MSG_ID" ]; then
  fail "Could not get email for attempts-exhausted test"
  print_summary
  exit 1
fi

MSG_SNIPPET=$(echo "$MESSAGES" | grep -o '"Snippet":"[^"]*"' | head -1 | cut -d'"' -f4 || true)
REAL_CODE=$(echo "$MSG_SNIPPET" | grep -oE '[0-9]{6}' | head -1 || true)

if [ -z "$REAL_CODE" ]; then
  fail "Could not extract code from email"
  print_summary
  exit 1
fi
pass "Got code for attempts-exhausted test"

# Burn 3 attempts with wrong codes
for i in 1 2 3; do
  noauth_post "/api/auth/email-code/verify" "{\"email\": \"${SAD_USER}\", \"code\": \"00000${i}\"}"
  assert_status "Wrong attempt $i" "401" "$HTTP_STATUS"
done

# Now try with the real code — should be rejected (attempts exhausted)
noauth_post "/api/auth/email-code/verify" "{\"email\": \"${SAD_USER}\", \"code\": \"${REAL_CODE}\"}"
assert_status "Correct code after 3 wrong attempts" "401" "$HTTP_STATUS"

# ── 4. Rate limit: send 6 codes to a separate user, only 5 should arrive
mailpit_delete_all
for i in $(seq 1 6); do
  noauth_post "/api/auth/email-code/send" "{\"email\": \"${RATE_USER}\"}"
  assert_status "Send code request $i" "200" "$HTTP_STATUS"
done

sleep 3
COUNT=$(mailpit_count "${RATE_USER}")
if [ "${COUNT:-0}" -le 5 ]; then
  pass "Rate limit enforced: ${COUNT} emails delivered (max 5)"
else
  fail "Rate limit NOT enforced: ${COUNT} emails delivered (expected <= 5)"
fi

print_summary

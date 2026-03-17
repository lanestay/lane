#!/usr/bin/env bash
# Test: Email code login (happy path)
# Requires: BASE_URL, API_KEY, MAILPIT_URL, SMTP configured on server
set -euo pipefail
source "$(dirname "$0")/lib.sh"

: "${API_KEY:?}"

# Check if Mailpit is reachable
if ! curl -s -o /dev/null -w "%{http_code}" "${MAILPIT_URL}/api/v1/messages" | grep -q "200"; then
  skip "Mailpit not reachable at ${MAILPIT_URL}"
  print_summary
  exit 0
fi

echo "  Email code login (happy path)"

# 1. Check auth status — smtp should be configured
noauth_get "/api/auth/status"
assert_status "Auth status endpoint" "200" "$HTTP_STATUS"
if echo "$HTTP_BODY" | grep -q '"smtp_configured":true'; then
  pass "SMTP is configured"
else
  skip "SMTP not configured on server — skipping email code tests"
  print_summary
  exit 0
fi

# Create a fresh user so we never hit the per-email hourly rate limit
CODE_USER="emailtest-$(date +%s)@integration.test"
api_post "/api/lane/admin/users" "{\"email\": \"${CODE_USER}\", \"is_admin\": false}"
assert_status "Create temp user for email code test" "201" "$HTTP_STATUS"

cleanup() { api_delete "/api/lane/admin/users/${CODE_USER}" > /dev/null 2>&1 || true; }
trap cleanup EXIT

# 2. Clear Mailpit inbox
mailpit_delete_all
pass "Cleared Mailpit inbox"

# 3. Send code
noauth_post "/api/auth/email-code/send" "{\"email\": \"${CODE_USER}\"}"
assert_status "Send email code" "200" "$HTTP_STATUS"
assert_body_contains "Response indicates success" "success" "$HTTP_BODY"

# 4. Wait for email delivery, then fetch from Mailpit
sleep 2
MESSAGES=$(curl -s "${MAILPIT_URL}/api/v1/messages?limit=5")
MSG_ID=$(echo "$MESSAGES" | grep -o '"ID":"[^"]*"' | head -1 | cut -d'"' -f4 || true)

if [ -z "$MSG_ID" ]; then
  fail "No email found in Mailpit for ${CODE_USER}"
  print_summary
  exit 1
fi

# Get the message snippet (contains the code as plaintext)
MSG_SNIPPET=$(echo "$MESSAGES" | grep -o '"Snippet":"[^"]*"' | head -1 | cut -d'"' -f4 || true)
CODE=$(echo "$MSG_SNIPPET" | grep -oE '[0-9]{6}' | head -1 || true)

if [ -n "$CODE" ]; then
  pass "Extracted 6-digit code from email"
else
  fail "Could not extract 6-digit code from email"
  print_summary
  exit 1
fi

# 5. Verify with correct code
noauth_post "/api/auth/email-code/verify" "{\"email\": \"${CODE_USER}\", \"code\": \"${CODE}\"}"
assert_status "Verify with correct code" "200" "$HTTP_STATUS"
assert_body_contains "Response has session_token" "session_token" "$HTTP_BODY"

# Extract session token for next step
SESSION_TOKEN=$(echo "$HTTP_BODY" | grep -o '"session_token":"[^"]*"' | cut -d'"' -f4 || true)

# 6. Use session token to check auth status
if [ -n "$SESSION_TOKEN" ]; then
  session_get "/api/auth/status" "$SESSION_TOKEN"
  assert_status "Auth status with session token" "200" "$HTTP_STATUS"
  assert_body_contains "Authenticated as user" "authenticated" "$HTTP_BODY"
else
  fail "No session token to test auth status"
fi

print_summary

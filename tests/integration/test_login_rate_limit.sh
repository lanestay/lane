#!/usr/bin/env bash
# Test: Login rate limiting
# Requires: BASE_URL, TEST_EMAIL, TEST_PASSWORD
set -euo pipefail
source "$(dirname "$0")/lib.sh"

: "${TEST_EMAIL:?}" "${TEST_PASSWORD:?}"

echo "  Login rate limiting"

# 1. Login with correct credentials
noauth_post "/api/auth/login" "{\"email\": \"${TEST_EMAIL}\", \"password\": \"${TEST_PASSWORD}\"}"
assert_status "Login with correct credentials" "200" "$HTTP_STATUS"
assert_body_contains "Response has session_token" "session_token" "$HTTP_BODY"

# 2. Login 5x with wrong password — each should be 401
for i in $(seq 1 5); do
  noauth_post "/api/auth/login" "{\"email\": \"${TEST_EMAIL}\", \"password\": \"wrong-password-${i}\"}"
  assert_status "Wrong password attempt $i" "401" "$HTTP_STATUS"
done

# 3. 6th wrong password — should be 429 (rate limited)
noauth_post "/api/auth/login" "{\"email\": \"${TEST_EMAIL}\", \"password\": \"wrong-password-6\"}"
assert_status "6th wrong attempt is rate limited" "429" "$HTTP_STATUS"

# 4. Correct password while rate limited — still 429
noauth_post "/api/auth/login" "{\"email\": \"${TEST_EMAIL}\", \"password\": \"${TEST_PASSWORD}\"}"
assert_status "Correct password while rate limited" "429" "$HTTP_STATUS"

print_summary

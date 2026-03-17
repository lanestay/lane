#!/usr/bin/env bash
# Test: Permission enforcement (sql_mode + fail-closed permissions)
# Requires: BASE_URL, API_KEY
set -euo pipefail
source "$(dirname "$0")/lib.sh"

: "${API_KEY:?}"

echo "  Permission enforcement"

# Generate a unique test email
TEST_USER="inttest-perm-$(date +%s)@integration.test"

cleanup() {
  api_delete "/api/lane/admin/users/${TEST_USER}" > /dev/null 2>&1 || true
}
trap cleanup EXIT

# ── 1. Create user with sql_mode=none (default), generate token ───────────────
api_post "/api/lane/admin/users" "{\"email\": \"${TEST_USER}\", \"is_admin\": false}"
if [ "$HTTP_STATUS" = "503" ]; then
  skip "Access control not enabled (503) — skipping permission tests"
  print_summary
  exit 0
fi
assert_status "Create test user" "201" "$HTTP_STATUS"

api_post "/api/lane/admin/tokens/generate" "{\"email\": \"${TEST_USER}\", \"label\": \"test\"}"
assert_status "Generate token" "200" "$HTTP_STATUS"
USER_TOKEN=$(echo "$HTTP_BODY" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)

if [ -z "$USER_TOKEN" ]; then
  fail "Could not extract token from response"
  print_summary
  exit 1
fi
pass "Token generated for ${TEST_USER}"

# ── 2. Query with sql_mode=none — should be 403 ──────────────────────────────
query_with_token "$USER_TOKEN" "master" "SELECT 1"
assert_status "Query with sql_mode=none" "403" "$HTTP_STATUS"

# ── 3. Update user to sql_mode=read_only ──────────────────────────────────────
api_put "/api/lane/admin/users/${TEST_USER}" '{"sql_mode": "read_only"}'
assert_status "Update to read_only" "200" "$HTTP_STATUS"

# Grant read permission so we can test sql_mode independently
api_post "/api/lane/admin/permissions" \
  "{\"email\": \"${TEST_USER}\", \"permissions\": [{\"database_name\": \"*\", \"can_read\": true}]}"
assert_status "Grant read permission" "200" "$HTTP_STATUS"

# ── 4. SELECT with read_only — should succeed ────────────────────────────────
query_with_token "$USER_TOKEN" "master" "SELECT 1 AS test"
assert_status "SELECT with read_only" "200" "$HTTP_STATUS"

# ── 5. INSERT with read_only — should be 403 ─────────────────────────────────
query_with_token "$USER_TOKEN" "master" "INSERT INTO test_table VALUES (1)"
assert_status "INSERT with read_only" "403" "$HTTP_STATUS"

# ── 6. Update to sql_mode=full, remove all permissions ────────────────────────
api_put "/api/lane/admin/users/${TEST_USER}" '{"sql_mode": "full"}'
assert_status "Update to sql_mode=full" "200" "$HTTP_STATUS"

# Clear permissions (set empty array)
api_post "/api/lane/admin/permissions" \
  "{\"email\": \"${TEST_USER}\", \"permissions\": []}"
assert_status "Clear all permissions" "200" "$HTTP_STATUS"

# ── 7. SELECT with no permission rows — should be 403 (fail-closed) ──────────
query_with_token "$USER_TOKEN" "master" "SELECT 1 AS test"
assert_status "SELECT with no permissions (fail-closed)" "403" "$HTTP_STATUS"

# ── 8. Add permission: database=*, can_read=true ──────────────────────────────
api_post "/api/lane/admin/permissions" \
  "{\"email\": \"${TEST_USER}\", \"permissions\": [{\"database_name\": \"*\", \"can_read\": true}]}"
assert_status "Add wildcard read permission" "200" "$HTTP_STATUS"

# ── 9. SELECT with can_read — should succeed ─────────────────────────────────
query_with_token "$USER_TOKEN" "master" "SELECT 1 AS test"
assert_status "SELECT with can_read permission" "200" "$HTTP_STATUS"

# ── 10. INSERT without can_write — should be 403 ─────────────────────────────
query_with_token "$USER_TOKEN" "master" "INSERT INTO test_table VALUES (1)"
assert_status "INSERT without can_write" "403" "$HTTP_STATUS"

# ── 11. Cleanup (handled by trap) ────────────────────────────────────────────
pass "Cleanup via trap"

print_summary

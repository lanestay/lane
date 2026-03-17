#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load .env if present
if [ -f "${SCRIPT_DIR}/../../.env" ]; then
  set -a
  source "${SCRIPT_DIR}/../../.env"
  set +a
fi

# Required env vars
: "${BASE_URL:?Set BASE_URL (e.g. http://localhost:3401)}"
: "${API_KEY:?Set API_KEY to the system admin API key}"
: "${TEST_EMAIL:?Set TEST_EMAIL to an existing user email}"
: "${TEST_PASSWORD:?Set TEST_PASSWORD to the password for TEST_EMAIL}"

MAILPIT_URL="${MAILPIT_URL:-http://localhost:8025}"

echo "╔══════════════════════════════════════╗"
echo "║   Lane Integration Tests      ║"
echo "╠══════════════════════════════════════╣"
echo "║  Server:  ${BASE_URL}"
echo "║  Mailpit: ${MAILPIT_URL}"
echo "╚══════════════════════════════════════╝"
echo ""

TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_SKIP=0
SUITE_FAILED=0

# Explicit ordering: email happy path before sad path, rate limit last
# (rate limit test poisons the IP for 15 min)
TEST_ORDER=(
  test_permissions.sh
  test_email_code.sh
  test_email_code_sad.sh
  test_login_rate_limit.sh
)

for test_name in "${TEST_ORDER[@]}"; do
  test_file="${SCRIPT_DIR}/${test_name}"
  [ -f "$test_file" ] || continue
  echo "━━━ ${test_name%.sh} ━━━"

  # Run each test file in a subshell so counters don't leak
  set +e
  output=$(bash "$test_file" 2>&1)
  exit_code=$?
  set -e

  echo "$output"

  # Extract counters from output (last lines)
  passes=$(echo "$output" | grep -c "PASS" || true)
  fails=$(echo "$output" | grep -c "FAIL" || true)
  skips=$(echo "$output" | grep -c "SKIP" || true)

  TOTAL_PASS=$((TOTAL_PASS + passes))
  TOTAL_FAIL=$((TOTAL_FAIL + fails))
  TOTAL_SKIP=$((TOTAL_SKIP + skips))

  if [ "$exit_code" -ne 0 ]; then
    SUITE_FAILED=1
  fi

  echo ""
done

echo "════════════════════════════════════════"
echo "  TOTAL: ${TOTAL_PASS} passed, ${TOTAL_FAIL} failed, ${TOTAL_SKIP} skipped"
echo "════════════════════════════════════════"

exit "$SUITE_FAILED"

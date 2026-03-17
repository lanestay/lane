#!/usr/bin/env bash
# Shared helpers for integration tests.
# Source this file from each test script: source "$(dirname "$0")/lib.sh"

set -euo pipefail

# ── Env defaults ──────────────────────────────────────────────────────────────
BASE_URL="${BASE_URL:-http://localhost:3401}"
MAILPIT_URL="${MAILPIT_URL:-http://localhost:8025}"

# ── Counters ──────────────────────────────────────────────────────────────────
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

# ── Colors ────────────────────────────────────────────────────────────────────
_green='\033[0;32m'
_red='\033[0;31m'
_yellow='\033[0;33m'
_reset='\033[0m'

pass() { PASS_COUNT=$((PASS_COUNT + 1)); printf "${_green}  PASS${_reset}  %s\n" "$1"; }
fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); printf "${_red}  FAIL${_reset}  %s\n" "$1"; }
skip() { SKIP_COUNT=$((SKIP_COUNT + 1)); printf "${_yellow}  SKIP${_reset}  %s\n" "$1"; }

# ── Assertions ────────────────────────────────────────────────────────────────

assert_status() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    pass "$desc (HTTP $actual)"
  else
    fail "$desc (expected $expected, got $actual)"
  fi
}

assert_body_contains() {
  local desc="$1" expected="$2" body="$3"
  if echo "$body" | grep -q "$expected"; then
    pass "$desc"
  else
    # Redact tokens/keys before printing body on failure
    local safe_body
    safe_body=$(echo "$body" | sed -E 's/("(session_token|token|api_key)":")[^"]+/\1[REDACTED]/g')
    fail "$desc — body missing '$expected'"
    printf "       body: %.200s\n" "$safe_body"
  fi
}

assert_body_not_contains() {
  local desc="$1" unexpected="$2" body="$3"
  if echo "$body" | grep -q "$unexpected"; then
    local safe_body
    safe_body=$(echo "$body" | sed -E 's/("(session_token|token|api_key)":")[^"]+/\1[REDACTED]/g')
    fail "$desc — body contains '$unexpected'"
    printf "       body: %.200s\n" "$safe_body"
  else
    pass "$desc"
  fi
}

# ── HTTP helpers (API key auth) ───────────────────────────────────────────────

# Returns: sets $HTTP_STATUS and $HTTP_BODY
api_get() {
  local path="$1"
  local tmp
  tmp=$(mktemp)
  HTTP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" \
    "${BASE_URL}${path}" \
    -H "x-api-key: ${API_KEY}")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

api_post() {
  local path="$1" body="$2"
  local tmp
  tmp=$(mktemp)
  HTTP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" -X POST \
    "${BASE_URL}${path}" \
    -H "x-api-key: ${API_KEY}" \
    -H "Content-Type: application/json" \
    -d "$body")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

api_put() {
  local path="$1" body="$2"
  local tmp
  tmp=$(mktemp)
  HTTP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" -X PUT \
    "${BASE_URL}${path}" \
    -H "x-api-key: ${API_KEY}" \
    -H "Content-Type: application/json" \
    -d "$body")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

api_delete() {
  local path="$1"
  local tmp
  tmp=$(mktemp)
  HTTP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" -X DELETE \
    "${BASE_URL}${path}" \
    -H "x-api-key: ${API_KEY}")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

# ── HTTP helpers (no auth — for login/public endpoints) ───────────────────────

noauth_post() {
  local path="$1" body="$2"
  local tmp
  tmp=$(mktemp)
  HTTP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" -X POST \
    "${BASE_URL}${path}" \
    -H "Content-Type: application/json" \
    -d "$body")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

noauth_get() {
  local path="$1"
  local tmp
  tmp=$(mktemp)
  HTTP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" \
    "${BASE_URL}${path}")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

# ── HTTP helpers (session token auth) ─────────────────────────────────────────

session_get() {
  local path="$1" token="$2"
  local tmp
  tmp=$(mktemp)
  HTTP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" \
    "${BASE_URL}${path}" \
    -H "Authorization: Bearer ${token}")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

session_post() {
  local path="$1" body="$2" token="$3"
  local tmp
  tmp=$(mktemp)
  HTTP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" -X POST \
    "${BASE_URL}${path}" \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" \
    -d "$body")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

# ── Token-based query helper ──────────────────────────────────────────────────

query_with_token() {
  local token="$1" database="$2" query="$3"
  local tmp
  tmp=$(mktemp)
  HTTP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" -X POST \
    "${BASE_URL}/api/lane" \
    -H "x-api-key: ${token}" \
    -H "Content-Type: application/json" \
    -d "{\"database\": \"$database\", \"query\": \"$query\"}")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

# ── Mailpit helpers ───────────────────────────────────────────────────────────

mailpit_delete_all() {
  curl -s -X DELETE "${MAILPIT_URL}/api/v1/messages" > /dev/null
}

# Fetch latest messages and extract the most recent one for a given email.
# Sets MAILPIT_BODY to the text body of the latest matching message.
mailpit_get_latest() {
  local email="$1"
  local messages
  messages=$(curl -s "${MAILPIT_URL}/api/v1/search?query=to:${email}&limit=1")
  local msg_id
  msg_id=$(echo "$messages" | grep -o '"ID":"[^"]*"' | head -1 | cut -d'"' -f4 || true)
  if [ -z "$msg_id" ]; then
    MAILPIT_BODY=""
    return 1
  fi
  MAILPIT_BODY=$(curl -s "${MAILPIT_URL}/api/v1/message/${msg_id}" | grep -o '"Text":"[^"]*"' | head -1 | cut -d'"' -f4 || true)
}

# Count messages for a given email address
mailpit_count() {
  local email="$1"
  local result
  result=$(curl -s "${MAILPIT_URL}/api/v1/search?query=to:${email}")
  echo "$result" | grep -o '"messages_count":[0-9]*' | cut -d: -f2 || echo "0"
}

# ── Summary ───────────────────────────────────────────────────────────────────

print_summary() {
  echo ""
  echo "─────────────────────────────────"
  printf "  Results: ${_green}%d passed${_reset}" "$PASS_COUNT"
  if [ "$FAIL_COUNT" -gt 0 ]; then
    printf ", ${_red}%d failed${_reset}" "$FAIL_COUNT"
  fi
  if [ "$SKIP_COUNT" -gt 0 ]; then
    printf ", ${_yellow}%d skipped${_reset}" "$SKIP_COUNT"
  fi
  echo ""
  echo "─────────────────────────────────"
  [ "$FAIL_COUNT" -eq 0 ]
}

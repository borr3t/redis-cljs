#!/usr/bin/env bash
# The publicity gate: src/, README.md, docs/ and dev/ are PUBLIC documentation. They must
# carry no development-process narrative — no plan/workspace file references,
# no internal machine paths, no user-decision provenance, no phase-landing
# stories, no internal evidence tags, no dated session stamps, no secrets, no
# account of how the code came to be.
#
# Runs FIRST in run-tests.sh (before the suite compiles). Zero network.
# Exit status: 0 when clean; on a hit it prints file:line and fails.
set -uo pipefail
cd "$(dirname "$0")/.."

status=0

# One grep per banned pattern. Each pattern lists what it catches; a comment
# beside it names what is ALLOWED to keep it from flagging (the allowlist
# lives here, not in the source: adding an exception means editing this gate,
# which is where a reviewer sees it).
check () {
  local name="$1" pattern="$2" paths="$3"
  local hits
  hits=$(grep -rnE "$pattern" $paths 2>/dev/null || true)
  if [ -n "$hits" ]; then
    echo "PUBLICITY: $name"
    echo "$hits"
    status=1
  fi
}

check "plan/workspace file references" \
      'plan:|house-rules|workspace\.org|conductor|orchestrator|phase-[0-9a-z-]*-notes' \
      "src README.md docs dev"

check "internal machine paths" \
      '/home/|/Users/|/etc/[a-z]|~/\.(secrets|config|local)' \
      "src README.md docs dev"

# A bare ~ in prose is a path reference too. Allowed: env-var NAMES, key-file
# NAMES without a home prefix.
check "home-relative path references" \
      '~/[a-zA-Z]' \
      "src README.md docs dev"

check "user-decision provenance" \
      '[Uu]ser (directive|ruling|decision|answer|commission)|user-locked|\[USER\]|confirmed by user|user ruling' \
      "src README.md docs dev"

check "phase-landing stories" \
      '[Pp]hase [0-9]+ (landed|complete[sd]?)|landed (at|in) |as-built|built (at|in) commit|[0-9a-f]{7,40}\)|commit [0-9a-f]{7,40}' \
      "src README.md docs dev"

check "internal evidence tags" \
      '\[(SRC|RPT|INV|CALC|MEM|LIVE|DOC|USER)\]' \
      "src README.md docs dev"

check "dated session stamps" \
      '20[0-9]{2}-[0-9]{2}-[0-9]{2}' \
      "src README.md docs dev"

# Narrative about how the code came to be: the documentation states what
# the code does, not what happened while it was written.
check "development narrative" \
      '[Ww]hile building|was met while|during (the )?development|the review(er)? (found|said|asked)|this session|as I |we found' \
      "src README.md docs dev"

# TODO markers addressed to the development process.
check "process TODOs" \
      '(TODO|FIXME|XXX):' \
      "src README.md docs dev"

# Credentials in examples: a password inside a redis:// URL, or a key that
# looks like a real secret. Allowed: the words :password / "password" as
# configuration KEYS, and the bracketed URL grammar of the README.
check "secrets" \
      'redis(s)?://[A-Za-z0-9_.-]*:[A-Za-z0-9_.-]+@|(AKIA|sk-|ghp_)[A-Za-z0-9]{8,}' \
      "src README.md docs dev"

if [ "$status" -ne 0 ]; then
  echo "publicity gate: FAILED — rewrite the lines above to state facts directly"
  exit 1
fi
echo "publicity gate: clean"

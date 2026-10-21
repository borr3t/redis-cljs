#!/usr/bin/env bash
# Compiles the cljs.test suite (shadow-cljs :node-test build) and runs it.
# Exit status: 0 when every test passes, non-zero otherwise.
#
# The build has :autorun true (tests execute on every compile/watch), but
# shadow-cljs does not propagate the autorun exit status, so we disable autorun
# for this one compile and run the generated script ourselves.
#
# CAVEAT: the last line is `exec` — this script REPLACES its shell with node, so
# never chain it mid-command (`./run-tests.sh && something` is fine from a parent
# shell, but nothing placed after the exec line inside this file would run).
#
# The tests never touch the network beyond the loopback interface, a Redis
# server or a secrets file: `npm install` (first run only) fetches the toolchain
# plus `redis`; the REAL node-redis client speaks RESP2 to the fake server of
# test/redis_cljs/fake_server.cljs, a net.Server on a free port of 127.0.0.1
# that answers from a scripted table. The one other socket opened goes to a
# port of 127.0.0.1 that the fake bound and released, to prove a refused
# connection is reported.
#
# The publicity gate runs FIRST: src/ and README.md are public documentation.
set -euo pipefail
cd "$(dirname "$0")"

bash test/publicity_gate.sh

if [ ! -d node_modules ]; then
  npm install
fi

# shadow-cljs needs a JDK on PATH or in JAVA_HOME; the system JDK is used as
# found. Set JAVA_HOME yourself when `java` on PATH is not a working JDK.

npx shadow-cljs compile node-test --config-merge '{:autorun false}'
exec node target/node-test.js

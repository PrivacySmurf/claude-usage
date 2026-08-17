#!/bin/bash
# claude-usage dashboard — scan + serve on loopback only.

set -e

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
LOG_DIR=${CLAUDE_USAGE_LOG_DIR:-"$HOME/.cc-agents/logs"}
mkdir -p "$LOG_DIR"

cd "$SCRIPT_DIR"

# Scan latest transcripts into SQLite DB. A failed initial scan must not take
# down the read-only dashboard; its status remains visible in the log.
python3 cli.py scan >> "$LOG_DIR/claude-usage.log" 2>&1 || true

# Fail closed: caller-supplied HOST cannot widen the LaunchAgent listener.
exec env HOST=127.0.0.1 python3 dashboard.py 9123

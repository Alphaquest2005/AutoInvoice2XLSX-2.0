#!/usr/bin/env bash
# Claude Code auto-commit Stop hook for AutoInvoice2XLSX.
#
# Runs after every Claude Code response. If the working tree has changes,
# stages everything and creates a conventional-format commit. The husky
# pre-commit gates (ruff / mypy / pytest) run naturally; if they fail,
# the changes stay staged and the user is notified on stderr — Claude
# itself is never blocked (hook always exits 0).
#
# Triggered via .claude/settings.json Stop hook.

set -o pipefail

PROJECT_DIR="${CLAUDE_PROJECT_DIR:-$(pwd)}"
LOG_FILE="${PROJECT_DIR}/.claude/hooks/auto-commit.log"

log() {
    printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$LOG_FILE"
}

cd "$PROJECT_DIR" || { log "cd failed: $PROJECT_DIR"; exit 0; }

# Must be inside a git repo
if ! git rev-parse --show-toplevel >/dev/null 2>&1; then
    log "not a git repo, skipping"
    exit 0
fi

# Anything to commit? (respects .gitignore)
if [ -z "$(git status --porcelain)" ]; then
    exit 0
fi

# Stage everything
git add -A

# After staging, verify the index actually differs from HEAD
# (covers the case where only ignored files changed)
if git diff --cached --quiet; then
    exit 0
fi

# Build a conventional-commit message that will pass .husky/commit-msg.
# Format: chore(auto): <n> file(s) — claude session checkpoint (<iso-ts>)
FILE_COUNT=$(git diff --cached --name-only | wc -l | tr -d '[:space:]')
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
MSG="chore(auto): claude session checkpoint (${FILE_COUNT} file(s), ${TIMESTAMP})"

# Commit through the husky pre-commit gates — no --no-verify.
# Capture output so we can surface failures without spamming Claude.
COMMIT_OUT=$(git commit -m "$MSG" 2>&1)
COMMIT_RC=$?

if [ $COMMIT_RC -eq 0 ]; then
    SHA=$(git rev-parse --short HEAD)
    log "committed $SHA — $MSG"
    echo "[auto-commit] $SHA: $MSG" >&2
else
    log "commit failed (rc=$COMMIT_RC) — changes left staged"
    log "$COMMIT_OUT"
    {
        echo "[auto-commit] commit failed (rc=$COMMIT_RC). Changes are staged. Details in $LOG_FILE"
        echo "$COMMIT_OUT" | tail -15
    } >&2
fi

# Never block Claude, even on failure.
exit 0

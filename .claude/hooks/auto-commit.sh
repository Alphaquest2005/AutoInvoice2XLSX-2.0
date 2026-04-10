#!/usr/bin/env bash
# Claude Code auto-commit + backup Stop hook for AutoInvoice2XLSX.
#
# After every Claude Code response:
#   1. If the working tree has changes, stage + commit them through the
#      husky pre-commit gates (ruff / mypy / pytest). Conventional-format
#      commit message. No --no-verify.
#   2. Push main to every configured git remote (e.g. d-backup on the D:
#      bare repo, origin on GitHub once the PAT is refreshed).
#
# Remote pushes are best-effort and non-blocking: a failing push logs an
# error but never blocks Claude (hook always exits 0).
#
# Triggered via .claude/settings.json Stop hook.

set -o pipefail

PROJECT_DIR="${CLAUDE_PROJECT_DIR:-$(pwd)}"
LOG_FILE="${PROJECT_DIR}/.claude/hooks/auto-commit.log"

log() {
    printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$LOG_FILE"
}

mirror_to_backups() {
    # Push main to every configured remote (d-backup, origin, ...).
    local remote push_out push_rc any=0
    while IFS= read -r remote; do
        [ -z "$remote" ] && continue
        any=1
        push_out=$(git push --quiet "$remote" main 2>&1)
        push_rc=$?
        if [ $push_rc -eq 0 ]; then
            log "pushed main -> ${remote}"
        else
            log "push to ${remote} failed (rc=${push_rc}): ${push_out}"
        fi
    done < <(git remote)
    if [ $any -eq 0 ]; then
        log "no git remotes configured, skipping push"
    fi
}

cd "$PROJECT_DIR" || { log "cd failed: $PROJECT_DIR"; exit 0; }

# Must be inside a git repo
if ! git rev-parse --show-toplevel >/dev/null 2>&1; then
    log "not a git repo, skipping"
    exit 0
fi

# Anything to commit? (respects .gitignore)
if [ -z "$(git status --porcelain)" ]; then
    # Clean tree — still mirror in case a previous run's push/bundle failed
    mirror_to_backups
    exit 0
fi

# Stage everything
git add -A

# After staging, verify the index actually differs from HEAD
# (covers the case where only ignored files changed)
if git diff --cached --quiet; then
    mirror_to_backups
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
    mirror_to_backups
else
    log "commit failed (rc=$COMMIT_RC) — changes left staged"
    log "$COMMIT_OUT"
    {
        echo "[auto-commit] commit failed (rc=$COMMIT_RC). Changes are staged. Details in $LOG_FILE"
        echo "$COMMIT_OUT" | tail -15
    } >&2
    # Do NOT mirror a failed state; leave remote/bundle pointing at last
    # known-good commit until gates pass.
fi

# Never block Claude, even on failure.
exit 0

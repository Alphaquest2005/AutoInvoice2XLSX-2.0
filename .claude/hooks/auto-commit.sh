#!/usr/bin/env bash
# Claude Code auto-commit + backup Stop hook for AutoInvoice2XLSX.
#
# After every Claude Code response:
#   1. If the working tree has changes, stage + commit them through the
#      husky pre-commit gates (ruff / mypy / pytest). Conventional-format
#      commit message. No --no-verify.
#   2. Mirror main to the local bare repo on D: (remote: d-backup).
#   3. Rewrite latest.bundle under /mnt/d/OneDrive/dev-backups/... so
#      OneDrive auto-syncs a fresh cloud snapshot.
#
# All backup steps are best-effort and non-blocking: a failing push or
# bundle write logs an error but never blocks Claude (hook always exits 0).
# The bundle is written via tmp+atomic-rename so OneDrive cannot catch it
# mid-write.
#
# Triggered via .claude/settings.json Stop hook.

set -o pipefail

PROJECT_DIR="${CLAUDE_PROJECT_DIR:-$(pwd)}"
LOG_FILE="${PROJECT_DIR}/.claude/hooks/auto-commit.log"
BACKUP_REMOTE="d-backup"
BUNDLE_DIR="/mnt/d/OneDrive/dev-backups/AutoInvoice2XLSX-2.0"
BUNDLE_FILE="${BUNDLE_DIR}/latest.bundle"

log() {
    printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$LOG_FILE"
}

mirror_to_backups() {
    # Push to D: bare repo (different physical disk)
    if git remote get-url "$BACKUP_REMOTE" >/dev/null 2>&1; then
        local push_out push_rc
        push_out=$(git push --quiet "$BACKUP_REMOTE" main 2>&1)
        push_rc=$?
        if [ $push_rc -eq 0 ]; then
            log "pushed main -> ${BACKUP_REMOTE}"
        else
            log "push to ${BACKUP_REMOTE} failed (rc=${push_rc}): ${push_out}"
        fi
    else
        log "remote ${BACKUP_REMOTE} not configured, skipping push"
    fi

    # Bundle to OneDrive-synced folder (cloud offsite)
    if [ -d "$BUNDLE_DIR" ]; then
        local tmp_bundle="${BUNDLE_FILE}.tmp.$$"
        if git bundle create "$tmp_bundle" --all >/dev/null 2>&1; then
            # Atomic on same filesystem — OneDrive cannot see a half-written file
            if mv -f "$tmp_bundle" "$BUNDLE_FILE"; then
                local sz
                sz=$(stat -c '%s' "$BUNDLE_FILE" 2>/dev/null || echo "?")
                log "bundle refreshed: ${BUNDLE_FILE} (${sz} bytes)"
            else
                rm -f "$tmp_bundle"
                log "bundle rename failed"
            fi
        else
            rm -f "$tmp_bundle"
            log "bundle create failed"
        fi
    else
        log "bundle dir ${BUNDLE_DIR} missing, skipping bundle"
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

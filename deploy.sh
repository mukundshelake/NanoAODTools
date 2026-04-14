#!/bin/bash
# =============================================================================
# deploy.sh — Rsync NanoAODTools repo to a remote host
#
# Usage:
#   bash deploy.sh <host>
#   bash deploy.sh lxplus          # uses .env.lxplus for remote REPO_ROOT
#   bash deploy.sh lxplus --dry-run
#
# What it does:
#   1. Reads the remote REPO_ROOT from .env.<host>
#   2. Reads the local  REPO_ROOT from .env.local
#   3. Rsyncs local → remote (excludes .git, __pycache__, build/, .env)
#   4. Copies .env.<host> to .env on the remote so jobs work immediately
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
HOST="${1:-}"
DRY_RUN=""
for arg in "$@"; do
    [[ "$arg" == "--dry-run" ]] && DRY_RUN="--dry-run"
done

if [[ -z "$HOST" ]]; then
    echo "Usage: bash deploy.sh <host> [--dry-run]"
    echo "  host   — matches a .env.<host> file in this repo"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOCAL_ENV=".env.local"
REMOTE_ENV=".env.${HOST}"

if [[ ! -f "$LOCAL_ENV" ]]; then
    echo "ERROR: $LOCAL_ENV not found. Cannot determine local REPO_ROOT."
    exit 1
fi
if [[ ! -f "$REMOTE_ENV" ]]; then
    echo "ERROR: $REMOTE_ENV not found. Cannot determine remote REPO_ROOT."
    exit 1
fi

# ---------------------------------------------------------------------------
# Read REPO_ROOT and SSH_HOST from env files
# ---------------------------------------------------------------------------
_read_var() { grep -E "^${2}=" "$1" | head -1 | sed "s/^${2}=//;s/['\"]//g"; }

LOCAL_REPO_ROOT="$(_read_var "$LOCAL_ENV" "REPO_ROOT")"
REMOTE_REPO_ROOT="$(_read_var "$REMOTE_ENV" "REPO_ROOT")"
# SSH_HOST can be overridden in the env file; otherwise use the host argument
SSH_HOST="$(_read_var "$REMOTE_ENV" "SSH_HOST")"
SSH_HOST="${SSH_HOST:-${HOST}.cern.ch}"

if [[ -z "$LOCAL_REPO_ROOT" ]]; then
    echo "ERROR: REPO_ROOT not set in $LOCAL_ENV"
    exit 1
fi
if [[ -z "$REMOTE_REPO_ROOT" ]]; then
    echo "ERROR: REPO_ROOT not set in $REMOTE_ENV"
    exit 1
fi

echo "============================================================"
echo " deploy.sh"
echo "  Host         : ${SSH_HOST}"
echo "  Local  root  : ${LOCAL_REPO_ROOT}"
echo "  Remote root  : ${SSH_HOST}:${REMOTE_REPO_ROOT}"
[[ -n "$DRY_RUN" ]] && echo "  Mode         : DRY RUN"
echo "============================================================"

# ---------------------------------------------------------------------------
# Temporarily copy .env.<host> to .env so it is included in the rsync and
# lands on the remote as .env — single SSH connection, single 2FA prompt.
# ---------------------------------------------------------------------------
CURRENT_ENV=""
if [[ -f ".env" ]]; then
    CURRENT_ENV="$(cat .env)"
fi

cleanup() {
    # Restore .env to its original content regardless of how we exit
    if [[ -n "$CURRENT_ENV" ]]; then
        printf '%s' "$CURRENT_ENV" > .env
    else
        rm -f .env
    fi
}
trap cleanup EXIT

if [[ -z "$DRY_RUN" ]]; then
    cp "${REMOTE_ENV}" .env
fi

# ---------------------------------------------------------------------------
# Rsync repo (code only — no build artefacts, no data)
# .env is included so it arrives as the correct host config in one shot.
# ---------------------------------------------------------------------------
rsync -avz --progress $DRY_RUN \
    --exclude='.git/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='build/' \
    --exclude='condor/logs/' \
    --exclude='condor/jobs/' \
    --exclude='*.root' \
    --exclude='*.parquet' \
    --exclude='*.pkl' \
    "${LOCAL_REPO_ROOT}/" \
    "${SSH_HOST}:${REMOTE_REPO_ROOT}/"

echo ""
echo "Done. Remote .env is now set for ${HOST}."

#!/bin/bash
# =============================================================================
# sync_data.sh — Rsync processed output files between hosts
#
# Usage:
#   bash sync_data.sh <direction> <host> [options]
#
# Directions:
#   push   — local OUTPUT_STORAGE → remote OUTPUT_STORAGE
#   pull   — remote OUTPUT_STORAGE → local OUTPUT_STORAGE
#
# Options:
#   --tag   <outputTag>   limit to one output tag  (e.g. midNov)
#   --era   <era>         limit to one era         (e.g. UL2018)
#   --stage <stage>       limit to one stage       (e.g. reco)
#   --dry-run             print what would be transferred without doing it
#
# Examples:
#   bash sync_data.sh pull  lxplus --tag midNov --era UL2018 --stage reco
#   bash sync_data.sh push  lxplus --tag midNov
#   bash sync_data.sh pull  lxplus --dry-run
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
DIRECTION="${1:-}"
HOST="${2:-}"

if [[ -z "$DIRECTION" || -z "$HOST" ]]; then
    echo "Usage: bash sync_data.sh <push|pull> <host> [--tag TAG] [--era ERA] [--stage STAGE] [--dry-run]"
    exit 1
fi

TAG=""
ERA=""
STAGE=""
DRY_RUN=""

shift 2
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)    TAG="$2";    shift 2 ;;
        --era)    ERA="$2";    shift 2 ;;
        --stage)  STAGE="$2";  shift 2 ;;
        --dry-run) DRY_RUN="--dry-run"; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOCAL_ENV=".env.local"
REMOTE_ENV=".env.${HOST}"

if [[ ! -f "$LOCAL_ENV" ]];  then echo "ERROR: $LOCAL_ENV not found.";  exit 1; fi
if [[ ! -f "$REMOTE_ENV" ]]; then echo "ERROR: $REMOTE_ENV not found."; exit 1; fi

_read_var() { grep -E "^${2}=" "$1" | head -1 | sed "s/^${2}=//;s/['\"]//g"; }

LOCAL_OUTPUT="$(_read_var "$LOCAL_ENV"  "OUTPUT_STORAGE")"
REMOTE_OUTPUT="$(_read_var "$REMOTE_ENV" "OUTPUT_STORAGE")"
SSH_HOST="$(_read_var "$REMOTE_ENV" "SSH_HOST")"
SSH_HOST="${SSH_HOST:-${HOST}.cern.ch}"

if [[ -z "$LOCAL_OUTPUT"  ]]; then echo "ERROR: OUTPUT_STORAGE not set in $LOCAL_ENV";  exit 1; fi
if [[ -z "$REMOTE_OUTPUT" ]]; then echo "ERROR: OUTPUT_STORAGE not set in $REMOTE_ENV"; exit 1; fi

# ---------------------------------------------------------------------------
# Build include/exclude filters from tag / era / stage
# ---------------------------------------------------------------------------
# Output directory structure is assumed to be:
#   OUTPUT_STORAGE/<tag>/<stage>/<era>/<key>/...
#   or any subset thereof.
FILTERS=()

if [[ -n "$TAG" ]];   then FILTERS+=("--filter=+ /${TAG}/***" "--filter=- /*/"  ); fi

# Note: rsync filter ordering matters — include specific paths, exclude rest
RSYNC_FILTERS=()
if [[ -n "$TAG" && -n "$STAGE" && -n "$ERA" ]]; then
    RSYNC_FILTERS=(
        "--filter=+ /${TAG}/"
        "--filter=+ /${TAG}/${STAGE}/"
        "--filter=+ /${TAG}/${STAGE}/${ERA}/***"
        "--filter=- *"
    )
elif [[ -n "$TAG" && -n "$STAGE" ]]; then
    RSYNC_FILTERS=(
        "--filter=+ /${TAG}/"
        "--filter=+ /${TAG}/${STAGE}/***"
        "--filter=- *"
    )
elif [[ -n "$TAG" ]]; then
    RSYNC_FILTERS=(
        "--filter=+ /${TAG}/***"
        "--filter=- *"
    )
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "============================================================"
echo " sync_data.sh"
echo "  Direction : ${DIRECTION}"
echo "  Host      : ${SSH_HOST}"
[[ -n "$TAG"   ]] && echo "  Tag       : ${TAG}"
[[ -n "$ERA"   ]] && echo "  Era       : ${ERA}"
[[ -n "$STAGE" ]] && echo "  Stage     : ${STAGE}"
[[ -n "$DRY_RUN" ]] && echo "  Mode      : DRY RUN"
echo "============================================================"

# ---------------------------------------------------------------------------
# Rsync
# ---------------------------------------------------------------------------
RSYNC_OPTS="-avz --progress ${DRY_RUN}"

if [[ "$DIRECTION" == "push" ]]; then
    echo "Pushing: ${LOCAL_OUTPUT}/ → ${SSH_HOST}:${REMOTE_OUTPUT}/"
    # shellcheck disable=SC2086
    echo "Running: rsync $RSYNC_OPTS ${RSYNC_FILTERS[*]+${RSYNC_FILTERS[*]}} ${LOCAL_OUTPUT}/ ${SSH_HOST}:${REMOTE_OUTPUT}/"
    rsync $RSYNC_OPTS "${RSYNC_FILTERS[@]+"${RSYNC_FILTERS[@]}"}" \
        "${LOCAL_OUTPUT}/" \
        "${SSH_HOST}:${REMOTE_OUTPUT}/"

elif [[ "$DIRECTION" == "pull" ]]; then
    echo "Pulling: ${SSH_HOST}:${REMOTE_OUTPUT}/ → ${LOCAL_OUTPUT}/"
    mkdir -p "${LOCAL_OUTPUT}"
    # shellcheck disable=SC2086
    echo "Running: rsync $RSYNC_OPTS ${RSYNC_FILTERS[*]+${RSYNC_FILTERS[*]}} ${SSH_HOST}:${REMOTE_OUTPUT}/ ${LOCAL_OUTPUT}/"
    rsync $RSYNC_OPTS "${RSYNC_FILTERS[@]+"${RSYNC_FILTERS[@]}"}" \
        "${SSH_HOST}:${REMOTE_OUTPUT}/" \
        "${LOCAL_OUTPUT}/"

else
    echo "ERROR: direction must be 'push' or 'pull', got '${DIRECTION}'"
    exit 1
fi

echo ""
echo "sync_data.sh complete."

#!/bin/bash
# CMSSW Environment Initialization
#
# This script initializes CMSSW environment if available.
# Safe to run on machines without CMSSW (will skip silently).
#
# Usage:
#   source scripts/cmssw_helper.sh
#   python scripts/run_all.py

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Load .env configuration
if [ ! -f "$REPO_ROOT/.env" ]; then
    echo "Warning: .env not found. CMSSW skipped."
    exit 0
fi

# Extract CMSSW_BASE from .env
CMSSW_BASE=$(grep "^CMSSW_BASE=" "$REPO_ROOT/.env" | cut -d'=' -f2 | tr -d '"' | tr -d "'")

# Skip if CMSSW_BASE is empty or doesn't exist
if [ -z "$CMSSW_BASE" ] || [ ! -d "$CMSSW_BASE" ]; then
    echo "ℹ  CMSSW not configured or not available."
    exit 0
fi

# Initialize CMSSW
echo "Initializing CMSSW..."
cd "$CMSSW_BASE"
eval $(scram runtime -sh)
cd - > /dev/null

echo "✓ CMSSW environment loaded"
echo "  CMSSW_BASE: $CMSSW_BASE"
echo "  SCRAM_ARCH: $SCRAM_ARCH"
echo "  CMSSW_VERSION: $CMSSW_VERSION"

#!/bin/bash
# =============================================================================
# setup_lxplus.sh  —  One-shot setup for NanoAODTools on lxplus
# =============================================================================
#
# Run this ONCE after copying the repo to lxplus.
# It handles conda installation, environment creation, standalone setup,
# and .env configuration.
#
# Usage:
#   bash setup_lxplus.sh [--env-only] [--no-conda-install]
#
#   --env-only          Skip conda installation; assume conda/mamba already exists
#   --no-conda-install  Same as --env-only (alias)
#
# After this script completes, start each new session with:
#   conda activate latestcoffea
#   source standalone/env_standalone.sh
# =============================================================================

set -eo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_NAME="latestcoffea"
ENV_YML="${REPO_ROOT}/environment_lxplus.yml"

# ---------- colour helpers ----------
_green()  { echo -e "\033[32m$*\033[0m"; }
_yellow() { echo -e "\033[33m$*\033[0m"; }
_red()    { echo -e "\033[31m$*\033[0m"; }
_blue()   { echo -e "\033[34m$*\033[0m"; }
_sep()    { echo "------------------------------------------------------------"; }

# ---------- flags ----------
SKIP_CONDA_INSTALL=0
for arg in "$@"; do
    [[ "$arg" == "--env-only" || "$arg" == "--no-conda-install" ]] && SKIP_CONDA_INSTALL=1
done

_sep
_blue " NanoAODTools — lxplus setup"
_blue " Repo root : $REPO_ROOT"
_blue " Env name  : $ENV_NAME"
_sep

# =============================================================================
# Step 1: Install Miniforge (provides conda + mamba) if not present
# =============================================================================
if [[ $SKIP_CONDA_INSTALL -eq 0 ]]; then
    if ! command -v conda &>/dev/null; then
        _yellow "conda not found — installing Miniforge3..."
        MINIFORGE_URL="https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh"
        MINIFORGE_SCRIPT="/tmp/Miniforge3-Linux-x86_64.sh"

        curl -fsSL "$MINIFORGE_URL" -o "$MINIFORGE_SCRIPT"
        bash "$MINIFORGE_SCRIPT" -b -p "${HOME}/miniforge3"
        rm -f "$MINIFORGE_SCRIPT"

        # Initialise conda for this shell session
        # shellcheck source=/dev/null
        source "${HOME}/miniforge3/etc/profile.d/conda.sh"
        conda init bash
        _green "Miniforge installed at ${HOME}/miniforge3"
        _yellow "NOTE: After this script finishes, restart your shell or run:"
        _yellow "  source ~/.bashrc"
    else
        _green "conda already installed: $(which conda)"
    fi
else
    _yellow "Skipping conda installation (--env-only)"
fi

# Make sure conda is callable in this shell (handles case where it was just installed)
CONDA_SH=""
for p in "${HOME}/miniforge3/etc/profile.d/conda.sh" \
          "${HOME}/miniconda3/etc/profile.d/conda.sh" \
          "${HOME}/anaconda3/etc/profile.d/conda.sh"; do
    [[ -f "$p" ]] && CONDA_SH="$p" && break
done

if [[ -n "$CONDA_SH" ]]; then
    # shellcheck source=/dev/null
    source "$CONDA_SH"
fi

if ! command -v conda &>/dev/null; then
    _red "ERROR: conda is still not available. Please install conda/mamba manually."
    exit 1
fi

# Prefer mamba for faster solves
INSTALLER="conda"
command -v mamba &>/dev/null && INSTALLER="mamba"
_green "Using installer: $INSTALLER"

# =============================================================================
# Step 2: Create / update conda environment
# =============================================================================
_sep
_blue "Step 2: conda environment"

if conda env list | grep -q "^${ENV_NAME} "; then
    _yellow "Environment '${ENV_NAME}' already exists."
    read -r -p "  Re-create it from scratch? [y/N] " answer
    if [[ "${answer,,}" == "y" ]]; then
        conda env remove -n "$ENV_NAME" -y
        _green "Removed old environment."
    else
        _yellow "Keeping existing environment — skipping creation."
        SKIP_ENV_CREATE=1
    fi
fi

if [[ -z "$SKIP_ENV_CREATE" ]]; then
    _yellow "Creating environment from ${ENV_YML} (this takes 10–20 min on first run)..."
    "$INSTALLER" env create -f "$ENV_YML" -n "$ENV_NAME"
    _green "Environment '${ENV_NAME}' created."
fi

# Activate for subsequent steps
# shellcheck source=/dev/null
conda activate "$ENV_NAME"
_green "Activated env: $CONDA_DEFAULT_ENV"

# =============================================================================
# Step 3: NanoAODTools standalone build
# =============================================================================
_sep
_blue "Step 3: NanoAODTools standalone build"

cd "$REPO_ROOT"

if [[ ! -d "build" ]]; then
    _yellow "Creating build directory..."
    source standalone/env_standalone.sh build
fi

source standalone/env_standalone.sh
_green "Standalone environment set."

# Quick import test
python3 -c "from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor; print('PostProcessor import OK')"

# =============================================================================
# Step 4: .env file
# =============================================================================
_sep
_blue "Step 4: .env configuration"

ENV_FILE="${REPO_ROOT}/.env"
if [[ -f "$ENV_FILE" ]]; then
    _yellow ".env already exists — skipping."
else
    cp "${REPO_ROOT}/.env.lxplus.example" "$ENV_FILE"
    _yellow "Copied .env.lxplus.example → .env"
    _yellow "IMPORTANT: Edit .env now and set the correct paths for your account."
    _yellow "  Required variables:"
    _yellow "    REPO_ROOT     = absolute path to this repo on lxplus"
    _yellow "    INPUT_STORAGE = /eos/user/<initial>/<username>/RUN2_UL/Tree_crab"
    _yellow "    OUTPUT_STORAGE= /eos/user/<initial>/<username>/skimmed_Run2"
    echo ""
    echo "  Opening .env for editing in 3 seconds (Ctrl+C to skip)..."
    sleep 3
    "${EDITOR:-vi}" "$ENV_FILE"
fi

# =============================================================================
# Step 5: Migrate existing JSON paths to relative (if needed)
# =============================================================================
_sep
_blue "Step 5: JSON path migration (dry-run check)"

python3 scripts/migrate_json_paths.py --dry-run 2>&1 | head -10

_yellow ""
_yellow "If the above shows paths to convert, run:"
_yellow "  python3 scripts/migrate_json_paths.py"

# =============================================================================
# Step 6: Quick smoke test
# =============================================================================
_sep
_blue "Step 6: Quick smoke test of main_new.py imports"

python3 - <<'PYEOF'
import sys
failed = []
for mod in ["ROOT", "yaml", "tqdm", "correctionlib", "uproot", "awkward", "vector"]:
    try:
        __import__(mod)
    except ImportError as e:
        failed.append(f"{mod}: {e}")

if failed:
    print("MISSING IMPORTS:")
    for f in failed:
        print(f"  {f}")
    sys.exit(1)
else:
    print("All key imports OK.")
PYEOF

# =============================================================================
# Done
# =============================================================================
_sep
_green " Setup complete!"
_sep
echo ""
echo "  To start a new session on lxplus, run:"
echo ""
echo "    conda activate ${ENV_NAME}"
echo "    cd ${REPO_ROOT}"
echo "    source standalone/env_standalone.sh"
echo ""
echo "  Test the reco stage with:"
echo "    python3 main_new.py -s reco -t midNov -e UL2016preVFP --sample"
echo ""

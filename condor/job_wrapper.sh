#!/bin/bash
# =============================================================================
# condor/job_wrapper.sh
# =============================================================================
# HTCondor worker script — called once per (stage, era, dataset-key) job.
#
# Arguments (positional, set by condor "arguments" line in .sub):
#   $1  REPO_ROOT    — absolute path to NanoAODTools repo
#   $2  OUTPUT_TAG   — config tag (e.g. midNov)
#   $3  STAGE        — pipeline stage (e.g. selection)
#   $4  ERA          — era (e.g. UL2018)
#   $5  KEY          — dataset key regex (anchored: ^KEY$)
#   $6  WORKERS      — number of CPU cores to use (default: 4)
#
# Environment variables can also be set via the .env file in REPO_ROOT.
# Real environment variables take priority over .env (so condor can override).
#
# Software setup strategy (first match wins):
#   1. If conda is available and env "latestcoffea" exists → activate it.
#   2. Else source LCG_106 from CVMFS (provides Python 3.11 + most packages).
#   3. Else fall back to the system python3 (likely missing packages — expect failures).
# =============================================================================

set -eo pipefail

REPO_ROOT="${1}"
OUTPUT_TAG="${2}"
STAGE="${3}"
ERA="${4}"
KEY="${5}"
WORKERS="${6:-4}"
SAMPLE_MODE="${7:-0}"  # pass 1 to add --sample (single-file test mode)

# ---------------------------------------------------------------------------
# Basic validation
# ---------------------------------------------------------------------------
if [[ -z "$REPO_ROOT" || -z "$OUTPUT_TAG" || -z "$STAGE" || -z "$ERA" || -z "$KEY" ]]; then
    echo "ERROR: missing required arguments."
    echo "Usage: $0 REPO_ROOT OUTPUT_TAG STAGE ERA KEY [WORKERS]"
    exit 1
fi

echo "========================================================"
echo " NanoAODTools HTCondor job"
echo "  REPO_ROOT  : $REPO_ROOT"
echo "  OUTPUT_TAG : $OUTPUT_TAG"
echo "  STAGE      : $STAGE"
echo "  ERA        : $ERA"
echo "  KEY        : $KEY"
echo "  WORKERS    : $WORKERS"
echo "  HOST       : $(hostname)"
echo "  DATE       : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"

# ---------------------------------------------------------------------------
# Source .env so that INPUT_STORAGE / OUTPUT_STORAGE are available
# (main_new.py also reads .env, but sourcing here helps any bash logic below)
# ---------------------------------------------------------------------------
ENV_FILE="${REPO_ROOT}/.env"
if [[ -f "$ENV_FILE" ]]; then
    echo "Sourcing $ENV_FILE"
    set -a
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    set +a
fi

# ---------------------------------------------------------------------------
# Software environment setup
# ---------------------------------------------------------------------------
CONDA_ENV_NAME="${CONDA_ENV_NAME:-latestcoffea}"

setup_done=0

# Option 1: conda
if command -v conda &>/dev/null; then
    echo "Found conda — activating env '${CONDA_ENV_NAME}'"
    # conda shell.bash hook may not be initialised in a batch job
    # eval + conda shell.bash hook is the robust way
    eval "$(conda shell.bash hook 2>/dev/null)" || true
    if conda activate "${CONDA_ENV_NAME}" 2>/dev/null; then
        echo "conda env '${CONDA_ENV_NAME}' activated."
        setup_done=1
    else
        echo "WARNING: conda env '${CONDA_ENV_NAME}' not found. Trying LCG..."
    fi
fi

# Option 2: LCG from CVMFS (lxplus default fallback)
if [[ $setup_done -eq 0 ]]; then
    LCG_SETUP="/cvmfs/sft.cern.ch/lcg/views/LCG_106/x86_64-el9-gcc13-opt/setup.sh"
    if [[ -f "$LCG_SETUP" ]]; then
        echo "Activating LCG_106 from CVMFS..."
        # shellcheck source=/dev/null
        source "$LCG_SETUP"
        setup_done=1
    fi
fi

if [[ $setup_done -eq 0 ]]; then
    echo "WARNING: Neither conda env nor CVMFS LCG found. Using system python3."
fi

echo "Python: $(which python3)  — $(python3 --version)"

# ---------------------------------------------------------------------------
# Set up PhysicsTools.NanoAODTools standalone (PYTHONPATH)
# ---------------------------------------------------------------------------
# The build/ symlink tree must exist (created once by:
#   source standalone/env_standalone.sh build
# on lxplus). Jobs activate it by sourcing without the 'build' argument.
STANDALONE="${REPO_ROOT}/standalone/env_standalone.sh"
if [[ -f "${STANDALONE}" ]]; then
    echo "Sourcing standalone env: ${STANDALONE}"
    # shellcheck source=/dev/null
    source "${STANDALONE}"
else
    echo "ERROR: standalone/env_standalone.sh not found in REPO_ROOT=${REPO_ROOT}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Run main_new.py
# ---------------------------------------------------------------------------
cd "${REPO_ROOT}"

echo ""
echo "Running: python3 -u main_new.py -t ${OUTPUT_TAG} --stage ${STAGE} --era ${ERA} --includeKeys '^${KEY}$' --workers ${WORKERS}${SAMPLE_MODE:+ --sample}"
echo ""

SAMPLE_FLAG=""
[[ "${SAMPLE_MODE}" == "1" ]] && SAMPLE_FLAG="--sample"

python3 -u main_new.py \
    --outputTag "${OUTPUT_TAG}" \
    --stage     "${STAGE}" \
    --era       "${ERA}" \
    --includeKeys "^${KEY}$" \
    --workers   "${WORKERS}" \
    ${SAMPLE_FLAG}

EXIT_CODE=$?

echo ""
echo "========================================================"
echo " Job finished with exit code: ${EXIT_CODE}"
echo " DATE: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================"

exit $EXIT_CODE

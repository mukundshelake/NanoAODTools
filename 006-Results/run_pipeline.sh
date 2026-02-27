#!/bin/bash
# run_pipeline.sh
# Runs the full asymmetry pipeline:
#   Step 1: getNs.py     -> <OUTPUT_FOLDER>/<OUTPUT_NAME>.coffea
#   Step 2: extractNs.py -> <OUTPUT_FOLDER>/<OUTPUT_NAME>.json  (counts)
#   Step 3: getAs.py     -> <OUTPUT_FOLDER>/As_<OUTPUT_NAME>.json
#
# Usage:
#   ./run_pipeline.sh [options]
#
# Options:
#   --era ERA              Era string [required]. One of: UL2016preVFP, UL2016postVFP, UL2017, UL2018
#   --json_file PATH       Path to the dataFiles JSON [required]
#   --lumiXinfo FILE       Path to the lumiXinfo JSON [required]
#   --output_folder DIR    Output folder (default: Outputs)
#   --output_name NAME     Base name for intermediate and final outputs (default: ns_<ERA>)
#   --bdt_cut SCORE        Apply BDTScore > SCORE cut in getNs.py (optional)
#   --qqbar_only           Filter MC to qqbar events only in getNs.py (optional)
#   --conda_env ENV        Conda environment to use (default: latestcoffea)
#   --skip_step1           Skip getNs.py (reuse existing coffea file)
#   --skip_step2           Skip extractNs.py (reuse existing counts JSON)
#   -h, --help             Print this help message

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
ERA=""
JSON_FILE=""
LUMIINFO_FILE=""
OUTPUT_FOLDER="Outputs"
OUTPUT_NAME=""
BDT_CUT=""
QQBAR_ONLY=false
CONDA_ENV="latestcoffea"
SKIP_STEP1=false
SKIP_STEP2=false

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/scripts"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --era)           ERA="$2";          shift 2 ;;
        --json_file)     JSON_FILE="$2";    shift 2 ;;
        --lumiXinfo)     LUMIINFO_FILE="$2"; shift 2 ;;
        --output_folder) OUTPUT_FOLDER="$2"; shift 2 ;;
        --output_name)   OUTPUT_NAME="$2";  shift 2 ;;
        --bdt_cut)       BDT_CUT="$2";      shift 2 ;;
        --qqbar_only)    QQBAR_ONLY=true;   shift ;;
        --conda_env)     CONDA_ENV="$2";    shift 2 ;;
        --skip_step1)    SKIP_STEP1=true;   shift ;;
        --skip_step2)    SKIP_STEP2=true;   shift ;;
        -h|--help)
            sed -n '/^# Usage:/,/^[^#]/{ /^[^#]/d; s/^# \{0,1\}//; p }' "$0"
            exit 0
            ;;
        *) echo "[ERROR] Unknown argument: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
errors=()
[[ -z "$ERA" ]]          && errors+=("--era is required")
[[ -z "$JSON_FILE" ]]    && errors+=("--json_file is required")
[[ -z "$LUMIINFO_FILE" ]] && errors+=("--lumiXinfo is required")

if [[ ${#errors[@]} -gt 0 ]]; then
    for e in "${errors[@]}"; do echo "[ERROR] $e"; done
    exit 1
fi

[[ -z "$OUTPUT_NAME" ]] && OUTPUT_NAME="ns_${ERA}"

COFFEA_FILE="${OUTPUT_FOLDER}/${OUTPUT_NAME}.coffea"
COUNTS_FILE="${OUTPUT_FOLDER}/${OUTPUT_NAME}.json"
AS_NAME="As_${OUTPUT_NAME}"

# ---------------------------------------------------------------------------
# Print run configuration
# ---------------------------------------------------------------------------
echo "============================================================"
echo " Pipeline configuration"
echo "============================================================"
echo "  ERA          : $ERA"
echo "  JSON file    : $JSON_FILE"
echo "  LumiXinfo    : $LUMIINFO_FILE"
echo "  Output folder: $OUTPUT_FOLDER"
echo "  Output name  : $OUTPUT_NAME"
echo "  BDT cut      : ${BDT_CUT:-none}"
echo "  qqbar only   : $QQBAR_ONLY"
echo "  Conda env    : $CONDA_ENV"
echo "  Skip step 1  : $SKIP_STEP1"
echo "  Skip step 2  : $SKIP_STEP2"
echo "============================================================"
echo ""

PYTHON="conda run -n ${CONDA_ENV} python3"

# Build optional getNs.py flags
GETNNS_OPTS=""
[[ -n "$BDT_CUT" ]]   && GETNNS_OPTS+=" --bdt_cut ${BDT_CUT}"
[[ "$QQBAR_ONLY" == true ]] && GETNNS_OPTS+=" --qqbar_only"

# ---------------------------------------------------------------------------
# Step 1: getNs.py
# ---------------------------------------------------------------------------
if [[ "$SKIP_STEP1" == true ]]; then
    echo "[Step 1] Skipped (--skip_step1). Using existing: ${COFFEA_FILE}"
else
    echo "[Step 1] Running getNs.py ..."
    $PYTHON "${SCRIPT_DIR}/getNs.py" \
        --era           "$ERA" \
        --json_file     "$JSON_FILE" \
        --output_folder "$OUTPUT_FOLDER" \
        --output_name   "$OUTPUT_NAME" \
        $GETNNS_OPTS
    echo "[Step 1] Done -> ${COFFEA_FILE}"
fi

echo ""

# ---------------------------------------------------------------------------
# Step 2: extractNs.py
# ---------------------------------------------------------------------------
if [[ "$SKIP_STEP2" == true ]]; then
    echo "[Step 2] Skipped (--skip_step2). Using existing: ${COUNTS_FILE}"
else
    echo "[Step 2] Running extractNs.py ..."
    $PYTHON "${SCRIPT_DIR}/extractNs.py" \
        --coffea_file   "$COFFEA_FILE" \
        --output_folder "$OUTPUT_FOLDER" \
        --output_name   "$OUTPUT_NAME"
    echo "[Step 2] Done -> ${COUNTS_FILE}"
fi

echo ""

# ---------------------------------------------------------------------------
# Step 3: getAs.py
# ---------------------------------------------------------------------------
echo "[Step 3] Running getAs.py ..."
$PYTHON "${SCRIPT_DIR}/getAs.py" \
    --counts_file    "$COUNTS_FILE" \
    --lumiXinfo_file "$LUMIINFO_FILE" \
    --output_folder  "$OUTPUT_FOLDER" \
    --output_name    "$AS_NAME"
echo "[Step 3] Done -> ${OUTPUT_FOLDER}/${AS_NAME}.json"

echo ""
echo "============================================================"
echo " Pipeline complete."
echo "============================================================"

#!/bin/bash
# run_all.sh
#
# Run the full analysis pipeline for ALL eras.
# For each era it runs:
#   1. getObservables_reco.py  (reco-level, BDT cut applied)
#   2. extractNs.py            (extract event counts from coffea file)
#   3. getAs.py                (compute asymmetries from counts + lumiXinfo)
#   4. makeDatacards.py        (generate Combine datacards + run_impacts.sh)
#
# Steps 2-4 require a per-era lumiXinfo JSON in Inputs/ and are skipped if absent.
# Eras that have no BDTScore dataFiles JSON in Inputs/ are skipped entirely.
#
# Usage:
#   ./run_all.sh                   # nominal only
#   SYST="--syst" ./run_all.sh     # enable weight-based systematics
#   ERA="UL2018" ./run_all.sh      # run a single era only
#
# Environment variables respected:
#   SYST   (default: empty)   — pass "--syst" to activate syst mode
#   ERA    (default: empty)   — restrict to one era; runs all four if unset

# ─── Configuration ─────────────────────────────────────────────────────────────
CONDA_ENV="latestcoffea"
# -u = unbuffered: Python flushes every print() immediately instead of batching
PYTHON="conda run -n ${CONDA_ENV} python3 -u"
export PYTHONUNBUFFERED=1   # belt-and-suspenders: also tells the C runtime

INPUT_TAG="midNov"                  # must match the processFlow tag
OUTPUT_FOLDER="Outputs/midNov"

BDT_CUT=0.55                        # BDT score threshold for signal enrichment

ALL_ERAS=("UL2016preVFP" "UL2016postVFP" "UL2017" "UL2018")

# ─── Logging helper (defined early so overrides block can use it) ──────────────
log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ─── Overrides from environment ────────────────────────────────────────────────
SYST="${SYST:-}"                    # "--syst" or empty
if [[ -n "${ERA:-}" ]]; then
    ALL_ERAS=("$ERA")
    log "Restricting to single era: $ERA"
fi

if [[ -n "$SYST" ]]; then
    SYST_SUFFIX="_syst"
    log "Systematics mode ON (--syst flag active)"
else
    SYST_SUFFIX=""
fi

# ─── Helpers ───────────────────────────────────────────────────────────────────
run_step() {
    # run_step LABEL cmd args...
    # Prints a timestamped start line, runs the command with live output,
    # prints done + elapsed time, and exits the whole script on failure.
    local label="$1"; shift
    local t0
    t0=$(date +%s)
    log "START  ▶ ${label}"
    echo "──────────────────────────────────────────────────────────"
    "$@"
    local rc=$?
    local elapsed=$(( $(date +%s) - t0 ))
    echo "──────────────────────────────────────────────────────────"
    if [[ $rc -ne 0 ]]; then
        log "ERROR  ✗ ${label} — failed (exit ${rc}) after ${elapsed}s"
        log "       Command: $*"
        exit $rc
    fi
    log "DONE   ✓ ${label} — finished in ${elapsed}s"
}

count_files_in_json() {
    # Print the total number of ROOT files listed in a dataFiles JSON.
    python3 -c "
import json, sys
with open('$1') as f: d = json.load(f)
total = sum(len(files) for grp in d.values() for files in grp.values())
print(total)
" 2>/dev/null || echo "?"
}

# ─── Main loop ─────────────────────────────────────────────────────────────────
all_started=0
all_skipped=0

SCRIPT_T0=$(date +%s)
log "════════════════════════════════════════════════════════"
log "  run_all.sh starting"
log "  Tag     : $INPUT_TAG"
log "  Eras    : ${ALL_ERAS[*]}"
log "  BDT cut : $BDT_CUT"
log "  Syst    : ${SYST:-none}"
log "  Outputs : $OUTPUT_FOLDER/"
log "════════════════════════════════════════════════════════"

for ERA_NAME in "${ALL_ERAS[@]}"; do
    JSON_FILE="Inputs/${INPUT_TAG}_BDTScore_${ERA_NAME}_dataFiles.json"

    if [[ ! -f "$JSON_FILE" ]]; then
        log "SKIP   ⊘ $ERA_NAME — input JSON not found: $JSON_FILE"
        (( all_skipped++ )) || true
        continue
    fi

    N_FILES=$(count_files_in_json "$JSON_FILE")
    echo ""
    log "════════════════════════════════════════════════════════"
    log "  Era: $ERA_NAME  ($N_FILES ROOT files in JSON)"
    log "════════════════════════════════════════════════════════"
    (( all_started++ )) || true

    # --- 1. Reco-level with BDT cut ---
    run_step "getObservables_reco  [$ERA_NAME]"\
        $PYTHON scripts/getObservables_reco.py \
            --era           "$ERA_NAME" \
            --json_file     "$JSON_FILE" \
            --output_folder "$OUTPUT_FOLDER/reco_bdt" \
            --bdt_cut       "$BDT_CUT" \
            $SYST

    # --- 2-4. extractNs + getAs + makeDatacards (only if lumiXinfo exists) ---
    LUMI_FILE="Inputs/${INPUT_TAG}_BDTScore_${ERA_NAME}_lumiXinfo.json"
    if [[ -f "$LUMI_FILE" ]]; then
        log "lumiXinfo found — running extractNs + getAs + makeDatacards for $ERA_NAME"

        COUNTS_NAME="counts_${ERA_NAME}${SYST_SUFFIX}"
        run_step "extractNs  [$ERA_NAME]"\
            $PYTHON scripts/extractNs.py \
                --input_folder  "${OUTPUT_FOLDER}/reco_bdt" \
                --era           "$ERA_NAME" \
                --json_file     "$JSON_FILE" \
                --output_folder "$OUTPUT_FOLDER" \
                --output_name   "$COUNTS_NAME"

        AS_NAME="asymmetries_${ERA_NAME}${SYST_SUFFIX}"
        run_step "getAs  [$ERA_NAME]"\
            $PYTHON scripts/getAs.py \
                --counts_file    "${OUTPUT_FOLDER}/${COUNTS_NAME}.json" \
                --lumiXinfo_file "$LUMI_FILE" \
                --output_folder  "$OUTPUT_FOLDER" \
                --output_name    "$AS_NAME"

        # --- 4. Generate Combine datacards ---
        run_step "makeDatacards  [$ERA_NAME]"\
            $PYTHON scripts/makeDatacards.py \
                --counts_file    "${OUTPUT_FOLDER}/${COUNTS_NAME}.json" \
                --lumiXinfo_file "$LUMI_FILE" \
                --era            "$ERA_NAME" \
                --output_folder  "Datacards/${ERA_NAME}${SYST_SUFFIX}"
    else
        log "No lumiXinfo for $ERA_NAME — skipping extractNs/getAs/makeDatacards"
        log "  (expected: $LUMI_FILE)"
    fi

done

# ─── Summary ───────────────────────────────────────────────────────────────────
TOTAL_ELAPSED=$(( $(date +%s) - SCRIPT_T0 ))
echo ""
log "════════════════════════════════════════════════════════"
log "  run_all.sh finished"
log "  Eras processed : $all_started"
log "  Eras skipped   : $all_skipped  (no input JSON)"
log "  Total wall time: ${TOTAL_ELAPSED}s  ($(( TOTAL_ELAPSED/60 ))m $(( TOTAL_ELAPSED%60 ))s)"
log "  Outputs in     : $OUTPUT_FOLDER/"
log "  Datacards in   : Datacards/"
log "════════════════════════════════════════════════════════"

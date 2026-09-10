#!/bin/bash
# Chains the full 003-ObjectSelectionI -> II -> III production run, with the
# full ABCD data-driven QCD treatment, for UL2017 then UL2018 (Data_mu +
# MC_mu only -- MC_alt intentionally excluded), tag "earlySep".
#
# Waits for the already-running earlySep_UL2016postVFP screen job to finish
# first (sequential, not parallel -- avoids two CPU-heavy 003-II runs
# contending for the same 20 cores), then runs UL2017 end to end, then
# UL2018 end to end. Self-contained: sets up its own environment, so it can
# run unattended inside `screen` without a live Claude session.
#
# Mirrors exactly the same command sequence already verified for
# UL2016preVFP and (in progress) UL2016postVFP in this "earlySep" tag:
# jetPUID dropped from 003-II's ModuleList, full ABCD region-A + region-B +
# QCD-template treatment in 003-III.
#
# Preflight already done manually before launching this script (needs a
# live, already-authenticated SSH session to lxplus, which this script does
# NOT have when run unattended/detached):
#   - inputs/SFs/{UL2017,UL2018}_pu_Weights.json fetched (both were missing).
#   - All other SF files (mu_ID, mu_HLT, mu_Iso, jet_jmar, jet_Btagging) and
#     both golden JSONs were already present from earlier work.
#   - UL2017 already has b-tagging efficiency maps on disk (from an earlier
#     run); UL2018 does not. This script computes/refreshes them for BOTH
#     eras anyway (harmless, keeps the two eras' handling identical -- the
#     efficiency computation is dataset-level and cheap relative to the
#     rest of the pipeline).
#
# Usage: bash run_earlySep_UL2017_UL2018_fullABCD.sh
# (intended to be launched inside `screen`, see the accompanying screen -dmS
# invocation)

set -eo pipefail
# Deliberately no `-u` (nounset): conda's own activation hooks reference
# unset variables internally and would abort the script on `conda activate`.

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySep"
PRESELECTION_TAG="earlySep"
PRESELECTION_HASH="a3e6961ac5f1"
POSTVFP_STATUS="$REPO/run_logs/${TAG}_UL2016postVFP/STATUS.txt"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

CHAIN_STATUS="$REPO/run_logs/${TAG}_UL2017_UL2018_chain_STATUS.txt"
mkdir -p "$(dirname "$CHAIN_STATUS")"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')" > "$CHAIN_STATUS"
trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$CHAIN_STATUS"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$CHAIN_STATUS"; else echo "RESULT=FAILED" >> "$CHAIN_STATUS"; fi' EXIT

# --- environment setup (self-contained) ---
source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

get_hash() {
    # $1 = chapter dir name (relative to $REPO)
    local dir="$1"
    ( cd "$REPO/$dir" && python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1 \
        | grep "Config hash:" | awk '{print $NF}' )
}

run_era_pipeline() {
    local ERA="$1"
    local LOGDIR="$REPO/run_logs/${TAG}_${ERA}"
    mkdir -p "$LOGDIR"
    local STATUS_FILE="$LOGDIR/STATUS.txt"
    echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')" > "$STATUS_FILE"

    # Local trap scoped to this function call via a subshell would lose the
    # calling script's own EXIT trap semantics, so record status explicitly
    # around each stage instead of relying on a nested trap.

    ############################################################################
    # 003-ObjectSelectionI
    ############################################################################
    log "=== 003-ObjectSelectionI: $ERA ==="
    cd "$REPO/003-ObjectSelectionI"

    python3 scripts/run_all.py --tag "$TAG" --generatePreselectionDatasetJSON \
        --preselectionTag "$PRESELECTION_TAG" --preselectionHash "$PRESELECTION_HASH" \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

    python3 scripts/run_all.py --tag "$TAG" --downloadGoldenJSONs \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

    python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

    python3 scripts/run_all.py --tag "$TAG" --writeBashScript \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

    log "[$ERA] Running 003-I object-selection skim production (full stats, no --sample)..."
    bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003I_production.log"

    python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

    python3 scripts/run_all.py --tag "$TAG" --verifyOutput \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

    local I_HASH
    I_HASH="$(get_hash 003-ObjectSelectionI)"
    log "[$ERA] 003-ObjectSelectionI config hash: $I_HASH"
    echo "$I_HASH" > "$LOGDIR/I_HASH.txt"

    ############################################################################
    # 003-ObjectSelectionII
    ############################################################################
    log "=== 003-ObjectSelectionII: $ERA ==="
    cd "$REPO/003-ObjectSelectionII"

    python3 scripts/run_all.py --tag "$TAG" --generateSelectionIDatasetJSON \
        --selectionITag "$TAG" --selectionIHash "$I_HASH" \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

    python3 scripts/run_all.py --tag "$TAG" --prepareEfficiencyFileset \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

    log "[$ERA] Computing/refreshing b-tagging efficiency maps..."
    python3 scripts/run_all.py --tag "$TAG" --computeBTaggingEfficiency \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

    python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

    python3 scripts/run_all.py --tag "$TAG" --writeBashScript \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

    log "[$ERA] Running 003-II SF-weighted skim production (heaviest step)..."
    bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003II_production.log"

    python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

    log "[$ERA] Computing data-driven ABCD scale factor (transfer factor R)..."
    python3 scripts/run_all.py --tag "$TAG" --computeABCDScaleFactor \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

    local II_HASH
    II_HASH="$(get_hash 003-ObjectSelectionII)"
    log "[$ERA] 003-ObjectSelectionII config hash: $II_HASH"
    echo "$II_HASH" > "$LOGDIR/II_HASH.txt"

    ############################################################################
    # 003-ObjectSelectionIII -- full ABCD treatment through to the plots
    ############################################################################
    log "=== 003-ObjectSelectionIII: $ERA (full ABCD) ==="
    cd "$REPO/003-ObjectSelectionIII"

    python3 scripts/run_all.py --tag "$TAG" --generateSelectionIIDatasetJSON \
        --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

    python3 scripts/run_all.py --tag "$TAG" --fetchABCDScaleFactor \
        --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
        --filter "$ERA" --force 2>&1 | tee -a "$LOGDIR/003III_steps.log"

    log "[$ERA] Building region-A (nominal) histograms..."
    python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 0 \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"

    python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 0 \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"

    log "[$ERA] Building region-B (R-weighted) histograms for the ABCD QCD template..."
    python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"

    python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 1 \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"

    python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

    python3 scripts/run_all.py --tag "$TAG" --makeplots --force \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

    local III_HASH
    III_HASH="$(get_hash 003-ObjectSelectionIII)"
    log "[$ERA] 003-ObjectSelectionIII config hash: $III_HASH"
    echo "$III_HASH" > "$LOGDIR/III_HASH.txt"

    local REPORT_PDF="$REPO/003-ObjectSelectionIII/outputs/${TAG}/${III_HASH}/${TAG}_${III_HASH}_report.pdf"
    log "=== [$ERA] DONE. Final PDF (full ABCD treatment): $REPORT_PDF ==="
    echo "REPORT_PDF=$REPORT_PDF" >> "$STATUS_FILE"
    echo "RESULT=SUCCESS" >> "$STATUS_FILE"
    echo "FINISHED $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
}

################################################################################
# Wait for the already-running UL2016postVFP job to finish before starting.
################################################################################
log "Waiting for earlySep_UL2016postVFP to finish before starting UL2017..."
while [ ! -f "$POSTVFP_STATUS" ] || ! grep -q "^RESULT=" "$POSTVFP_STATUS"; do
    sleep 60
done
log "UL2016postVFP finished ($(grep '^RESULT=' "$POSTVFP_STATUS")). Proceeding."

################################################################################
# UL2017, then UL2018
################################################################################
run_era_pipeline "UL2017"
log ">>> UL2017 complete. Starting UL2018. <<<"
run_era_pipeline "UL2018"
log ">>> UL2018 complete. Both eras done. <<<"

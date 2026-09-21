#!/bin/bash
# Full 003-ObjectSelectionI -> II -> III production run, with the full ABCD
# data-driven QCD treatment, for era UL2016postVFP (Data_mu + MC_mu), tag
# "earlySep". Self-contained: sets up its own environment, so it can be run
# unattended (e.g. inside `screen`) without an interactive Claude session.
#
# Mirrors exactly the command sequence already run and verified for
# UL2016preVFP in this same "earlySep" tag (003-I hash a809db6c1a10, 003-II
# hash 778350fe53e4 with jetPUID dropped from ModuleList, 003-III hash
# 52637ffebecd with full ABCD region-A + region-B + QCD template treatment).
#
# Preflight already done manually before launching this script (needs a live,
# already-authenticated SSH session to lxplus, which this script does NOT
# have when run unattended/detached):
#   - inputs/SFs/UL2016postVFP_pu_Weights.json fetched (was missing, same gap
#     as UL2016preVFP had).
#   - All other UL2016postVFP SF files (mu_ID, mu_HLT, mu_Iso, jet_jmar,
#     jet_Btagging) and the golden JSON were already present from earlier work.
#   - b-tagging efficiency maps for UL2016postVFP do NOT exist yet -- this
#     script computes them itself (needs 003-I's UL2016postVFP skims to exist
#     first, which is why it's sequenced after the 003-I skim step below).
#
# Usage: bash run_earlySep_UL2016postVFP_fullABCD.sh
# (intended to be launched inside `screen`, see the accompanying screen -dmS
# invocation)

# Deliberately no `-u` (nounset): conda's own activation hooks (e.g.
# activate-binutils_linux-64.sh) reference unset variables internally and
# would abort the script the moment we `conda activate`.
set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySep"
ERA="UL2016postVFP"
PRESELECTION_TAG="earlySep"
PRESELECTION_HASH="a3e6961ac5f1"
LOGDIR="$REPO/run_logs/${TAG}_${ERA}"

mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')" > "$STATUS_FILE"

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; else echo "RESULT=FAILED" >> "$STATUS_FILE"; fi' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# --- environment setup (self-contained: do not rely on an already-activated shell) ---
source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

get_hash() {
    # $1 = chapter dir name (relative to $REPO), remaining args passed through to run_all.py
    local dir="$1"; shift
    ( cd "$REPO/$dir" && python3 scripts/run_all.py --tag "$TAG" --printHash "$@" 2>&1 \
        | grep "Config hash:" | awk '{print $NF}' )
}

################################################################################
# 003-ObjectSelectionI
################################################################################
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

log "Running 003-I object-selection skim production (full stats, no --sample)..."
bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003I_production.log"

python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

python3 scripts/run_all.py --tag "$TAG" --verifyOutput \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

I_HASH="$(get_hash 003-ObjectSelectionI)"
log "003-ObjectSelectionI config hash: $I_HASH"
echo "$I_HASH" > "$LOGDIR/I_HASH.txt"

################################################################################
# 003-ObjectSelectionII
################################################################################
log "=== 003-ObjectSelectionII: $ERA ==="
cd "$REPO/003-ObjectSelectionII"

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIDatasetJSON \
    --selectionITag "$TAG" --selectionIHash "$I_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

python3 scripts/run_all.py --tag "$TAG" --prepareEfficiencyFileset \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

log "Computing b-tagging efficiency maps for $ERA (not yet on disk)..."
python3 scripts/run_all.py --tag "$TAG" --computeBTaggingEfficiency \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

python3 scripts/run_all.py --tag "$TAG" --writeBashScript \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

log "Running 003-II SF-weighted skim production (heaviest step -- ~50min for the equivalent UL2016preVFP run)..."
bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003II_production.log"

python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

log "Computing data-driven ABCD scale factor (transfer factor R)..."
python3 scripts/run_all.py --tag "$TAG" --computeABCDScaleFactor \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

II_HASH="$(get_hash 003-ObjectSelectionII)"
log "003-ObjectSelectionII config hash: $II_HASH"
echo "$II_HASH" > "$LOGDIR/II_HASH.txt"

################################################################################
# 003-ObjectSelectionIII -- full ABCD treatment through to the plots
################################################################################
log "=== 003-ObjectSelectionIII: $ERA (full ABCD) ==="
cd "$REPO/003-ObjectSelectionIII"

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIIDatasetJSON \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

python3 scripts/run_all.py --tag "$TAG" --fetchABCDScaleFactor \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" --force 2>&1 | tee -a "$LOGDIR/003III_steps.log"

log "Building region-A (nominal) histograms..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 0 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"

python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 0 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"

log "Building region-B (R-weighted) histograms for the ABCD QCD template..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"

python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 1 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"

python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

python3 scripts/run_all.py --tag "$TAG" --makeplots --force \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

III_HASH="$(get_hash 003-ObjectSelectionIII)"
log "003-ObjectSelectionIII config hash: $III_HASH"
echo "$III_HASH" > "$LOGDIR/III_HASH.txt"

REPORT_PDF="$REPO/003-ObjectSelectionIII/outputs/${TAG}/${III_HASH}/${TAG}_${III_HASH}_report.pdf"
log "=== DONE. Final PDF (full ABCD treatment): $REPORT_PDF ==="
echo "REPORT_PDF=$REPORT_PDF" >> "$STATUS_FILE"

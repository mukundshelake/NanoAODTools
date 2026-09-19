#!/bin/bash
# Full (non-sample) 003-ObjectSelectionI -> II -> III production run for era
# UL2016preVFP (Data_mu + MC_mu), tag "earlySepetember_correted", full ABCD
# treatment, exercising EVERYTHING added this session:
#   - additional-lepton veto (003-I SelectionCuts)
#   - METXYCorr (ModuleList, MC+Data)
#   - jetVetoMap, JetJER, MuonRochester (PreSelectionCorrectionModuleList,
#     the early pre-selection-cut pass -- --applyPreSelectionCorrections)
#   - TopPtWeight (003-II ModuleList)
#   - weight-only systematic band (--systematics on region-A buildSelectionHists:
#     bTag, muonID, muonHLT, muonIso, puWeight, L1PreFiring)
# on top of the pre-existing full ABCD data-driven QCD estimate (region A/B,
# QCD template, final plots).
#
# Sends a Telegram progress message (scripts/send_telegram.py) after each
# major stage, and on failure.
#
# Usage: bash run_earlySepetember_correted_UL2016preVFP_fullABCD.sh

set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySepetember_correted"
ERA="UL2016preVFP"
PRESELECTION_TAG="earlySeptember"
PRESELECTION_HASH="f7b250452aef"
LOGDIR="$REPO/run_logs/${TAG}_${ERA}"

mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')" > "$STATUS_FILE"

# send_telegram.py needs requests+dotenv, which live in the base conda env,
# not latestcoffea -- capture the base env's python3 now, before
# `conda activate latestcoffea` below shadows it.
TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"

notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG/$ERA full] $1" >/dev/null 2>&1 || true
}

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# --- environment setup (self-contained: do not rely on an already-activated shell) ---
source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

get_hash() {
    local dir="$1"; shift
    ( cd "$REPO/$dir" && python3 scripts/run_all.py --tag "$TAG" --printHash "$@" 2>&1 \
        | grep "Config hash:" | awk '{print $NF}' )
}

notify "Starting FULL run (full ABCD + all corrections + weight-only systematic band)."

################################################################################
# 003-ObjectSelectionI
################################################################################
log "=== 003-ObjectSelectionI: $ERA (full) ==="
cd "$REPO/003-ObjectSelectionI"

python3 scripts/run_all.py --tag "$TAG" --generatePreselectionDatasetJSON \
    --preselectionTag "$PRESELECTION_TAG" --preselectionHash "$PRESELECTION_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

python3 scripts/run_all.py --tag "$TAG" --downloadGoldenJSONs \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

log "Running pre-selection corrections pass (jetVetoMap/jetJER/muonRochester, full stats)..."
python3 scripts/run_all.py --tag "$TAG" --applyPreSelectionCorrections \
    --filter "$ERA" --workers 15 2>&1 | tee -a "$LOGDIR/003I_precorr.log"
notify "003-I: pre-selection corrections pass done."

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
notify "003-I done (hash $I_HASH). Skims + verifyOutput complete."

################################################################################
# 003-ObjectSelectionII
################################################################################
log "=== 003-ObjectSelectionII: $ERA (full) ==="
cd "$REPO/003-ObjectSelectionII"

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIDatasetJSON \
    --selectionITag "$TAG" --selectionIHash "$I_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

log "Reusing existing UL2016preVFP b-tagging/jetPUID efficiency maps (not recomputed for this run)."

python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

python3 scripts/run_all.py --tag "$TAG" --writeBashScript \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

log "Running 003-II SF-weighted skim production (heaviest step; includes topPtWeight)..."
bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003II_production.log"

python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

log "Computing data-driven ABCD scale factor (transfer factor R)..."
python3 scripts/run_all.py --tag "$TAG" --computeABCDScaleFactor \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

II_HASH="$(get_hash 003-ObjectSelectionII)"
log "003-ObjectSelectionII config hash: $II_HASH"
echo "$II_HASH" > "$LOGDIR/II_HASH.txt"
notify "003-II done (hash $II_HASH). SF-weighted skims + ABCD scale factor complete."

################################################################################
# 003-ObjectSelectionIII -- full ABCD treatment + weight-only systematic band
################################################################################
log "=== 003-ObjectSelectionIII: $ERA (full ABCD + systematics) ==="
cd "$REPO/003-ObjectSelectionIII"

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIIDatasetJSON \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

python3 scripts/run_all.py --tag "$TAG" --fetchABCDScaleFactor \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" --force 2>&1 | tee -a "$LOGDIR/003III_steps.log"

log "Building region-A (nominal) histograms WITH --systematics (weight-only band: bTag/muonID/muonHLT/muonIso/puWeight/L1PreFiring)..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 0 --systematics \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"

python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 0 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"
notify "003-III: region-A histograms (with weight-only systematics) done."

log "Building region-B (R-weighted) histograms for the ABCD QCD template (no --systematics -- QCD template pipeline doesn't consume weight variants)..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"

python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 1 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"

python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
notify "003-III: region-B histograms + QCD template done."

python3 scripts/run_all.py --tag "$TAG" --makeplots --force \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

III_HASH="$(get_hash 003-ObjectSelectionIII)"
log "003-ObjectSelectionIII config hash: $III_HASH"
echo "$III_HASH" > "$LOGDIR/III_HASH.txt"

REPORT_PDF="$REPO/003-ObjectSelectionIII/outputs/${TAG}/${III_HASH}/${TAG}_${III_HASH}_report.pdf"
log "=== DONE. Final PDF (full ABCD + all corrections + systematic band): $REPORT_PDF ==="
echo "REPORT_PDF=$REPORT_PDF" >> "$STATUS_FILE"
notify "003-III: plots + PDF done. Report: $REPORT_PDF"

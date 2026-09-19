#!/bin/bash
# 003-ObjectSelectionIII only, --sample, for the already-completed
# earlySepetember_correted/UL2016postVFP run -- 003-I/003-II config hasn't
# changed since that run (verified: --printHash gives the exact same
# I_HASH=daebceb13352/II_HASH=37f710b13012), so this reuses those skims as-is
# and only re-runs 003-III fresh (new hash, since 003-III's config.yaml
# gained the weightSystematics block) with --systematics on, to validate the
# weight-only systematic band end-to-end on a real (sample-scale) plot.
#
# Sends a Telegram progress message after each major stage, and on failure.
#
# Usage: bash run_earlySepetember_correted_UL2016postVFP_sample_syst.sh

set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySepetember_correted"
ERA="UL2016postVFP"
I_HASH="daebceb13352"
II_HASH="37f710b13012"
LOGDIR="$REPO/run_logs/${TAG}_${ERA}_sample_syst"

mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')" > "$STATUS_FILE"

TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG/$ERA sample+syst] $1" >/dev/null 2>&1 || true
}

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

get_hash() {
    ( cd "$REPO/003-ObjectSelectionIII" && python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1 \
        | grep "Config hash:" | awk '{print $NF}' )
}

notify "Starting 003-III-only sample run with --systematics (reusing existing 003-I/II skims)."

cd "$REPO/003-ObjectSelectionIII"

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIIDatasetJSON \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/steps.log"

python3 scripts/run_all.py --tag "$TAG" --fetchABCDScaleFactor \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" --force 2>&1 | tee -a "$LOGDIR/steps.log"

log "Building region-A (nominal) histograms with --systematics (--sample)..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 0 --sample --systematics \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/regionA.log"

python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 0 --sample \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/regionA.log"
notify "003-III: region-A histograms (with systematics) done."

log "Building region-B (R-weighted) histograms for the ABCD QCD template (--sample; no --systematics -- QCD template pipeline doesn't consume weight variants)..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 --sample \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/regionB.log"

python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 1 --sample \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/regionB.log"

python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate --sample \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/steps.log"
notify "003-III: region-B histograms + QCD template done."

python3 scripts/run_all.py --tag "$TAG" --makeplots --sample --force \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/steps.log"

III_HASH="$(get_hash)"
log "003-ObjectSelectionIII config hash: $III_HASH"
echo "$III_HASH" > "$LOGDIR/III_HASH.txt"

REPORT_PDF="$REPO/003-ObjectSelectionIII/outputs/${TAG}/${III_HASH}/${TAG}_${III_HASH}_report.pdf"
log "=== DONE (sample+systematics run). Report: $REPORT_PDF ==="
echo "REPORT_PDF=$REPORT_PDF" >> "$STATUS_FILE"
notify "003-III: plots + PDF done. Report: $REPORT_PDF"

#!/bin/bash
# 003-ObjectSelectionI only (pre-selection corrections pass + object-selection
# skims), UL2017, tag earlySeptember_corrected -- the first stage of the full
# corrected 003 chain for UL2017 (none existed; the old earlySeptember/
# 778350fe53e4 UL2017 outputs predate the correction pass).
# Same 003-I steps as run_earlySepetember_correted_UL2016preVFP_fullABCD.sh,
# reading preselection earlySeptember/f7b250452aef. Data_mu + MC_mu only: MC_alt is
# not needed for this analysis (UL2017/UL2018 preselection has it, 2016 does not).
# Expected 003-I hash: daebceb13352 (same as 2016 eras).
#
# Usage: bash run_earlySeptember_corrected_UL2017_003I.sh

set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySeptember_corrected"
ERA="UL2017"
PRESELECTION_TAG="earlySeptember"
PRESELECTION_HASH="f7b250452aef"
WORKERS=15
LOGDIR="$REPO/run_logs/${TAG}_${ERA}_003I"

mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')" > "$STATUS_FILE"

TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG/$ERA 003-I] $1" >/dev/null 2>&1 || true
}

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

cd "$REPO/003-ObjectSelectionI"
I_HASH="$(python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1 | grep "Config hash:" | awk '{print $NF}')"
[ -n "$I_HASH" ] || { echo "Could not compute the 003-I config hash" >&2; exit 1; }
echo "I_HASH=$I_HASH" >> "$STATUS_FILE"
log "=== 003-ObjectSelectionI: $ERA, hash $I_HASH, $WORKERS workers ==="
notify "Starting (003-I hash $I_HASH, $WORKERS workers)."

python3 scripts/run_all.py --tag "$TAG" --generatePreselectionDatasetJSON \
    --preselectionTag "$PRESELECTION_TAG" --preselectionHash "$PRESELECTION_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

python3 scripts/run_all.py --tag "$TAG" --downloadGoldenJSONs \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

log "Pre-selection corrections pass (jetVetoMap/jetJER/muonRochester)..."
python3 scripts/run_all.py --tag "$TAG" --applyPreSelectionCorrections \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" --workers "$WORKERS" 2>&1 | tee -a "$LOGDIR/003I_precorr.log"
notify "Pre-selection corrections pass done."
echo "PRECORR_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"

python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

python3 scripts/run_all.py --tag "$TAG" --writeBashScript --workers "$WORKERS" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
cp -p "scripts/run_all_${TAG}.sh" "$LOGDIR/run_all_${TAG}_${ERA}.sh"

log "003-I object-selection skim production..."
bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003I_production.log"

python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

python3 scripts/run_all.py --tag "$TAG" --verifyOutput --workers "$WORKERS" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

echo "$I_HASH" > "$LOGDIR/I_HASH.txt"
log "=== DONE: 003-I $ERA hash $I_HASH ==="

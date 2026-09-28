#!/bin/bash
# 003-ObjectSelectionIII only, UL2016preVFP, for whatever config.yaml is
# committed (logs go to run_logs/<tag>_<era>_003III_<hash>). Reads the existing 003-II output, which for
# preVFP lives under the typo tag earlySepetember_correted (hash 37f710b13012),
# and writes 003-III output under the correctly spelled tag.
# Same steps as the 003-III section of run_earlySepetember_correted_UL2016preVFP_fullABCD.sh.
#
# Usage: bash run_earlySeptember_corrected_UL2016preVFP_003III_rebin.sh

set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySeptember_corrected"
II_TAG="earlySepetember_correted"
II_HASH="37f710b13012"
ERA="UL2016preVFP"
source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

cd "$REPO/003-ObjectSelectionIII"
III_HASH="$(python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1 | grep "Config hash:" | awk '{print $NF}')"
[ -n "$III_HASH" ] || { echo "Could not compute the 003-III config hash" >&2; exit 1; }
LOGDIR="$REPO/run_logs/${TAG}_${ERA}_003III_${III_HASH}"

mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')" > "$STATUS_FILE"

TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG/$ERA 003-III rebin] $1" >/dev/null 2>&1 || true
}

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

echo "III_HASH=$III_HASH" >> "$STATUS_FILE"
log "=== 003-ObjectSelectionIII: $ERA, hash $III_HASH ==="
notify "Starting (003-III hash $III_HASH)."

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIIDatasetJSON \
    --selectionIITag "$II_TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

python3 scripts/run_all.py --tag "$TAG" --fetchABCDScaleFactor \
    --selectionIITag "$II_TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" --force 2>&1 | tee -a "$LOGDIR/003III_steps.log"

log "Region A with --systematics..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 0 --systematics \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"
python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 0 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"
notify "Region A done."
echo "REGION_A_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"

log "Region B (R-weighted) + QCD template..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"
python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 1 \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"
python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
notify "Region B + QCD template done."
echo "REGION_B_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"

python3 scripts/run_all.py --tag "$TAG" --makeplots --force \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

echo "$III_HASH" > "$LOGDIR/III_HASH.txt"
log "=== DONE: $REPO/003-ObjectSelectionIII/outputs/$TAG/$III_HASH/$ERA/plots/ ==="

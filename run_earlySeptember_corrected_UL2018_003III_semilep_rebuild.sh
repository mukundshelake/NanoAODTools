#!/bin/bash
# Rebuild UL2018 003-III after removing the max_chunks(..., 300) cap from
# buildSelectionHists.py. ttbar_SemiLeptonic has 391 files, so the first build
# filled only its first 300 (76.7% of region A) while normalising to the full
# Ngen, giving Data/MC = 1.163. Only that dataset exceeded the cap, so only it
# is rebuilt (regions A and B); aggregation, the QCD template (which subtracts
# MC in region B) and the plots are then redone for the whole era.
#
# Usage: bash run_earlySeptember_corrected_UL2018_003III_semilep_rebuild.sh

set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySeptember_corrected"
ERA="UL2018"
EXPECTED_III_HASH="aaf38cf077ed"
LOGDIR="$REPO/run_logs/${TAG}_${ERA}_003III_semilep_rebuild"

mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')" > "$STATUS_FILE"

TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG/$ERA 003-III SemiLep rebuild] $1" >/dev/null 2>&1 || true
}
trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

cd "$REPO/003-ObjectSelectionIII"
III_HASH="$(python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1 | grep "Config hash:" | awk '{print $NF}')"
echo "III_HASH=$III_HASH" >> "$STATUS_FILE"
[ "$III_HASH" = "$EXPECTED_III_HASH" ] || { echo "003-III hash $III_HASH != $EXPECTED_III_HASH" >&2; exit 1; }
grep -q "max_chunks" scripts/buildSelectionHists.py && { echo "max_chunks cap still present" >&2; exit 1; }
notify "Starting."

log "Rebuilding ttbar_SemiLeptonic, regions A (with systematics) and B, in parallel..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 0 --systematics --force \
    --filter "$ERA/MC_mu/SemiLeptonic" > "$LOGDIR/003III_regionA.log" 2>&1 &
PID_A=$!
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 --force \
    --filter "$ERA/MC_mu/SemiLeptonic" > "$LOGDIR/003III_regionB.log" 2>&1 &
PID_B=$!
wait $PID_A
wait $PID_B
echo "SEMILEP_REBUILT $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"

for region in 0 1; do
    python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter $region --force \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
done
python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate --force \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
python3 scripts/run_all.py --tag "$TAG" --makeplots --force \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

log "=== DONE: $REPO/003-ObjectSelectionIII/outputs/$TAG/$III_HASH/$ERA/plots/ ==="

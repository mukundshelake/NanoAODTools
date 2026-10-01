#!/bin/bash
# 003-ObjectSelectionII -> 003-ObjectSelectionIII, UL2017, tag earlySeptember_corrected.
# Continues run_earlySeptember_corrected_UL2017_003I.sh (003-I hash daebceb13352).
# Same II/III steps as run_earlySepetember_correted_UL2016preVFP_fullABCD.sh, but
# restricted to Data_mu + MC_mu (MC_alt is not processed).
# Aborts unless the hashes match the 2016 corrected chain (II 37f710b13012,
# III aaf38cf077ed), so all eras share one config.
#
# Usage: bash run_earlySeptember_corrected_UL2017_003II_III.sh

set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySeptember_corrected"
ERA="UL2017"
I_HASH="daebceb13352"
EXPECTED_II_HASH="37f710b13012"
EXPECTED_III_HASH="aaf38cf077ed"
WORKERS=15
LOGDIR="$REPO/run_logs/${TAG}_${ERA}_003II_III"

mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')" > "$STATUS_FILE"

TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG/$ERA 003-II/III] $1" >/dev/null 2>&1 || true
}

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

get_hash() {
    ( cd "$REPO/$1" && python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1 \
        | grep "Config hash:" | awk '{print $NF}' )
}

II_HASH="$(get_hash 003-ObjectSelectionII)"
III_HASH="$(get_hash 003-ObjectSelectionIII)"
echo "II_HASH=$II_HASH" >> "$STATUS_FILE"
echo "III_HASH=$III_HASH" >> "$STATUS_FILE"
[ "$II_HASH" = "$EXPECTED_II_HASH" ] || { echo "003-II hash $II_HASH != $EXPECTED_II_HASH" >&2; exit 1; }
[ "$III_HASH" = "$EXPECTED_III_HASH" ] || { echo "003-III hash $III_HASH != $EXPECTED_III_HASH" >&2; exit 1; }

notify "Starting (II $II_HASH, III $III_HASH, $WORKERS workers)."

################################################################################
# 003-ObjectSelectionII
################################################################################
log "=== 003-ObjectSelectionII: $ERA, hash $II_HASH ==="
cd "$REPO/003-ObjectSelectionII"

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIDatasetJSON \
    --selectionITag "$TAG" --selectionIHash "$I_HASH" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

log "Reusing existing UL2017 b-tagging efficiency maps (inputs/SFs/Efficiency/UL2017)."

python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

python3 scripts/run_all.py --tag "$TAG" --writeBashScript --workers "$WORKERS" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
cp -p "scripts/run_all_${TAG}.sh" "$LOGDIR/run_all_${TAG}_${ERA}_003II.sh"

log "003-II SF-weighted skim production..."
bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003II_production.log"

python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

log "Computing data-driven ABCD scale factor (transfer factor R)..."
python3 scripts/run_all.py --tag "$TAG" --computeABCDScaleFactor \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

notify "003-II done (hash $II_HASH)."
echo "II_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"

################################################################################
# 003-ObjectSelectionIII
################################################################################
log "=== 003-ObjectSelectionIII: $ERA, hash $III_HASH ==="
cd "$REPO/003-ObjectSelectionIII"

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIIDatasetJSON \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

python3 scripts/run_all.py --tag "$TAG" --fetchABCDScaleFactor \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" --force 2>&1 | tee -a "$LOGDIR/003III_steps.log"

log "Region A with --systematics..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 0 --systematics --force \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"
python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 0 --force \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"
notify "003-III region A done."
echo "REGION_A_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"

log "Region B (R-weighted) + QCD template..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 --force \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"
python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 1 --force \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"
python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
notify "003-III region B + QCD template done."
echo "REGION_B_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"

python3 scripts/run_all.py --tag "$TAG" --makeplots --force \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

log "=== DONE: $REPO/003-ObjectSelectionIII/outputs/$TAG/$III_HASH/$ERA/plots/ ==="

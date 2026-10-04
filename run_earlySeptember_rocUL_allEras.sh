#!/bin/bash
# Full 003-I -> II -> III reprocessing of all four eras with the UL Rochester
# tables (roccor.Run2.v5: RoccoR2016aUL/2016bUL/2017UL/2018UL, see
# python/postprocessing/data/roccor.Run2.v5/PROVENANCE), replacing the legacy
# pre-UL roccor.Run2.v3 tables used by earlySeptember_corrected.
#
# New tag, not earlySeptember_corrected: config hashes are per chapter, and only
# 003-I's config changed. Under the old tag, 003-II/III would land in the existing
# 37f710b13012 / aaf38cf077ed directories and either skip (stale v3 files kept) or
# overwrite what 004A/Appendix-B were built from. A new tag keeps both productions intact.
#
# Steps per era are those of run_earlySeptember_corrected_UL2018_003I.sh and
# run_earlySeptember_corrected_UL2018_003II_III.sh (Data_mu + MC_mu only, never MC_alt;
# existing b-tag efficiency maps reused; 003-III region A with --systematics, region B,
# QCD template, plots). Eras run one after another; the chain stops at the first failure.
#
# Launch detached:  setsid nohup ./run_earlySeptember_rocUL_allEras.sh > run_logs/earlySeptember_rocUL_chain.out 2>&1 &
set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySeptember_rocUL"
PRESELECTION_TAG="earlySeptember"
PRESELECTION_HASH="f7b250452aef"
ERAS=(UL2018 UL2017 UL2016postVFP UL2016preVFP)
EXPECTED_I_HASH="9f3bc832d268"
EXPECTED_II_HASH="37f710b13012"
EXPECTED_III_HASH="aaf38cf077ed"
WORKERS=15

CHAINLOG="$REPO/run_logs/${TAG}_chain"
mkdir -p "$CHAINLOG"
CHAIN_STATUS="$CHAINLOG/STATUS.txt"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S') eras=${ERAS[*]}" > "$CHAIN_STATUS"

TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG] $1" >/dev/null 2>&1 || true
}
trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$CHAIN_STATUS"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$CHAIN_STATUS"; notify "All eras DONE (success)."; else echo "RESULT=FAILED" >> "$CHAIN_STATUS"; notify "FAILED at line $LINENO (exit $ec). See $CHAINLOG"; fi' EXIT
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

get_hash() {
    ( cd "$REPO/$1" && python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1 \
        | grep "Config hash:" | awk '{print $NF}' )
}
I_HASH="$(get_hash 003-ObjectSelectionI)"
II_HASH="$(get_hash 003-ObjectSelectionII)"
III_HASH="$(get_hash 003-ObjectSelectionIII)"
echo "I_HASH=$I_HASH II_HASH=$II_HASH III_HASH=$III_HASH" >> "$CHAIN_STATUS"
[ "$I_HASH" = "$EXPECTED_I_HASH" ]     || { echo "003-I hash $I_HASH != $EXPECTED_I_HASH" >&2; exit 1; }
[ "$II_HASH" = "$EXPECTED_II_HASH" ]   || { echo "003-II hash $II_HASH != $EXPECTED_II_HASH" >&2; exit 1; }
[ "$III_HASH" = "$EXPECTED_III_HASH" ] || { echo "003-III hash $III_HASH != $EXPECTED_III_HASH" >&2; exit 1; }
notify "Starting ${ERAS[*]} (I $I_HASH, II $II_HASH, III $III_HASH, $WORKERS workers)."

for ERA in "${ERAS[@]}"; do
    LOGDIR="$REPO/run_logs/${TAG}_${ERA}"
    mkdir -p "$LOGDIR"
    STATUS_FILE="$LOGDIR/STATUS.txt"
    echo "STARTED $(date '+%Y-%m-%d %H:%M:%S') I=$I_HASH II=$II_HASH III=$III_HASH" > "$STATUS_FILE"
    echo "$ERA STARTED $(date '+%Y-%m-%d %H:%M:%S')" >> "$CHAIN_STATUS"

    # ------------------------------ 003-I ------------------------------
    log "=== $ERA 003-ObjectSelectionI, hash $I_HASH ==="
    cd "$REPO/003-ObjectSelectionI"
    python3 scripts/run_all.py --tag "$TAG" --generatePreselectionDatasetJSON \
        --preselectionTag "$PRESELECTION_TAG" --preselectionHash "$PRESELECTION_HASH" \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --downloadGoldenJSONs \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    log "Pre-selection corrections pass (jetVetoMap/jetJER/muonRochester UL)..."
    python3 scripts/run_all.py --tag "$TAG" --applyPreSelectionCorrections \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" --workers "$WORKERS" 2>&1 | tee -a "$LOGDIR/003I_precorr.log"
    echo "PRECORR_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --writeBashScript --workers "$WORKERS" \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    cp -p "scripts/run_all_${TAG}.sh" "$LOGDIR/run_all_${TAG}_${ERA}_003I.sh"
    log "003-I object-selection skim production..."
    bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003I_production.log"
    python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --verifyOutput --workers "$WORKERS" \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    echo "I_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    notify "$ERA 003-I done."

    # ------------------------------ 003-II ------------------------------
    log "=== $ERA 003-ObjectSelectionII, hash $II_HASH ==="
    cd "$REPO/003-ObjectSelectionII"
    python3 scripts/run_all.py --tag "$TAG" --generateSelectionIDatasetJSON \
        --selectionITag "$TAG" --selectionIHash "$I_HASH" \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
    log "Reusing existing $ERA b-tagging efficiency maps (inputs/SFs/Efficiency/$ERA)."
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
    echo "II_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    notify "$ERA 003-II done."

    # ------------------------------ 003-III ------------------------------
    log "=== $ERA 003-ObjectSelectionIII, hash $III_HASH ==="
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
    echo "REGION_A_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    log "Region B (R-weighted) + QCD template..."
    python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 --force \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"
    python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 1 --force \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"
    python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
    echo "REGION_B_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    python3 scripts/run_all.py --tag "$TAG" --makeplots --force \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
    echo "RESULT=SUCCESS $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    echo "$ERA DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$CHAIN_STATUS"
    notify "$ERA complete (003-I/II/III)."
    log "=== $ERA DONE ==="
done

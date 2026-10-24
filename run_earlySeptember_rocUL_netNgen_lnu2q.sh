#!/bin/bash
# earlySeptember_rocUL after ef58e7b (aMC@NLO Ngen -> net event counts, new
# WWTo1L1Nu2Q / WZTo1L1Nu2Q diboson samples, UL2017 WZTo2Q2L xsec fix).
#
# ef58e7b only touched NgenandXsec, which 003-I/II never use per event, but the
# config hash is the raw config.yaml bytes, so all three 003 hashes moved:
#   003-I   9f3bc832d268 -> 56e1170feb60
#   003-II  37f710b13012 -> 1883383068e0
#   003-III aaf38cf077ed -> 0ed415a7e727
# Before this script, the existing per-event outputs were hard-linked (cp -al,
# originals untouched) into the new hash dirs:
#   preSelectionCorrected, selectionI: 9f3bc832d268 -> 56e1170feb60
#   selectionII:                       37f710b13012 -> 1883383068e0
# so 003-I/II only process the two new datasets (every step skips existing
# outputs per file). Everything normalisation-dependent is then redone for all
# datasets: the ABCD transfer factor R, 003-III region A (rebuilt, not reused)
# and region B, the QCD template and the plots. b-tag efficiency maps are
# computed for the two new datasets only (existing maps untouched).
#
# Eras run serially (run_all.py --writeBashScript is per tag). Data_mu + MC_mu
# only. Launch detached:
#   setsid nohup ./run_earlySeptember_rocUL_netNgen_lnu2q.sh > run_logs/earlySeptember_rocUL_netNgen_chain.out 2>&1 &
set -eo pipefail

REPO="/home/mukund/Projects/PhysicsTools/NanoAODTools"
TAG="earlySeptember_rocUL"
PRESELECTION_TAG="earlySeptember"
PRESELECTION_HASH="f7b250452aef"
ERAS=(UL2016preVFP UL2016postVFP UL2017 UL2018)
NEW_DATASETS=(WWTo1L1Nu2Q WZTo1L1Nu2Q)
EXPECTED_I_HASH="56e1170feb60"
EXPECTED_II_HASH="1883383068e0"
EXPECTED_III_HASH="0ed415a7e727"
WORKERS=12

CHAINLOG="$REPO/run_logs/${TAG}_netNgen_chain"
mkdir -p "$CHAINLOG"
CHAIN_STATUS="$CHAINLOG/STATUS.txt"
echo "STARTED $(date '+%Y-%m-%d %H:%M:%S') eras=${ERAS[*]}" > "$CHAIN_STATUS"

TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG netNgen] $1" >/dev/null 2>&1 || true
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
    LOGDIR="$REPO/run_logs/${TAG}_netNgen_${ERA}"
    mkdir -p "$LOGDIR"
    STATUS_FILE="$LOGDIR/STATUS.txt"
    echo "STARTED $(date '+%Y-%m-%d %H:%M:%S') I=$I_HASH II=$II_HASH III=$III_HASH" > "$STATUS_FILE"
    echo "$ERA STARTED $(date '+%Y-%m-%d %H:%M:%S')" >> "$CHAIN_STATUS"
    NEW_FILTER=()
    for d in "${NEW_DATASETS[@]}"; do NEW_FILTER+=("$ERA/MC_mu/Diboson/$d"); done

    # ------------------------------ 003-I (new datasets) ------------------------------
    log "=== $ERA 003-ObjectSelectionI, hash $I_HASH ==="
    cd "$REPO/003-ObjectSelectionI"
    python3 scripts/run_all.py --tag "$TAG" --generatePreselectionDatasetJSON \
        --preselectionTag "$PRESELECTION_TAG" --preselectionHash "$PRESELECTION_HASH" \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --downloadGoldenJSONs \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --applyPreSelectionCorrections \
        --filter "${NEW_FILTER[@]}" --workers "$WORKERS" 2>&1 | tee -a "$LOGDIR/003I_precorr.log"
    echo "PRECORR_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON \
        --filter "${NEW_FILTER[@]}" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --writeBashScript --workers "$WORKERS" \
        --filter "${NEW_FILTER[@]}" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    cp -p "scripts/run_all_${TAG}.sh" "$LOGDIR/run_all_${TAG}_${ERA}_003I.sh"
    bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003I_production.log"
    python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --verifyOutput --workers "$WORKERS" \
        --filter "${NEW_FILTER[@]}" 2>&1 | tee -a "$LOGDIR/003I_steps.log"
    echo "I_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    notify "$ERA 003-I done."

    # ------------------------------ 003-II ------------------------------
    log "=== $ERA 003-ObjectSelectionII, hash $II_HASH ==="
    cd "$REPO/003-ObjectSelectionII"
    python3 scripts/run_all.py --tag "$TAG" --generateSelectionIDatasetJSON \
        --selectionITag "$TAG" --selectionIHash "$I_HASH" \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
    log "b-tagging efficiency maps for the new datasets only..."
    python3 scripts/run_all.py --tag "$TAG" --prepareEfficiencyFileset \
        --filter "${NEW_FILTER[@]}" 2>&1 | tee -a "$LOGDIR/003II_effmaps.log"
    python3 scripts/run_all.py --tag "$TAG" --computeBTaggingEfficiency --workers "$WORKERS" \
        --filter "${NEW_FILTER[@]}" 2>&1 | tee -a "$LOGDIR/003II_effmaps.log"
    for d in "${NEW_DATASETS[@]}"; do
        [ -f "inputs/SFs/Efficiency/$ERA/$d.root" ] || { echo "Missing efficiency map inputs/SFs/Efficiency/$ERA/$d.root" >&2; exit 1; }
    done
    echo "EFFMAPS_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON \
        --filter "${NEW_FILTER[@]}" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --writeBashScript --workers "$WORKERS" \
        --filter "${NEW_FILTER[@]}" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
    cp -p "scripts/run_all_${TAG}.sh" "$LOGDIR/run_all_${TAG}_${ERA}_003II.sh"
    bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003II_production.log"
    python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --computeABCDScaleFactor \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
    echo "II_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    notify "$ERA 003-II done."

    # ------------------------------ 003-III (all datasets) ------------------------------
    log "=== $ERA 003-ObjectSelectionIII, hash $III_HASH ==="
    cd "$REPO/003-ObjectSelectionIII"
    python3 scripts/run_all.py --tag "$TAG" --generateSelectionIIDatasetJSON \
        --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --fetchABCDScaleFactor \
        --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
        --filter "$ERA" --force 2>&1 | tee -a "$LOGDIR/003III_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 0 --systematics --force \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"
    python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 0 --force \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"
    echo "REGION_A_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 --force \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"
    python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 1 --force \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"
    python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
    echo "REGION_B_DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    python3 scripts/run_all.py --tag "$TAG" --makeplots --force \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
    if grep -h "Histogram file not found" "$LOGDIR"/003III_region*.log > "$LOGDIR/missing_hists.txt"; then
        echo "WARNING=missing_hists $(wc -l < "$LOGDIR/missing_hists.txt")" >> "$STATUS_FILE"
        notify "$ERA WARNING: $(wc -l < "$LOGDIR/missing_hists.txt") 'Histogram file not found' lines."
    fi
    echo "RESULT=SUCCESS $(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
    echo "$ERA DONE $(date '+%Y-%m-%d %H:%M:%S')" >> "$CHAIN_STATUS"
    notify "$ERA complete (003-I/II/III)."
done

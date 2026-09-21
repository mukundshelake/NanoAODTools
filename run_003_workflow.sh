#!/bin/bash
# General, reusable driver for the full 003-ObjectSelectionI -> II -> III
# workflow, for one era at a time:
#   003-I:   additional-lepton veto (SelectionCuts) + pre-selection
#            corrections (jetVetoMap, JetJER [MC], MuonRochester -- the early
#            pass that has to run before muonCut/extraMuonVetoCut/jetCut/
#            bjetCut ever see Muon_pt/Jet_pt, see JetJER.py's docstring) +
#            object selection (SelectedObjectsProducer) + METXYCorr.
#   003-II:  SF weights (muon ID/HLT/Iso, b-tagging, PU, L1prefire,
#            TopPtWeight) + the ABCD transfer factor R.
#   003-III: full ABCD data-driven QCD estimate (region A + R-weighted
#            region B + QCD template) + weight-only systematic band
#            (bTag/muonID/muonHLT/muonIso/puWeight/L1PreFiring, region A
#            only) + final CMS-style Data/MC plots + PDF report.
#
# Usage:
#   ./run_003_workflow.sh --tag TAG --era ERA \
#       --preselectionTag PRESELECTION_TAG --preselectionHash PRESELECTION_HASH \
#       [--sample] [--systematics] [--workers N] [--computeEfficiencyMaps]
#
# --sample:     first file per dataset only (quick-look; safe to run before a
#               full pass under the same tag -- every 003-III filename this
#               touches gets a "_sample" marker, see cms01's fix).
# --systematics: also build the weight-only systematic variant histograms in
#               003-III's region-A pass (config.yaml's weightSystematics).
#               Does not change output filenames -- if region-A histograms
#               already exist for this tag/hash from an earlier run without
#               --systematics, pass --force manually (edit the
#               --buildSelectionHists call below) to rebuild them with
#               variants included.
# --computeEfficiencyMaps: compute this era's b-tagging (and jet-PU-ID, even
#               though that SF isn't in ModuleList.MC yet) efficiency maps
#               before 003-II's SF-weighting pass. Needed once per era ever
#               (maps only depend on era/physics, not on the 003-tag) -- omit
#               once they already exist under 003-ObjectSelectionII/inputs/SFs/
#               Efficiency/{era}/.
#
# Requires already in place (none of this is automated here):
#   - The 002-Samples preselection production at
#     {STORAGE}/preselection/{preselectionTag}/{preselectionHash}/{era}
#     already on disk (Data_mu + MC_mu).
#   - Correctionlib/efficiency files fetched into 003-ObjectSelectionI/inputs/SFs/
#     and 003-ObjectSelectionII/inputs/SFs/ for this era (muon ID/HLT/Iso,
#     b-tagging, jet PU ID, jet_Jerc, jetvetomaps, met, pu_Weights) --
#     run_all.py --fetchSFFiles needs an interactive SSH session to lxplus
#     and is not run by this script.
#
# Sends a Telegram progress message (scripts/send_telegram.py) after each
# major stage, and on failure, so this can be launched inside `screen` and
# tracked without an interactive session watching it.

set -eo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- argument parsing -------------------------------------------------------
TAG=""; ERA=""; PRESELECTION_TAG=""; PRESELECTION_HASH=""
SAMPLE=false; SYSTEMATICS=false; WORKERS=15; COMPUTE_EFF=false

usage() {
    cat <<EOF
Usage: $0 --tag TAG --era ERA --preselectionTag PTAG --preselectionHash PHASH
          [--sample] [--systematics] [--workers N] [--computeEfficiencyMaps]
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag) TAG="$2"; shift 2 ;;
        --era) ERA="$2"; shift 2 ;;
        --preselectionTag) PRESELECTION_TAG="$2"; shift 2 ;;
        --preselectionHash) PRESELECTION_HASH="$2"; shift 2 ;;
        --sample) SAMPLE=true; shift ;;
        --systematics) SYSTEMATICS=true; shift ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --computeEfficiencyMaps) COMPUTE_EFF=true; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown argument: $1"; usage ;;
    esac
done

if [[ -z "$TAG" || -z "$ERA" || -z "$PRESELECTION_TAG" || -z "$PRESELECTION_HASH" ]]; then
    echo "Error: --tag, --era, --preselectionTag, --preselectionHash are all required."
    usage
fi

SAMPLE_FLAG=()
$SAMPLE && SAMPLE_FLAG=(--sample)
SYST_FLAG=()
$SYSTEMATICS && SYST_FLAG=(--systematics)
RUN_LABEL="full"
$SAMPLE && RUN_LABEL="sample"

LOGDIR="$REPO/run_logs/${TAG}_${ERA}_${RUN_LABEL}"
mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
{
    echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')"
    echo "TAG=$TAG ERA=$ERA PRESELECTION_TAG=$PRESELECTION_TAG PRESELECTION_HASH=$PRESELECTION_HASH SAMPLE=$SAMPLE SYSTEMATICS=$SYSTEMATICS WORKERS=$WORKERS COMPUTE_EFF=$COMPUTE_EFF"
} > "$STATUS_FILE"

# send_telegram.py needs requests+dotenv, which live in the base conda env,
# not latestcoffea -- capture the base env's python3 now, before
# `conda activate latestcoffea` below shadows it.
TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG/$ERA $RUN_LABEL] $1" >/dev/null 2>&1 || true
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

notify "Starting ($RUN_LABEL, systematics=$SYSTEMATICS): preselection=$PRESELECTION_TAG/$PRESELECTION_HASH"

################################################################################
# 003-ObjectSelectionI
################################################################################
log "=== 003-ObjectSelectionI: $ERA ($RUN_LABEL) ==="
cd "$REPO/003-ObjectSelectionI"

python3 scripts/run_all.py --tag "$TAG" --generatePreselectionDatasetJSON \
    --preselectionTag "$PRESELECTION_TAG" --preselectionHash "$PRESELECTION_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

python3 scripts/run_all.py --tag "$TAG" --downloadGoldenJSONs \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

log "Running pre-selection corrections pass (jetVetoMap/jetJER/muonRochester, ${RUN_LABEL})..."
python3 scripts/run_all.py --tag "$TAG" --applyPreSelectionCorrections "${SAMPLE_FLAG[@]}" \
    --filter "$ERA" --workers "$WORKERS" 2>&1 | tee -a "$LOGDIR/003I_precorr.log"
notify "003-I: pre-selection corrections pass done."

python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON "${SAMPLE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

python3 scripts/run_all.py --tag "$TAG" --writeBashScript "${SAMPLE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

log "Running 003-I object-selection skim production (${RUN_LABEL})..."
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
log "=== 003-ObjectSelectionII: $ERA ($RUN_LABEL) ==="
cd "$REPO/003-ObjectSelectionII"

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIDatasetJSON \
    --selectionITag "$TAG" --selectionIHash "$I_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

if $COMPUTE_EFF; then
    log "First-time-era setup: computing b-tagging/jet-PU-ID efficiency maps for $ERA..."
    python3 scripts/run_all.py --tag "$TAG" --prepareEfficiencyFileset \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --computeBTaggingEfficiency \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
    python3 scripts/run_all.py --tag "$TAG" --computeJetPUIDEfficiency \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003II_steps.log"
    notify "003-II: b-tagging/jet-PU-ID efficiency maps computed for $ERA."
else
    log "Reusing existing $ERA b-tagging/jetPUID efficiency maps (pass --computeEfficiencyMaps if this era is new)."
fi

python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON "${SAMPLE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

python3 scripts/run_all.py --tag "$TAG" --writeBashScript "${SAMPLE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

log "Running 003-II SF-weighted skim production (${RUN_LABEL}; includes TopPtWeight)..."
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
log "=== 003-ObjectSelectionIII: $ERA ($RUN_LABEL, full ABCD) ==="
cd "$REPO/003-ObjectSelectionIII"

python3 scripts/run_all.py --tag "$TAG" --generateSelectionIIDatasetJSON \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

python3 scripts/run_all.py --tag "$TAG" --fetchABCDScaleFactor \
    --selectionIITag "$TAG" --selectionIIHash "$II_HASH" \
    --filter "$ERA" --force 2>&1 | tee -a "$LOGDIR/003III_steps.log"

log "Building region-A (nominal) histograms${SYSTEMATICS:+ with --systematics}..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 0 \
    "${SAMPLE_FLAG[@]}" "${SYST_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"

python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 0 "${SAMPLE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionA.log"
notify "003-III: region-A histograms done."

log "Building region-B (R-weighted) histograms for the ABCD QCD template (no --systematics -- QCD template doesn't consume weight variants)..."
python3 scripts/run_all.py --tag "$TAG" --buildSelectionHists --regionFilter 1 "${SAMPLE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"

python3 scripts/run_all.py --tag "$TAG" --aggregrateGroupHists --regionFilter 1 "${SAMPLE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003III_regionB.log"

python3 scripts/run_all.py --tag "$TAG" --buildQCDTemplate "${SAMPLE_FLAG[@]}" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"
notify "003-III: region-B histograms + QCD template done."

python3 scripts/run_all.py --tag "$TAG" --makeplots "${SAMPLE_FLAG[@]}" --force \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003III_steps.log"

III_HASH="$(get_hash 003-ObjectSelectionIII)"
log "003-ObjectSelectionIII config hash: $III_HASH"
echo "$III_HASH" > "$LOGDIR/III_HASH.txt"

REPORT_PDF="$REPO/003-ObjectSelectionIII/outputs/${TAG}/${III_HASH}/${TAG}_${III_HASH}_report.pdf"
log "=== DONE ($RUN_LABEL). Final PDF: $REPORT_PDF ==="
echo "REPORT_PDF=$REPORT_PDF" >> "$STATUS_FILE"
notify "003-III: plots + PDF done. Report: $REPORT_PDF"

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
#       [--sample] [--systematics] [--workers N] [--computeEfficiencyMaps] [--condor]
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
# --condor:     run 003-I's and 003-II's per-file skim production as HTCondor
#               jobs instead of locally, via each chapter's run_all.py
#               --submitCondorJobs/--checkCondorStatus (002-Samples' model).
#               Only those two stages move -- every JSON/efficiency/histogram
#               step around them still runs here, since they are single
#               aggregate passes with nothing to parallelise. lxplus only:
#               needs eossubmit schedds, and --output-dir/--work-area must be
#               on EOS.
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
SAMPLE=false; SYSTEMATICS=false; WORKERS=15; COMPUTE_EFF=false; CONDOR=false

usage() {
    cat <<EOF
Usage: $0 --tag TAG --era ERA --preselectionTag PTAG --preselectionHash PHASH
          [--sample] [--systematics] [--workers N] [--computeEfficiencyMaps] [--condor]
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
        --condor) CONDOR=true; shift ;;
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
    echo "TAG=$TAG ERA=$ERA PRESELECTION_TAG=$PRESELECTION_TAG PRESELECTION_HASH=$PRESELECTION_HASH SAMPLE=$SAMPLE SYSTEMATICS=$SYSTEMATICS WORKERS=$WORKERS COMPUTE_EFF=$COMPUTE_EFF CONDOR=$CONDOR"
} > "$STATUS_FILE"

# send_telegram.py needs requests+dotenv, which live in the base conda env,
# not latestcoffea -- capture the base env's python3 now, before
# `conda activate latestcoffea` below shadows it.
TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
# lxplus has no such conda install; fall back to whatever python3 is on PATH.
# notify() tolerates failure, so a missing requests/dotenv only means no
# Telegram message, never an aborted run.
[[ -x "$TELEGRAM_PY" ]] || TELEGRAM_PY="python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[$TAG/$ERA $RUN_LABEL] $1" >/dev/null 2>&1 || true
}

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT
# Make signal exits explicit (143/130) so STATUS.txt names them. Note: a run
# killed with SIGTERM once recorded EXIT_CODE=0 / RESULT=SUCCESS here; that could
# not be reproduced afterwards (a group-killed copy of this trap setup records
# exit 1 on lxplus's bash 5.1.8 even without these lines), so these traps are
# not a confirmed fix for it -- treat a SUCCESS with no final stage in the log
# with suspicion.
trap 'exit 143' TERM
trap 'exit 130' INT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# --- environment setup (self-contained: do not rely on an already-activated shell) ---
# The two machines this runs on provide the same Python stack differently:
#   cms02 (no CVMFS CMSSW release in use): the `latestcoffea` conda env, plus
#     standalone/env_standalone.sh to put PhysicsTools.NanoAODTools on the path.
#   lxplus: cmsenv + CRAB client via setupEnv.sh (the env half of each 003
#     chapter's getcrabReady.sh). NanoAODTools comes from the CMSSW area and coffea/
#     awkward/correctionlib/uproot/hist/mplhep/scipy from the user's ~/.local
#     (all verified importing under cmsenv alone). No conda env exists there,
#     and none is used for condor either -- condor workers set up cmsenv plus
#     the LCG stack themselves (see each chapter's scripts/condor/).
# Detect by what is actually present rather than by hostname.
CMS02_CONDA="/home/mukund/miniconda3/etc/profile.d/conda.sh"
LXPLUS_CMSSW_SRC="/eos/user/m/mshelake/Analysis/CMSSW_13_3_0/src"
if [[ -f "$CMS02_CONDA" ]]; then
    source "$CMS02_CONDA"
    conda activate latestcoffea
    source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1
elif [[ -d "$LXPLUS_CMSSW_SRC" ]]; then
    # Same environment getcrabReady.sh sets up, minus its proxy creation and
    # CRAB resubmission (see setupEnv.sh for why those are split off).
    source "$REPO/003-ObjectSelectionI/scripts/crab/setupEnv.sh"
    # coffea stack matching cms02's latestcoffea (coffea.dataset_tools etc.) --
    # built once by setup_lxplus_venv.sh; ROOT/NanoAODTools still come from CMSSW.
    LXPLUS_VENV="${LXPLUS_VENV:-/eos/user/m/mshelake/venvs/latestcoffea}"
    if [[ ! -f "$LXPLUS_VENV/bin/activate" ]]; then
        echo "Error: no Python venv at $LXPLUS_VENV. Build it once with $REPO/setup_lxplus_venv.sh"
        exit 1
    fi
    source "$LXPLUS_VENV/bin/activate"
else
    echo "Error: no known environment found (neither $CMS02_CONDA nor $LXPLUS_CMSSW_SRC)."
    exit 1
fi

# Block until this chapter's condor jobs for $ERA are all done, going through
# run_all.py --checkCondorStatus exactly as a user would (it owns the work-area
# layout, same as for submission). Held jobs are released and missing ones
# resubmitted on each pass; checkCondorStatus.py's own guard refuses a mass
# resubmit (>25 jobs and >40% of the work area at once), so a systemic failure
# stops here instead of silently re-queueing the era. Must be called from
# inside the chapter directory.
condor_wait() {
    # $1: label for logs; any further args go to run_all.py (e.g. --condorStage X)
    local stage="$1"; shift
    # Resubmission rounds allowed before giving up. A job that fails every time
    # would otherwise be resubmitted forever; three rounds covers transient
    # worker/EOS hiccups without masking a job that simply cannot succeed.
    local max_rounds=3 rounds=0
    while true; do
        local line
        line="$(python3 scripts/run_all.py --tag "$TAG" --checkCondorStatus "$@" --filter "$ERA" 2>&1 \
                | grep "TOTAL:" | tail -1 || true)"
        if [[ -z "$line" ]]; then
            log "  [$stage] ERROR: --checkCondorStatus returned no TOTAL line (nothing submitted?)"
            return 1
        fi
        log "  [$stage] $line"
        if echo "$line" | grep -qE "missing=[1-9]|held=[1-9]"; then
            if (( rounds >= max_rounds )); then
                log "  [$stage] ERROR: jobs still missing/held after $max_rounds resubmission rounds; giving up."
                log "  [$stage] Inspect with: run_all.py --tag $TAG --checkCondorStatus $* --filter $ERA"
                return 1
            fi
            rounds=$((rounds + 1))
            log "  [$stage] releasing held / resubmitting missing (round $rounds/$max_rounds)..."
            python3 scripts/run_all.py --tag "$TAG" --checkCondorStatus "$@" \
                --resubmitHeldCondorJobs --resubmitMissingCondorJobs --filter "$ERA" \
                >>"$LOGDIR/${stage}_condor.log" 2>&1 || \
                log "  [$stage] resubmit declined or failed (see ${stage}_condor.log)"
        elif ! echo "$line" | grep -qE "idle=[1-9]|running=[1-9]"; then
            log "  [$stage] all jobs complete."
            return 0
        fi
        sleep 120
    done
}

get_hash() {
    local dir="$1"; shift
    ( cd "$REPO/$dir" && python3 scripts/run_all.py --tag "$TAG" --printHash "$@" 2>&1 \
        | grep "Config hash:" | awk '{print $NF}' )
}

if $CONDOR; then
    # `module` is a shell function, absent in a non-interactive shell until the
    # modules init is sourced. It must also be run unpiped and in THIS shell --
    # piping it (e.g. `module load ... | tail -1`) runs it in a subshell, so
    # CONDOR_CONFIG and the eossubmit schedd routing never reach condor_submit.
    # That exact mistake cost days of silently-failing submissions in 002-Samples.
    if ! command -v module >/dev/null 2>&1; then
        [ -f /etc/profile.d/modules.sh ] && source /etc/profile.d/modules.sh
    fi
    module load lxbatch/eossubmit
    log "condor mode: loaded lxbatch/eossubmit (CONDOR_CONFIG=${CONDOR_CONFIG:-unset})"
fi

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
# Same DataMC scope as the selection below: correcting MC_alt here would only
# produce files nothing downstream reads (for UL2017, 1612 files / 170G).
if $CONDOR; then
    # Per-file pass like the selection, so it goes to condor too rather than
    # running on the login node. run_all.py writes to the same
    # {STORAGE}/preSelectionCorrected/... tree the local pass does; the scan that
    # turns it into preSelectionCorrected_{era}_datasets.json runs once the jobs
    # are all done.
    python3 scripts/run_all.py --tag "$TAG" --submitCondorJobs --condorStage precorrection \
        "${SAMPLE_FLAG[@]}" --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_precorr.log"
    condor_wait 003Iprecorr --condorStage precorrection
    python3 scripts/run_all.py --tag "$TAG" --generatePreSelectionCorrectedDatasetJSON \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/003I_precorr.log"
else
    python3 scripts/run_all.py --tag "$TAG" --applyPreSelectionCorrections "${SAMPLE_FLAG[@]}" \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" --workers "$WORKERS" 2>&1 | tee -a "$LOGDIR/003I_precorr.log"
fi
notify "003-I: pre-selection corrections pass done."

python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON "${SAMPLE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

if $CONDOR; then
    # Same --filter as the local branch: Data_mu + MC_mu only. run_all.py picks the
    # inputs (corrected where --applyPreSelectionCorrections produced them) and lays
    # out output/work areas, exactly as for the local and CRAB paths.
    log "Submitting 003-I object-selection skims to condor (${RUN_LABEL})..."
    python3 scripts/run_all.py --tag "$TAG" --submitCondorJobs "${SAMPLE_FLAG[@]}" \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_production.log"
    condor_wait 003I --condorStage selection
else
    python3 scripts/run_all.py --tag "$TAG" --writeBashScript "${SAMPLE_FLAG[@]}" \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003I_steps.log"

    log "Running 003-I object-selection skim production (${RUN_LABEL})..."
    bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003I_production.log"
fi

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

if $CONDOR; then
    # MC jobs need this era's b-tagging efficiency maps; the submitter checks per
    # dataset and exits nonzero before queueing anything if one is absent, which is
    # what --computeEfficiencyMaps above exists to prevent.
    log "Submitting 003-II SF-weighted skims to condor (${RUN_LABEL}; includes TopPtWeight)..."
    python3 scripts/run_all.py --tag "$TAG" --submitCondorJobs "${SAMPLE_FLAG[@]}" \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_production.log"
    condor_wait 003II
else
    python3 scripts/run_all.py --tag "$TAG" --writeBashScript "${SAMPLE_FLAG[@]}" \
        --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/003II_steps.log"

    log "Running 003-II SF-weighted skim production (${RUN_LABEL}; includes TopPtWeight)..."
    bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/003II_production.log"
fi

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
# --makeplots deliberately carries on if createPlotsPDF.py fails ("Continuing
# without PDF creation..."), so its exit status says nothing about the report.
# Check the file itself before claiming it: a run once logged "Final PDF: <path>"
# and RESULT=SUCCESS with no PDF there, createPlotsPDF.py having died on a
# missing reportlab.
if [[ ! -s "$REPORT_PDF" ]]; then
    log "ERROR: plots were made but the report PDF is missing or empty: $REPORT_PDF"
    log "       See createPlotsPDF.py's traceback in $LOGDIR/003III_steps.log."
    exit 1
fi
log "=== DONE ($RUN_LABEL). Final PDF: $REPORT_PDF ==="
echo "REPORT_PDF=$REPORT_PDF" >> "$STATUS_FILE"
notify "003-III: plots + PDF done. Report: $REPORT_PDF"

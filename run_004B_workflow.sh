#!/bin/bash
# General, reusable driver for 004B-BDTVariables, for one era at a time:
#   computes 17 event-shape / Fox-Wolfram variables per event from the
#   004A-Reconstruction skims (JetHT, pTSum, FW1-3, AL, the six sphericity
#   tensor elements, S, P, A, p2in, p2out), plus the truth-level hard-scatter
#   label y (1=qqbar, 2=gg, 3=qg, 4=qq' , 5=qq, 0=undefined/Data) and qDir.
#   Writes one *_Skim.root per input file (NanoAODTools' default postfix --
#   runBDTVariables.py passes none; the name is not *_BDTVars.root).
#
# Usage:
#   ./run_004B_workflow.sh --tag TAG --era ERA --reconstructionHash HASH \
#       [--reconstructionTag RECO_TAG] [--sample] [--workers N] \
#       [--makeVariablePlots] [--force]
#
# --reconstructionTag: tag of the 004A-Reconstruction production to read.
#               Defaults to --tag. Pass it explicitly when the upstream era
#               sits under a different tag string than the one this chapter's
#               output should carry. (For the earlySeptember campaign both are
#               "earlySeptember_corrected": 004A wrote *its* output under the
#               correct spelling for both UL2016 eras, even though preVFP read
#               its 003 input from the misspelled "earlySepetember_correted".
#               The typo does not propagate past 003.)
# --sample:     first file per dataset only (quick-look). Safe to run before a
#               full pass under the same tag: output is one _Skim.root per
#               input file and runBDTVariables.py skips any file whose output
#               already exists, so the later full pass just processes the
#               remainder. This driver does not build aggregated histograms,
#               so there is nothing that can go stale between the two passes
#               (unlike 003-III, where a sample-then-full run under one tag
#               silently keeps the sample's histograms -- see --force there).
# --makeVariablePlots: after production, build the BDT-variable histograms and
#               Data/MC comparison plots (--buildBDTVariableHists,
#               --aggregateBDTVariableHists, --makeBDTVariablePlots). Off by
#               default. Unlike every other stage here, a failure in this one
#               is reported but does NOT fail the run: by that point the ROOT
#               files and the dataset map -- the things downstream chapters
#               actually consume -- are already on disk, and losing a 40-hour
#               production's SUCCESS status over a plotting bug helps nobody.
# --force:      re-process files whose _Skim.root already exists, instead of
#               skipping them.
#
# Requires already in place (none of this is automated here):
#   - The 004A-Reconstruction production at
#     {STORAGE}/reconstruction/{reconstructionTag}/{reconstructionHash}/{era}
#     already on disk. The script hard-fails up front if it is not.
#   - The NanoAODTools framework on PYTHONPATH (env_standalone.sh below).
#     BDTvariableModule needs only numpy -- no scipy, unlike 004A's RecoModule.
#
# No cut string and no golden JSON are re-applied here; both were handled
# upstream in selectionII, and run_all.py hard-codes both to None.
#
# Note: run_all.py --writeBashScript names its output scripts/run_all_{TAG}.sh
# -- per tag, not per era. Do not run two eras concurrently under the same tag;
# they would clobber each other's production script. Run them serially. The
# same applies to the chapter-level inputs/reconstruction_{era}_datasets.json
# that step [0] writes: it is keyed by era but not by tag, so two tags working
# the same era at once would race. Step [1] reads the per-run snapshot under
# outputs/{tag}/{hash}/inputs/ rather than that shared file, which narrows the
# window to between [0] and [1] but does not close it.
#
# Sends a Telegram progress message (scripts/send_telegram.py) after each major
# stage, and on failure, so this can be launched inside `screen` and tracked
# without an interactive session watching it.

set -eo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER="$REPO/004B-BDTVariables"

# --- argument parsing -------------------------------------------------------
TAG=""; ERA=""; RECO_TAG=""; RECO_HASH=""
SAMPLE=false; WORKERS=15; MAKE_PLOTS=false; FORCE=false

usage() {
    cat <<EOF
Usage: $0 --tag TAG --era ERA --reconstructionHash HASH
          [--reconstructionTag RECO_TAG] [--sample] [--workers N]
          [--makeVariablePlots] [--force]
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag) TAG="$2"; shift 2 ;;
        --era) ERA="$2"; shift 2 ;;
        --reconstructionTag) RECO_TAG="$2"; shift 2 ;;
        --reconstructionHash) RECO_HASH="$2"; shift 2 ;;
        --sample) SAMPLE=true; shift ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --makeVariablePlots) MAKE_PLOTS=true; shift ;;
        --force) FORCE=true; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown argument: $1"; usage ;;
    esac
done

if [[ -z "$TAG" || -z "$ERA" || -z "$RECO_HASH" ]]; then
    echo "Error: --tag, --era and --reconstructionHash are all required."
    usage
fi

# Default the upstream tag to this chapter's tag -- the common case.
[[ -z "$RECO_TAG" ]] && RECO_TAG="$TAG"

SAMPLE_FLAG=()
$SAMPLE && SAMPLE_FLAG=(--sample)
FORCE_FLAG=()
$FORCE && FORCE_FLAG=(--force)
RUN_LABEL="full"
$SAMPLE && RUN_LABEL="sample"

# Prefixed with 004B_ so these never collide with the 003/004A drivers'
# run_logs/... for the same tag and era.
LOGDIR="$REPO/run_logs/004B_${TAG}_${ERA}_${RUN_LABEL}"
mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
{
    echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')"
    echo "TAG=$TAG ERA=$ERA RECONSTRUCTION_TAG=$RECO_TAG RECONSTRUCTION_HASH=$RECO_HASH SAMPLE=$SAMPLE WORKERS=$WORKERS MAKE_PLOTS=$MAKE_PLOTS FORCE=$FORCE"
} > "$STATUS_FILE"

# send_telegram.py needs requests+dotenv, which live in the base conda env, not
# latestcoffea -- capture the base env's python3 now, before
# `conda activate latestcoffea` below shadows it.
TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[004B $TAG/$ERA $RUN_LABEL] $1" >/dev/null 2>&1 || true
}

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# Print "<n_datasets> <n_files>" for one of the chapter's dataset-map JSONs
# (the {DataMC: {group: {dataset: {path: tree}}}} shape generateDatasetJSON.py
# emits). Exits the script if the file is missing or unreadable, so a failed
# count can never be mistaken downstream for a legitimate count of zero.
count_json() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "Error: expected dataset map not found: $path" >&2
        exit 1
    fi
    python3 - "$path" <<'PY' || exit 1
import json, sys
with open(sys.argv[1]) as fh:
    payload = json.load(fh)
datasets = files = 0
for data_mc in payload.values():
    for group in data_mc.values():
        for entries in group.values():
            datasets += 1
            files += len(entries)
print(datasets, files)
PY
}

# --- environment setup (self-contained: do not rely on an already-activated shell) ---
source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea
source "$REPO/standalone/env_standalone.sh" >/dev/null 2>&1

cd "$CHAPTER"

# --- pre-flight -------------------------------------------------------------
# One --printHash probe gives us both this chapter's config hash and the
# machine-resolved STORAGE path, rather than duplicating
# utils.resolve_storage_path()'s hostname matching here. (It also creates
# outputs/{tag}/{hash} and snapshots inputs/, which step [0] then adds to.)
PROBE="$(python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1)"
BDT_HASH="$(echo "$PROBE" | grep 'Config hash:'        | awk '{print $NF}')"
STORAGE="$( echo "$PROBE" | grep 'Using storage base:' | awk '{print $NF}')"

if [[ -z "$BDT_HASH" || -z "$STORAGE" ]]; then
    echo "Error: could not determine config hash / storage base from run_all.py --printHash:"
    echo "$PROBE"
    exit 1
fi

RECO_DIR="$STORAGE/reconstruction/$RECO_TAG/$RECO_HASH/$ERA"
if [[ ! -d "$RECO_DIR" ]]; then
    # generateDatasetJSON.py would otherwise die on a bare os.listdir() with a
    # raw FileNotFoundError, several frames deep and with no hint as to which
    # tag/hash/era was actually wrong.
    echo "Error: 004A-Reconstruction output not found at:"
    echo "  $RECO_DIR"
    echo "Check --reconstructionTag / --reconstructionHash / --era. Available:"
    ls -1 "$STORAGE/reconstruction" 2>/dev/null | sed 's/^/  tag: /'
    [[ -d "$STORAGE/reconstruction/$RECO_TAG" ]] && \
        find "$STORAGE/reconstruction/$RECO_TAG" -mindepth 2 -maxdepth 2 -type d \
            -printf '  have: %P\n' 2>/dev/null
    exit 1
fi

log "004B-BDTVariables config hash: $BDT_HASH"
echo "$BDT_HASH" > "$LOGDIR/BDT_HASH.txt"
{
    echo "BDT_HASH=$BDT_HASH"
    echo "STORAGE=$STORAGE"
    echo "RECONSTRUCTION_DIR=$RECO_DIR"
    echo "BDT_OUTPUT_DIR=$STORAGE/BDTVariables/$TAG/$BDT_HASH/$ERA"
} >> "$STATUS_FILE"

notify "Starting ($RUN_LABEL): reading reconstruction=$RECO_TAG/$RECO_HASH, writing BDTVariables/$TAG/$BDT_HASH"

################################################################################
# [0] Build the reconstruction dataset map fresh from disk
################################################################################
# Scanned fresh every run (rather than reusing a JSON some earlier invocation
# left in inputs/) so the recorded paths always reflect where the files
# actually are now. Era-filtered: an unfiltered run would also try eras with no
# 004A production under this tag and abort on the first one missing.
log "=== [0] generateReconstructionDatasetJSON: $ERA (from $RECO_TAG/$RECO_HASH) ==="
python3 scripts/run_all.py --tag "$TAG" --generateReconstructionDatasetJSON \
    --reconstructionTag "$RECO_TAG" --reconstructionHash "$RECO_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/004B_steps.log"

# Read the per-run snapshot, not the chapter-level inputs/ copy: this is the
# one step [1] actually consumes, so counting it is what verifies step [1] will
# see anything at all.
DATASET_JSON="$CHAPTER/outputs/$TAG/$BDT_HASH/inputs/reconstruction_${ERA}_datasets.json"
read -r N_DATASETS N_FILES <<<"$(count_json "$DATASET_JSON")"
if [[ ! "$N_FILES" =~ ^[0-9]+$ ]]; then
    echo "Error: could not count $DATASET_JSON (got '$N_DATASETS' '$N_FILES')."
    exit 1
fi
log "reconstruction map: $N_DATASETS datasets, $N_FILES healthy files"
echo "RECONSTRUCTION_DATASETS=$N_DATASETS RECONSTRUCTION_FILES=$N_FILES" >> "$STATUS_FILE"

if [[ "$N_FILES" -eq 0 ]]; then
    # An existing-but-empty era directory, or one whose every ROOT file failed
    # generateDatasetJSON.py's health check. Either way there is nothing to
    # process, and continuing would "succeed" having done nothing at all.
    echo "Error: no healthy reconstruction ROOT files found under $RECO_DIR."
    echo "Nothing to process -- check the 004A production and its logs."
    exit 1
fi
notify "[0] reconstruction map built: $N_DATASETS datasets, $N_FILES files."

################################################################################
# [1] Build the per-file task list
################################################################################
# Data_mu/MC_mu only: MC_alt is listed in config.yaml's NgenandXsec table for
# every era but has never been produced upstream in this campaign. Naming the
# groups explicitly also keeps step [2] from emitting no-op runBDTVariables
# calls (and stray mkdir -p log directories) for it.
log "=== [1] generateProcessListJSON: $ERA ($RUN_LABEL) ==="
python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON "${FORCE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/004B_steps.log"

PROCESS_LIST="$CHAPTER/outputs/$TAG/$BDT_HASH/$ERA/${TAG}_${ERA}_processListJSON.json"
if [[ ! -f "$PROCESS_LIST" ]]; then
    echo "Error: expected process list not found: $PROCESS_LIST"
    exit 1
fi
# --sample is not applied when the process list is built -- run_all.py always
# writes every not-yet-done task and flags the first file of each dataset
# "isSample", and it is runBDTVariables.py that filters down to those at run
# time. So the list length is the full count even for a sample run; count the
# isSample entries instead, or the log claims far more work than will be done.
N_TASKS="$(SAMPLE_ONLY="$SAMPLE" python3 - "$PROCESS_LIST" <<'PY'
import json, os, sys
with open(sys.argv[1]) as fh:
    tasks = json.load(fh)
if os.environ.get("SAMPLE_ONLY") == "true":
    tasks = [t for t in tasks if t.get("isSample")]
print(len(tasks))
PY
)"
if [[ ! "$N_TASKS" =~ ^[0-9]+$ ]]; then
    echo "Error: could not count $PROCESS_LIST (got '$N_TASKS')."
    exit 1
fi
if $SAMPLE; then
    log "process list: $N_TASKS sample tasks to run (one per dataset; $N_FILES input files total)"
else
    log "process list: $N_TASKS tasks to run (of $N_FILES input files)"
fi
echo "TASKS=$N_TASKS" >> "$STATUS_FILE"

if [[ "$N_TASKS" -eq 0 ]]; then
    # Not an error: every output already exists, so this is a no-op re-run.
    log "Nothing to do -- all BDT-variable output already exists for this tag/hash."
    log "Pass --force to re-process. Continuing to the dataset map."
fi

################################################################################
# [2] Write the production script
################################################################################
log "=== [2] writeBashScript: $ERA (workers=$WORKERS) ==="
python3 scripts/run_all.py --tag "$TAG" --writeBashScript "${SAMPLE_FLAG[@]}" "${FORCE_FLAG[@]}" \
    --workers "$WORKERS" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/004B_steps.log"

PRODUCTION_SCRIPT="$CHAPTER/scripts/run_all_${TAG}.sh"
if [[ ! -f "$PRODUCTION_SCRIPT" ]]; then
    echo "Error: expected production script not found: $PRODUCTION_SCRIPT"
    exit 1
fi

################################################################################
# [3] Production
################################################################################
# Much cheaper per event than 004A: BDTvariableProducer is numpy arithmetic over
# the Jet collection plus a GenPart scan for the y label -- no per-permutation
# SLSQP fit. Expect this stage to be I/O- rather than CPU-bound, and far shorter
# than the 004A pass that produced its input. Restartable: runBDTVariables.py
# skips any input whose _Skim.root already exists.
log "=== [3] Running BDT-variable production ($RUN_LABEL, $N_TASKS tasks, workers=$WORKERS) ==="
bash "$PRODUCTION_SCRIPT" 2>&1 | tee -a "$LOGDIR/004B_production.log"
notify "[3] BDT-variable production done."

################################################################################
# [4] Dataset map of the BDTVariables output -- 005/006/007's input
################################################################################
BDT_OUT_DIR="$STORAGE/BDTVariables/$TAG/$BDT_HASH/$ERA"
if [[ ! -d "$BDT_OUT_DIR" ]]; then
    # Every task failed, or production never actually ran. generateDatasetJSON.py
    # would otherwise die on a bare os.listdir() of this path.
    echo "Error: no BDTVariables output directory at:"
    echo "  $BDT_OUT_DIR"
    echo "Production produced nothing -- see $LOGDIR/004B_production.log."
    exit 1
fi

log "=== [4] generateDatasetJSON: $ERA ==="
python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/004B_steps.log"

BDT_JSON="$CHAPTER/outputs/$TAG/$BDT_HASH/$ERA/BDTVariables_${TAG}_${ERA}_datasets.json"
read -r N_OUT_DATASETS N_OUT_FILES <<<"$(count_json "$BDT_JSON")"
if [[ ! "$N_OUT_FILES" =~ ^[0-9]+$ ]]; then
    echo "Error: could not count $BDT_JSON (got '$N_OUT_DATASETS' '$N_OUT_FILES')."
    exit 1
fi
log "BDTVariables map: $N_OUT_DATASETS datasets, $N_OUT_FILES healthy files (input had $N_DATASETS / $N_FILES)"
echo "BDT_DATASETS=$N_OUT_DATASETS BDT_FILES=$N_OUT_FILES" >> "$STATUS_FILE"

# A full pass should reproduce every input file one-for-one. Fewer means some
# files failed or were dropped as unhealthy -- worth flagging loudly, but not
# worth failing the run over: the map is still valid for whatever did land, and
# 004B_production.log has the per-file errors.
if ! $SAMPLE && [[ "$N_OUT_FILES" -ne "$N_FILES" ]]; then
    log "WARNING: $N_OUT_FILES BDTVariables files for $N_FILES inputs -- $((N_FILES - N_OUT_FILES)) missing."
    log "         Check $LOGDIR/004B_production.log, then re-run this script to fill the gaps."
    echo "WARNING=bdt_file_count_mismatch expected=$N_FILES got=$N_OUT_FILES" >> "$STATUS_FILE"
    notify "[4] WARNING: $N_OUT_FILES/$N_FILES files produced -- $((N_FILES - N_OUT_FILES)) missing."
else
    notify "[4] dataset map done: $N_OUT_DATASETS datasets, $N_OUT_FILES files."
fi

################################################################################
# [5] BDT-variable histograms and Data/MC plots (optional)
################################################################################
# Deliberately non-fatal, unlike every stage above: see --makeVariablePlots in
# the header. The `|| PLOTS_OK=false` also stops set -e from killing the run.
if $MAKE_PLOTS; then
    log "=== [5] BDT-variable histograms and plots: $ERA ==="
    PLOTS_OK=true
    python3 scripts/run_all.py --tag "$TAG" \
        --buildBDTVariableHists --aggregateBDTVariableHists --makeBDTVariablePlots \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/004B_steps.log" || PLOTS_OK=false
    if $PLOTS_OK; then
        log "BDT-variable plots done."
        echo "PLOTS=OK" >> "$STATUS_FILE"
        notify "[5] BDT-variable plots done."
    else
        log "WARNING: BDT-variable histogram/plot step failed -- production output is unaffected."
        log "         See $LOGDIR/004B_steps.log."
        echo "WARNING=plots_failed" >> "$STATUS_FILE"
        notify "[5] WARNING: plot step failed (production output is fine). See $LOGDIR"
    fi
else
    log "Skipping BDT-variable histograms/plots (pass --makeVariablePlots to build them)."
fi

log "=== DONE ($RUN_LABEL). BDTVariables output: $BDT_OUT_DIR ==="
log "    Downstream (005/006/007) input map: $BDT_JSON"
echo "BDT_DATASET_JSON=$BDT_JSON" >> "$STATUS_FILE"

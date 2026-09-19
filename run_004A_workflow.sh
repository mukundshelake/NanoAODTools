#!/bin/bash
# General, reusable driver for 004A-Reconstruction, for one era at a time:
#   ttbar semi-leptonic kinematic reconstruction of the 003-ObjectSelectionII
#   skims -- neutrino-pz quadratic from the leptonic-W mass constraint, up to 4
#   permutations (2 b-jet assignments x pz roots), a full scipy SLSQP chi2 fit
#   per permutation with soft W/equal-top-mass penalties, keeping the
#   lowest-chi2 converged fit. Writes Top_lep_*, Top_had_*, Chi2, Chi2_prefit,
#   Pgof, chi2_status.
#
# Usage:
#   ./run_004A_workflow.sh --tag TAG --era ERA --selectionIIHash HASH \
#       [--selectionIITag SEL2_TAG] [--sample] [--workers N] \
#       [--makeDeltaPlots] [--force]
#
# --selectionIITag: tag of the 003-ObjectSelectionII production to read.
#               Defaults to --tag. Pass it explicitly when the upstream era
#               sits under a different tag string than the one you want this
#               chapter's output to carry -- e.g. the earlySeptember campaign,
#               whose UL2016preVFP 003 output landed under the misspelled tag
#               "earlySepetember_correted" while UL2016postVFP is under
#               "earlySeptember_corrected". Same physics (identical 003
#               hashes), different tag string only.
# --sample:     first file per dataset only (quick-look). Safe to run before a
#               full pass under the same tag: reconstruction output is one
#               _Skim.root per input file and runReco.py skips any file whose
#               output already exists, so the later full pass just processes
#               the remaining files. (Unlike 003-III, there are no aggregated
#               histograms here to go stale -- no --force needed.)
# --makeDeltaPlots: after production, build the reconstructed-vs-generator
#               top-mass residual plots. MC only, and only reads
#               ttbar_SemiLeptonic datasets (needs GenPart_pdgId/statusFlags/
#               mass) -- a Data-only run has nothing to plot.
# --force:      re-process files whose reconstruction output already exists,
#               instead of skipping them.
#
# Requires already in place (none of this is automated here):
#   - The 003-ObjectSelectionII production at
#     {STORAGE}/selectionII/{selectionIITag}/{selectionIIHash}/{era}
#     already on disk. The script hard-fails up front if it is not.
#   - scipy, which RecoModule needs and which is NOT in the base conda env --
#     hence the `conda activate latestcoffea` below.
#
# No cut string and no golden JSON are re-applied here; both were already
# handled upstream in selectionII.
#
# Note: run_all.py --writeBashScript names its output scripts/run_all_{TAG}.sh
# -- per tag, not per era. Do not run two eras concurrently under the same
# tag; they would clobber each other's production script. Run them serially.
#
# Sends a Telegram progress message (scripts/send_telegram.py) after each
# major stage, and on failure, so this can be launched inside `screen` and
# tracked without an interactive session watching it.

set -eo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER="$REPO/004A-Reconstruction"

# --- argument parsing -------------------------------------------------------
TAG=""; ERA=""; SEL2_TAG=""; SEL2_HASH=""
SAMPLE=false; WORKERS=15; DELTA_PLOTS=false; FORCE=false

usage() {
    cat <<EOF
Usage: $0 --tag TAG --era ERA --selectionIIHash HASH
          [--selectionIITag SEL2_TAG] [--sample] [--workers N]
          [--makeDeltaPlots] [--force]
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag) TAG="$2"; shift 2 ;;
        --era) ERA="$2"; shift 2 ;;
        --selectionIITag) SEL2_TAG="$2"; shift 2 ;;
        --selectionIIHash) SEL2_HASH="$2"; shift 2 ;;
        --sample) SAMPLE=true; shift ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --makeDeltaPlots) DELTA_PLOTS=true; shift ;;
        --force) FORCE=true; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown argument: $1"; usage ;;
    esac
done

if [[ -z "$TAG" || -z "$ERA" || -z "$SEL2_HASH" ]]; then
    echo "Error: --tag, --era and --selectionIIHash are all required."
    usage
fi

# Default the upstream tag to this chapter's tag -- the common case, where the
# 003 chain and 004A share one tag string.
[[ -z "$SEL2_TAG" ]] && SEL2_TAG="$TAG"

SAMPLE_FLAG=()
$SAMPLE && SAMPLE_FLAG=(--sample)
FORCE_FLAG=()
$FORCE && FORCE_FLAG=(--force)
RUN_LABEL="full"
$SAMPLE && RUN_LABEL="sample"

# Prefixed with 004A_ so these never collide with run_003_workflow.sh's
# run_logs/{TAG}_{ERA}_{RUN_LABEL} for the same tag and era.
LOGDIR="$REPO/run_logs/004A_${TAG}_${ERA}_${RUN_LABEL}"
mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
{
    echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')"
    echo "TAG=$TAG ERA=$ERA SELECTIONII_TAG=$SEL2_TAG SELECTIONII_HASH=$SEL2_HASH SAMPLE=$SAMPLE WORKERS=$WORKERS DELTA_PLOTS=$DELTA_PLOTS FORCE=$FORCE"
} > "$STATUS_FILE"

# send_telegram.py needs requests+dotenv, which live in the base conda env,
# not latestcoffea -- capture the base env's python3 now, before
# `conda activate latestcoffea` below shadows it.
TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[004A $TAG/$ERA $RUN_LABEL] $1" >/dev/null 2>&1 || true
}

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# Print "<n_datasets> <n_files>" for one of the chapter's dataset-map JSONs
# (the {DataMC: {group: {dataset: {path: tree}}}} shape both
# generateDatasetJSON.py outputs use). Exits the script if the file is
# missing or unreadable, so a failed count can never be mistaken downstream
# for a legitimate count of zero.
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
# One --printHash probe gives us both the config hash and the machine-resolved
# STORAGE path, rather than duplicating utils.resolve_storage_path()'s
# hostname matching here. (It also creates outputs/{tag}/{hash} and snapshots
# inputs/, exactly as step [0] would.)
PROBE="$(python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1)"
RECO_HASH="$(echo "$PROBE" | grep 'Config hash:'       | awk '{print $NF}')"
STORAGE="$(  echo "$PROBE" | grep 'Using storage base:' | awk '{print $NF}')"

if [[ -z "$RECO_HASH" || -z "$STORAGE" ]]; then
    echo "Error: could not determine config hash / storage base from run_all.py --printHash:"
    echo "$PROBE"
    exit 1
fi

SEL2_DIR="$STORAGE/selectionII/$SEL2_TAG/$SEL2_HASH/$ERA"
if [[ ! -d "$SEL2_DIR" ]]; then
    # generateDatasetJSON.py would otherwise die on a bare os.listdir() with a
    # raw FileNotFoundError, several frames deep and with no hint as to which
    # tag/hash/era was actually wrong.
    echo "Error: 003-ObjectSelectionII output not found at:"
    echo "  $SEL2_DIR"
    echo "Check --selectionIITag / --selectionIIHash / --era. Available:"
    ls -1 "$STORAGE/selectionII" 2>/dev/null | sed 's/^/  tag: /'
    [[ -d "$STORAGE/selectionII/$SEL2_TAG" ]] && \
        find "$STORAGE/selectionII/$SEL2_TAG" -mindepth 2 -maxdepth 2 -type d \
            -printf '  have: %P\n' 2>/dev/null
    exit 1
fi

log "004A-Reconstruction config hash: $RECO_HASH"
echo "$RECO_HASH" > "$LOGDIR/RECO_HASH.txt"
{
    echo "RECO_HASH=$RECO_HASH"
    echo "STORAGE=$STORAGE"
    echo "SELECTIONII_DIR=$SEL2_DIR"
    echo "RECO_OUTPUT_DIR=$STORAGE/reconstruction/$TAG/$RECO_HASH/$ERA"
} >> "$STATUS_FILE"

notify "Starting ($RUN_LABEL): reading selectionII=$SEL2_TAG/$SEL2_HASH, writing reconstruction/$TAG/$RECO_HASH"

################################################################################
# [0] Build the selectionII dataset map fresh from disk
################################################################################
# Scanned fresh every run (rather than reusing a JSON some earlier invocation
# left in inputs/) so the recorded paths always reflect where the files
# actually are now. Era-filtered: an unfiltered run would also try eras with
# no production under this tag and abort on the first one missing.
log "=== [0] generateSelectionIIDatasetJSON: $ERA (from $SEL2_TAG/$SEL2_HASH) ==="
python3 scripts/run_all.py --tag "$TAG" --generateSelectionIIDatasetJSON \
    --selectionIITag "$SEL2_TAG" --selectionIIHash "$SEL2_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/004A_steps.log"

DATASET_JSON="$CHAPTER/outputs/$TAG/$RECO_HASH/inputs/selectionII_${ERA}_datasets.json"
read -r N_DATASETS N_FILES <<<"$(count_json "$DATASET_JSON")"
if [[ ! "$N_FILES" =~ ^[0-9]+$ ]]; then
    echo "Error: could not count $DATASET_JSON (got '$N_DATASETS' '$N_FILES')."
    exit 1
fi
log "selectionII map: $N_DATASETS datasets, $N_FILES healthy files"
echo "SELECTIONII_DATASETS=$N_DATASETS SELECTIONII_FILES=$N_FILES" >> "$STATUS_FILE"

if [[ "$N_FILES" -eq 0 ]]; then
    # An existing-but-empty era directory, or one whose every ROOT file failed
    # generateDatasetJSON.py's health check. Either way there is nothing to
    # reconstruct, and continuing would "succeed" having done nothing at all.
    echo "Error: no healthy selectionII ROOT files found under $SEL2_DIR."
    echo "Nothing to reconstruct -- check the upstream production and its logs."
    exit 1
fi
notify "[0] selectionII map built: $N_DATASETS datasets, $N_FILES files."

################################################################################
# [1] Build the per-file task list
################################################################################
# Data_mu/MC_mu only: MC_alt is listed in config.yaml's NgenandXsec table but
# has never been produced upstream in this campaign, and naming the groups
# explicitly also keeps step [2] from emitting no-op runReco calls for it.
log "=== [1] generateProcessListJSON: $ERA ($RUN_LABEL) ==="
python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON "${FORCE_FLAG[@]}" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/004A_steps.log"

PROCESS_LIST="$CHAPTER/outputs/$TAG/$RECO_HASH/$ERA/${TAG}_${ERA}_processListJSON.json"
if [[ ! -f "$PROCESS_LIST" ]]; then
    echo "Error: expected process list not found: $PROCESS_LIST"
    exit 1
fi
# --sample is not applied when the process list is built -- run_all.py always
# writes every task and flags the first file of each dataset "isSample", and it
# is runReco.py that filters down to those at run time. So the list length is
# the full count even for a sample run; count the isSample entries instead, or
# the log claims ~40x more work than will actually be done.
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
    log "Nothing to do -- all reconstruction output already exists for this tag/hash."
    log "Pass --force to re-process. Continuing to the dataset map / plots."
fi

################################################################################
# [2] Write the production script
################################################################################
log "=== [2] writeBashScript: $ERA (workers=$WORKERS) ==="
python3 scripts/run_all.py --tag "$TAG" --writeBashScript "${SAMPLE_FLAG[@]}" "${FORCE_FLAG[@]}" \
    --workers "$WORKERS" \
    --filter "$ERA/Data_mu" "$ERA/MC_mu" 2>&1 | tee -a "$LOGDIR/004A_steps.log"

################################################################################
# [3] Production -- the CPU-heavy stage
################################################################################
# One SLSQP minimisation per permutation per event: measured at ~9 Hz per
# worker, so ~8-10M events at --workers 15 is a ~18-20 h pass. Restartable --
# runReco.py skips any input whose _Skim.root already exists.
#
# The production script runs one group at a time, 15-way parallel within each.
# A full pass is therefore throughput-bound (total events / 15 workers). A
# --sample pass is NOT: it gives each group one file per dataset, so a
# single-dataset group (FullyLeptonic, SemiLeptonic, DrellYan) runs one file on
# one core with the other 14 idle, and wall time is set by the largest single
# file rather than the total. Measured: UL2016preVFP --sample took 2h42m for
# 100k events (SemiLeptonic's 43k-event file alone was 78 min of it), against
# ~18 h for the full 8.6M. Do not extrapolate a full-run ETA from a sample.
log "=== [3] Running reconstruction production ($RUN_LABEL, $N_TASKS tasks, ~9 Hz/worker) ==="
bash "scripts/run_all_${TAG}.sh" 2>&1 | tee -a "$LOGDIR/004A_production.log"
notify "[3] reconstruction production done."

################################################################################
# [4] Dataset map of the reconstruction output -- 004B-BDTVariables' input
################################################################################
RECO_OUT_DIR="$STORAGE/reconstruction/$TAG/$RECO_HASH/$ERA"
if [[ ! -d "$RECO_OUT_DIR" ]]; then
    # Every task failed, or production never actually ran. generateDatasetJSON.py
    # would otherwise die on a bare os.listdir() of this path.
    echo "Error: no reconstruction output directory at:"
    echo "  $RECO_OUT_DIR"
    echo "Production produced nothing -- see $LOGDIR/004A_production.log."
    exit 1
fi

log "=== [4] generateDatasetJSON: $ERA ==="
python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/004A_steps.log"

RECO_JSON="$CHAPTER/outputs/$TAG/$RECO_HASH/$ERA/reconstruction_${TAG}_${ERA}_datasets.json"
read -r N_OUT_DATASETS N_OUT_FILES <<<"$(count_json "$RECO_JSON")"
if [[ ! "$N_OUT_FILES" =~ ^[0-9]+$ ]]; then
    echo "Error: could not count $RECO_JSON (got '$N_OUT_DATASETS' '$N_OUT_FILES')."
    exit 1
fi
log "reconstruction map: $N_OUT_DATASETS datasets, $N_OUT_FILES healthy files (input had $N_DATASETS / $N_FILES)"
echo "RECO_DATASETS=$N_OUT_DATASETS RECO_FILES=$N_OUT_FILES" >> "$STATUS_FILE"

# A full pass should reproduce every input file one-for-one. Fewer means some
# files failed or were dropped as unhealthy -- worth flagging loudly, but not
# worth failing the run over: the map is still valid for whatever did land,
# and 004A_production.log has the per-file errors.
if ! $SAMPLE && [[ "$N_OUT_FILES" -ne "$N_FILES" ]]; then
    log "WARNING: $N_OUT_FILES reconstruction files for $N_FILES inputs -- $((N_FILES - N_OUT_FILES)) missing."
    log "         Check $LOGDIR/004A_production.log, then re-run this script to fill the gaps."
    echo "WARNING=reco_file_count_mismatch expected=$N_FILES got=$N_OUT_FILES" >> "$STATUS_FILE"
    notify "[4] WARNING: $N_OUT_FILES/$N_FILES files reconstructed -- $((N_FILES - N_OUT_FILES)) missing."
else
    notify "[4] dataset map done: $N_OUT_DATASETS datasets, $N_OUT_FILES files."
fi

################################################################################
# [5] Reconstructed-vs-generator top-mass residual plots (optional, MC only)
################################################################################
if $DELTA_PLOTS; then
    log "=== [5] makeDeltaPlots: $ERA (ttbar_SemiLeptonic only) ==="
    python3 scripts/run_all.py --tag "$TAG" --makeDeltaPlots \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/004A_steps.log"
    DELTA_DIR="$CHAPTER/outputs/$TAG/$RECO_HASH/$ERA/plots/deltaMass"
    log "Delta-mass plots: $DELTA_DIR"
    echo "DELTA_PLOTS_DIR=$DELTA_DIR" >> "$STATUS_FILE"
    notify "[5] delta-mass plots done: $DELTA_DIR"
else
    log "Skipping delta-mass plots (pass --makeDeltaPlots to build them)."
fi

log "=== DONE ($RUN_LABEL). Reconstruction output: $STORAGE/reconstruction/$TAG/$RECO_HASH/$ERA ==="
log "    004B-BDTVariables input map: $RECO_JSON"
echo "RECO_DATASET_JSON=$RECO_JSON" >> "$STATUS_FILE"

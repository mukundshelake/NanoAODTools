#!/bin/bash
# General, reusable driver for 004C-BDTTraining, for one era at a time:
#   extracts the BDT feature columns + the y training target from the
#   004B-BDTVariables skims into parquet, then (optionally) trains the
#   qqbar-vs-non-qqbar XGBoost classifier for that era.
#
# Usage:
#   ./run_004C_workflow.sh --tag TAG --era ERA --BDTVariablesHash HASH \
#       [--BDTVariablesTag BDTV_TAG] [--sample] [--workers N] \
#       [--trainBDT] [--force]
#
# --BDTVariablesTag: tag of the 004B-BDTVariables production to read.
#               Defaults to --tag, the common case.
# --sample:     quick-look pass. Unlike the other chapters', 004C's --sample
#               works at DATASET granularity: it runs only the first dataset
#               of the era, reading only that dataset's first ROOT file.
#               Everything it writes is named apart from a full pass's output
#               ({dataset}_sample_part{N}.parquet, Parquet_..._sample_
#               datasets.json, bdt/{hash}_sample/), so running --sample and
#               then a full pass under one tag is safe -- the full pass sees
#               no sample artifacts and re-extracts that dataset properly.
#               (It was not safe before: the existing-output check is per
#               dataset, so a sample's single part file made the full pass
#               skip the whole dataset and leave it truncated.)
#               Note that --sample extracts the era's FIRST dataset, which is
#               Data_mu/SingleMuon -- not something you can train on. So when
#               --sample is combined with --trainBDT the extraction is
#               filtered to training_config.yaml's TrainingSample instead, to
#               produce the one dataset the training stage actually reads.
#               Without that, a sample training pass dies on a KeyError for
#               MC_mu with only Data_mu extracted.
# --trainBDT:   after extraction, train the classifier for this era, pinned
#               to the extraction run's own config hash. Off by default --
#               extraction is the expensive, re-usable artifact; training is
#               cheap to repeat and its settings (training_config.yaml) are
#               hashed separately, so re-tuning never forces re-extraction.
# --force:      re-extract datasets whose parquet output already exists (and
#               retrain over an existing model), instead of skipping them.
#
# Requires already in place (none of this is automated here):
#   - The 004B-BDTVariables production at
#     {STORAGE}/BDTVariables/{BDTVariablesTag}/{BDTVariablesHash}/{era}
#     already on disk. The script hard-fails up front if it is not.
#   - The latestcoffea conda env. This chapter is pure-python (uproot /
#     awkward / pyarrow / xgboost): no PostProcessor, no CMSSW, no CRAB.
#
# Note: run_all.py --writeBashScript names its output scripts/run_all_{TAG}.sh
# -- per tag, not per era. Do not run two eras concurrently under the same
# tag; they would clobber each other's extraction script. Run them serially.
# The chapter-level inputs/BDTVariables_{era}_datasets.json that step [0]
# writes is keyed by era but not by tag, with the same caveat as 004B's.
#
# Sends a Telegram progress message (scripts/send_telegram.py) after each
# major stage, and on failure, so this can be launched inside `screen` and
# tracked without an interactive session watching it.

set -eo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER="$REPO/004C-BDTTraining"

# --- argument parsing -------------------------------------------------------
TAG=""; ERA=""; BDTV_TAG=""; BDTV_HASH=""
SAMPLE=false; WORKERS=8; TRAIN=false; FORCE=false

usage() {
    cat <<EOF
Usage: $0 --tag TAG --era ERA --BDTVariablesHash HASH
          [--BDTVariablesTag BDTV_TAG] [--sample] [--workers N]
          [--trainBDT] [--force]
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag) TAG="$2"; shift 2 ;;
        --era) ERA="$2"; shift 2 ;;
        --BDTVariablesTag) BDTV_TAG="$2"; shift 2 ;;
        --BDTVariablesHash) BDTV_HASH="$2"; shift 2 ;;
        --sample) SAMPLE=true; shift ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --trainBDT) TRAIN=true; shift ;;
        --force) FORCE=true; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown argument: $1"; usage ;;
    esac
done

if [[ -z "$TAG" || -z "$ERA" || -z "$BDTV_HASH" ]]; then
    echo "Error: --tag, --era and --BDTVariablesHash are all required."
    usage
fi

[[ -z "$BDTV_TAG" ]] && BDTV_TAG="$TAG"

SAMPLE_FLAG=()
$SAMPLE && SAMPLE_FLAG=(--sample)
FORCE_FLAG=()
$FORCE && FORCE_FLAG=(--force)
RUN_LABEL="full"
$SAMPLE && RUN_LABEL="sample"
JSON_SUFFIX=""
$SAMPLE && JSON_SUFFIX="_sample"

LOGDIR="$REPO/run_logs/004C_${TAG}_${ERA}_${RUN_LABEL}"
mkdir -p "$LOGDIR"
STATUS_FILE="$LOGDIR/STATUS.txt"
{
    echo "STARTED $(date '+%Y-%m-%d %H:%M:%S')"
    echo "TAG=$TAG ERA=$ERA BDTVARIABLES_TAG=$BDTV_TAG BDTVARIABLES_HASH=$BDTV_HASH SAMPLE=$SAMPLE WORKERS=$WORKERS TRAIN=$TRAIN FORCE=$FORCE"
} > "$STATUS_FILE"

TELEGRAM_PY="/home/mukund/miniconda3/bin/python3"
notify() {
    "$TELEGRAM_PY" "$REPO/scripts/send_telegram.py" "[004C $TAG/$ERA $RUN_LABEL] $1" >/dev/null 2>&1 || true
}

trap 'ec=$?; echo "EXIT_CODE=$ec at $(date "+%Y-%m-%d %H:%M:%S") (line $LINENO)" >> "$STATUS_FILE"; if [ $ec -eq 0 ]; then echo "RESULT=SUCCESS" >> "$STATUS_FILE"; notify "DONE (success)."; else echo "RESULT=FAILED" >> "$STATUS_FILE"; notify "FAILED at line $LINENO (exit $ec). See $LOGDIR"; fi' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# Print "<n_datasets> <n_files> <n_rows>" for one of this chapter's dataset
# maps. Both the input map (004B ROOT files) and the output map (parquet
# parts) store a ROW COUNT as each file's value, which is what makes the
# end-to-end event-count check in step [4] possible -- parts do not
# correspond one-to-one with input files, so counting files would prove
# nothing. Exits the script if the file is missing or unreadable, so a
# failed count can never be mistaken downstream for a legitimate zero.
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
datasets = files = rows = 0
for data_mc in payload.values():
    for group in data_mc.values():
        for entries in group.values():
            datasets += 1
            files += len(entries)
            rows += sum(v for v in entries.values() if isinstance(v, int))
print(datasets, files, rows)
PY
}

# --- environment setup (self-contained: do not rely on an already-activated shell) ---
source /home/mukund/miniconda3/etc/profile.d/conda.sh
conda activate latestcoffea

cd "$CHAPTER"

# --- pre-flight -------------------------------------------------------------
PROBE="$(python3 scripts/run_all.py --tag "$TAG" --printHash 2>&1)"
PARQUET_HASH="$(echo "$PROBE" | grep 'Config hash:'        | awk '{print $NF}')"
STORAGE="$(     echo "$PROBE" | grep 'Using storage base:' | awk '{print $NF}')"

if [[ -z "$PARQUET_HASH" || -z "$STORAGE" ]]; then
    echo "Error: could not determine config hash / storage base from run_all.py --printHash:"
    echo "$PROBE"
    exit 1
fi

BDTV_DIR="$STORAGE/BDTVariables/$BDTV_TAG/$BDTV_HASH/$ERA"
if [[ ! -d "$BDTV_DIR" ]]; then
    echo "Error: 004B-BDTVariables output not found at:"
    echo "  $BDTV_DIR"
    echo "Check --BDTVariablesTag / --BDTVariablesHash / --era. Available:"
    ls -1 "$STORAGE/BDTVariables" 2>/dev/null | sed 's/^/  tag: /'
    [[ -d "$STORAGE/BDTVariables/$BDTV_TAG" ]] && \
        find "$STORAGE/BDTVariables/$BDTV_TAG" -mindepth 2 -maxdepth 2 -type d \
            -printf '  have: %P\n' 2>/dev/null
    exit 1
fi

log "004C-BDTTraining config hash: $PARQUET_HASH"
echo "$PARQUET_HASH" > "$LOGDIR/PARQUET_HASH.txt"
{
    echo "PARQUET_HASH=$PARQUET_HASH"
    echo "STORAGE=$STORAGE"
    echo "BDTVARIABLES_DIR=$BDTV_DIR"
    echo "PARQUET_OUTPUT_DIR=$STORAGE/BDTParquet/$TAG/$PARQUET_HASH/$ERA"
} >> "$STATUS_FILE"

# A sample pass normally covers the era's first dataset. That is Data_mu,
# which the training stage cannot use, so a --sample --trainBDT run has to aim
# the extraction at training_config.yaml's TrainingSample instead. Read it
# from that config rather than hard-coding it, so the two stay in step.
EXTRACT_FILTER="$ERA"
if $SAMPLE && $TRAIN; then
    TRAINING_TARGET="$(python3 - "$CHAPTER/training_config.yaml" <<'PYCFG'
import sys, yaml
ts = yaml.safe_load(open(sys.argv[1]))["TrainingSample"]
print(f"{ts['DataMC']}/{ts['group']}/{ts['dataset']}")
PYCFG
)"
    if [[ -z "$TRAINING_TARGET" ]]; then
        echo "Error: could not read TrainingSample from $CHAPTER/training_config.yaml"
        exit 1
    fi
    EXTRACT_FILTER="$ERA/$TRAINING_TARGET"
    log "--sample with --trainBDT: extracting $EXTRACT_FILTER (the training sample) rather than the era's first dataset."
    echo "SAMPLE_EXTRACT_FILTER=$EXTRACT_FILTER" >> "$STATUS_FILE"
fi

notify "Starting ($RUN_LABEL): reading BDTVariables=$BDTV_TAG/$BDTV_HASH, writing BDTParquet/$TAG/$PARQUET_HASH"

################################################################################
# [0] Build the 004B dataset map fresh from disk
################################################################################
log "=== [0] generateBDTVariablesDatasetJSON: $ERA (from $BDTV_TAG/$BDTV_HASH) ==="
python3 scripts/run_all.py --tag "$TAG" --generateBDTVariablesDatasetJSON \
    --BDTVariablesTag "$BDTV_TAG" --BDTVariablesHash "$BDTV_HASH" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/004C_steps.log"

INPUT_JSON="$CHAPTER/outputs/$TAG/$PARQUET_HASH/inputs/BDTVariables_${ERA}_datasets.json"
read -r N_DATASETS N_FILES N_ROWS <<<"$(count_json "$INPUT_JSON")"
if [[ ! "$N_FILES" =~ ^[0-9]+$ ]]; then
    echo "Error: could not count $INPUT_JSON (got '$N_DATASETS' '$N_FILES' '$N_ROWS')."
    exit 1
fi
log "BDTVariables map: $N_DATASETS datasets, $N_FILES files, $N_ROWS events"
echo "BDTVARIABLES_DATASETS=$N_DATASETS BDTVARIABLES_FILES=$N_FILES BDTVARIABLES_ROWS=$N_ROWS" >> "$STATUS_FILE"

if [[ "$N_FILES" -eq 0 ]]; then
    # This exact failure shipped: the chapter's generateDatasetJSON.py only
    # matched *.parquet, so scanning 004B's *_Skim.root output produced a map
    # of N datasets containing zero files -- and still exited 0, leaving every
    # later step to "succeed" having done nothing.
    echo "Error: mapped $N_DATASETS datasets but 0 files under $BDTV_DIR."
    echo "Nothing to extract -- check the 004B production and its logs."
    exit 1
fi
notify "[0] BDTVariables map built: $N_DATASETS datasets, $N_FILES files, $N_ROWS events."

################################################################################
# [1] Build the per-dataset task list
################################################################################
# No group filter needed here (unlike 004A/004B): this step iterates the map
# built in [0], which only contains what is actually on disk, rather than
# config.yaml's group table with its never-produced MC_alt entries.
log "=== [1] generateProcessListJSON: $ERA ($RUN_LABEL) ==="
python3 scripts/run_all.py --tag "$TAG" --generateProcessListJSON "${FORCE_FLAG[@]}" "${SAMPLE_FLAG[@]}" \
    --filter "$EXTRACT_FILTER" 2>&1 | tee -a "$LOGDIR/004C_steps.log"

PROCESS_LIST="$CHAPTER/outputs/$TAG/$PARQUET_HASH/$ERA/${TAG}_${ERA}_processListJSON.json"
if [[ ! -f "$PROCESS_LIST" ]]; then
    echo "Error: expected process list not found: $PROCESS_LIST"
    exit 1
fi
# One task = one dataset here, not one file, so this count is datasets.
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
log "process list: $N_TASKS dataset task(s) to run (of $N_DATASETS datasets, $N_FILES files)"
echo "TASKS=$N_TASKS" >> "$STATUS_FILE"

if [[ "$N_TASKS" -eq 0 ]]; then
    log "Nothing to do -- all parquet output already exists for this tag/hash."
    log "Pass --force to re-extract. Continuing to the dataset map."
fi

################################################################################
# [2] Write the extraction script
################################################################################
log "=== [2] writeBashScript: $ERA (workers=$WORKERS) ==="
python3 scripts/run_all.py --tag "$TAG" --writeBashScript "${SAMPLE_FLAG[@]}" "${FORCE_FLAG[@]}" \
    --workers "$WORKERS" \
    --filter "$EXTRACT_FILTER" 2>&1 | tee -a "$LOGDIR/004C_steps.log"

EXTRACT_SCRIPT="$CHAPTER/scripts/run_all_${TAG}.sh"
if [[ ! -f "$EXTRACT_SCRIPT" ]]; then
    echo "Error: expected extraction script not found: $EXTRACT_SCRIPT"
    exit 1
fi

################################################################################
# [3] Extraction
################################################################################
# Parallelism is per dataset, so the era's wall time is set by its largest
# single dataset (ttbar_SemiLeptonic) rather than by the total -- more workers
# than datasets buys nothing. Memory, not CPU, is the constraint worth
# watching: each worker accumulates up to MaxEventsPerParquet rows in memory
# before flushing a part file.
log "=== [3] Running parquet extraction ($RUN_LABEL, $N_TASKS dataset task(s), workers=$WORKERS) ==="
bash "$EXTRACT_SCRIPT" 2>&1 | tee -a "$LOGDIR/004C_extraction.log"
notify "[3] parquet extraction done."

################################################################################
# [4] Dataset map of the parquet output
################################################################################
PARQUET_OUT_DIR="$STORAGE/BDTParquet/$TAG/$PARQUET_HASH/$ERA"
if [[ ! -d "$PARQUET_OUT_DIR" ]]; then
    echo "Error: no BDTParquet output directory at:"
    echo "  $PARQUET_OUT_DIR"
    echo "Extraction produced nothing -- see $LOGDIR/004C_extraction.log."
    exit 1
fi

log "=== [4] generateDatasetJSON: $ERA ==="
python3 scripts/run_all.py --tag "$TAG" --generateDatasetJSON "${SAMPLE_FLAG[@]}" \
    --filter "$ERA" 2>&1 | tee -a "$LOGDIR/004C_steps.log"

PARQUET_JSON="$CHAPTER/outputs/$TAG/$PARQUET_HASH/$ERA/Parquet_${TAG}_${ERA}${JSON_SUFFIX}_datasets.json"
read -r N_OUT_DATASETS N_OUT_PARTS N_OUT_ROWS <<<"$(count_json "$PARQUET_JSON")"
if [[ ! "$N_OUT_PARTS" =~ ^[0-9]+$ ]]; then
    echo "Error: could not count $PARQUET_JSON (got '$N_OUT_DATASETS' '$N_OUT_PARTS' '$N_OUT_ROWS')."
    exit 1
fi
log "parquet map: $N_OUT_DATASETS datasets, $N_OUT_PARTS part file(s), $N_OUT_ROWS events (input had $N_DATASETS / $N_FILES / $N_ROWS)"
echo "PARQUET_DATASETS=$N_OUT_DATASETS PARQUET_PARTS=$N_OUT_PARTS PARQUET_ROWS=$N_OUT_ROWS" >> "$STATUS_FILE"

# Events, not files: parquet parts are re-chunked at MaxEventsPerParquet
# boundaries and deliberately do not correspond to input files. A full pass
# must carry every event through; anything less means a dataset failed or was
# silently truncated.
if ! $SAMPLE && [[ "$N_OUT_ROWS" -ne "$N_ROWS" ]]; then
    log "WARNING: extracted $N_OUT_ROWS events for $N_ROWS input events -- $((N_ROWS - N_OUT_ROWS)) missing."
    log "         Check $LOGDIR/004C_extraction.log, then re-run this script to fill the gaps."
    echo "WARNING=parquet_row_count_mismatch expected=$N_ROWS got=$N_OUT_ROWS" >> "$STATUS_FILE"
    notify "[4] WARNING: $N_OUT_ROWS/$N_ROWS events extracted -- $((N_ROWS - N_OUT_ROWS)) missing."
else
    notify "[4] parquet map done: $N_OUT_DATASETS datasets, $N_OUT_PARTS parts, $N_OUT_ROWS events."
fi

################################################################################
# [5] Train the qqbar-vs-non-qqbar classifier (optional)
################################################################################
# Deliberately non-fatal, like 004B's plot stage: by this point the parquet --
# the expensive, reusable artifact every later training depends on -- is
# already on disk and mapped, and a grid-search or plotting failure should not
# mark the extraction run FAILED.
if $TRAIN; then
    log "=== [5] trainBDT: $ERA (parquetHash=$PARQUET_HASH) ==="
    TRAIN_OK=true
    python3 scripts/run_all.py --tag "$TAG" --trainBDT --parquetHash "$PARQUET_HASH" \
        "${SAMPLE_FLAG[@]}" "${FORCE_FLAG[@]}" \
        --filter "$ERA" 2>&1 | tee -a "$LOGDIR/004C_training.log" || TRAIN_OK=false
    if $TRAIN_OK; then
        TRAINING_HASH="$(grep -a 'Training config hash:' "$LOGDIR/004C_training.log" | tail -1 | awk '{print $NF}')"
        BDT_DIR="$CHAPTER/outputs/$TAG/$PARQUET_HASH/$ERA/bdt/${TRAINING_HASH}${JSON_SUFFIX}"
        log "BDT training artifacts: $BDT_DIR"
        echo "TRAINING_HASH=$TRAINING_HASH" >> "$STATUS_FILE"
        echo "BDT_DIR=$BDT_DIR" >> "$STATUS_FILE"
        notify "[5] BDT training done: $BDT_DIR"
    else
        log "WARNING: BDT training failed -- the parquet extraction is unaffected."
        log "         See $LOGDIR/004C_training.log."
        echo "WARNING=training_failed" >> "$STATUS_FILE"
        notify "[5] WARNING: BDT training failed (parquet output is fine). See $LOGDIR"
    fi
else
    log "Skipping BDT training (pass --trainBDT to run it)."
fi

log "=== DONE ($RUN_LABEL). Parquet output: $PARQUET_OUT_DIR ==="
log "    Parquet map: $PARQUET_JSON"
echo "PARQUET_DATASET_JSON=$PARQUET_JSON" >> "$STATUS_FILE"

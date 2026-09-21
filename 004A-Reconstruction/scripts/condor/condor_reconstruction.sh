#!/bin/bash
# 004A-Reconstruction/scripts/condor/condor_reconstruction.sh
#
# HTCondor executable for the reconstruction stage -- the condor equivalent
# of scripts/crab/crab_reconstruction.sh. Every path below is passed in as an
# absolute /eos/... argument (nothing is condor-transferred): EOS is a real
# mounted filesystem on lxbatch worker nodes, so there is no need to fight
# eossubmit's "no input directories, only same-filename-across-cluster
# transfers" restrictions -- just read/write EOS directly by absolute path
# from inside the job. Same approach as 002-Samples' condor path.
#
# Args: job_id era config_yaml files_json is_data out_dir worker_py chapter_dir modules_dir

set -e

JOB_ID=$1
ERA=$2
CONFIG_YAML=$3
FILES_JSON=$4
IS_DATA=$5
OUT_DIR=$6
WORKER_PY=$7
CHAPTER_DIR=$8
MODULES_DIR=$9

echo "JOB_ID=$JOB_ID ERA=$ERA hostname=$(hostname) date=$(date)"
echo "CONFIG_YAML=$CONFIG_YAML FILES_JSON=$FILES_JSON IS_DATA=$IS_DATA"
echo "OUT_DIR=$OUT_DIR WORKER_PY=$WORKER_PY CHAPTER_DIR=$CHAPTER_DIR"
echo "SCRATCH=$_CONDOR_SCRATCH_DIR PWD=$(pwd)"

echo "Setting up CMSSW environment..."
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /eos/user/m/mshelake/Analysis/CMSSW_13_3_0/src
cmsenv
cd "$_CONDOR_SCRATCH_DIR"
echo "CMSSW_BASE=$CMSSW_BASE"

# The worker looks for ./config.yaml in its cwd (same convention as the CRAB
# worker), and imports its modules as flat top-level names -- so put both
# within reach of the scratch directory rather than relying on the package
# layout, which is not installed.
cp "$CONFIG_YAML" config.yaml
cp "$MODULES_DIR"/*.py .
# Run the worker from the scratch directory rather than by absolute path:
# python puts sys.path[0] at the *script's* directory, so invoking it as
# "$WORKER_PY" makes the flat module copies above invisible to it
# (confirmed live: ModuleNotFoundError: No module named 'SelectedObjects').
cp "$WORKER_PY" .

echo "Running reconstruction..."
python3 "./$(basename "$WORKER_PY")" "$ERA" "$FILES_JSON" "$IS_DATA" "$CHAPTER_DIR"

# A job whose input files all fail the cut string legitimately produces no
# output (the worker says so and exits 0). Treat that as success, not as a
# missing-output failure -- it is the same 0-event case runSelection.py and
# consolidateCrabOutput.py both already handle explicitly.
SKIM=$(ls *_Skim.root 2>/dev/null | head -1)
if [ -z "$SKIM" ]; then
    echo "No *_Skim.root produced (no events survived); nothing to copy."
    echo "DONE condor_reconstruction.sh"
    exit 0
fi

mkdir -p "$OUT_DIR"
OUT_PATH="$OUT_DIR/${JOB_ID}.root"
cp "$SKIM" "$OUT_PATH"
echo "Copied $SKIM -> $OUT_PATH"
echo "DONE condor_reconstruction.sh"

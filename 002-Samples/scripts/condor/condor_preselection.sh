#!/bin/bash
# 002-Samples/scripts/condor/condor_preselection.sh
#
# HTCondor executable for the preselection stage -- the condor equivalent of
# scripts/crab/crab_preselection.sh. Every path below is passed in as an
# absolute /eos/... argument (nothing is condor-transferred): EOS is a real
# mounted filesystem on lxbatch worker nodes (confirmed via a live test), so
# there is no need to fight eossubmit's "no input directories, only
# same-filename-across-cluster transfers" restrictions -- just read/write
# EOS directly by absolute path from inside the job.
#
# Args: job_id era config_yaml files_json golden_json_or_NONE is_data out_dir worker_py

set -e

JOB_ID=$1
ERA=$2
CONFIG_YAML=$3
FILES_JSON=$4
GOLDEN_JSON=$5
IS_DATA=$6
OUT_DIR=$7
WORKER_PY=$8

echo "JOB_ID=$JOB_ID ERA=$ERA hostname=$(hostname) date=$(date)"
echo "CONFIG_YAML=$CONFIG_YAML FILES_JSON=$FILES_JSON GOLDEN_JSON=$GOLDEN_JSON IS_DATA=$IS_DATA"
echo "OUT_DIR=$OUT_DIR WORKER_PY=$WORKER_PY"
echo "SCRATCH=$_CONDOR_SCRATCH_DIR PWD=$(pwd)"

echo "Setting up CMSSW environment..."
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /eos/user/m/mshelake/Analysis/CMSSW_13_3_0/src
cmsenv
cd "$_CONDOR_SCRATCH_DIR"
echo "CMSSW_BASE=$CMSSW_BASE"

# condor_script_preselection.py looks for ./config.yaml in its cwd (same
# convention as crab_script_preselection.py under CRAB).
cp "$CONFIG_YAML" config.yaml

echo "Running NanoAODTools preselection..."
python3 "$WORKER_PY" "$ERA" "$FILES_JSON" "$GOLDEN_JSON" "$IS_DATA"

SKIM=$(ls *_Skim.root 2>/dev/null | head -1)
if [ -z "$SKIM" ]; then
    echo "ERROR: no *_Skim.root output produced"
    exit 1
fi

mkdir -p "$OUT_DIR"
OUT_PATH="$OUT_DIR/${JOB_ID}.root"
cp "$SKIM" "$OUT_PATH"
echo "Copied $SKIM -> $OUT_PATH"
echo "DONE condor_preselection.sh"

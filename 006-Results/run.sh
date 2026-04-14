#!/bin/bash
# run.sh
# Run the getObservables pipeline for UL2016preVFP.
#
# To enable weight-based systematic variations (adds ~226 histograms per MC dataset),
# set SYST="--syst" before running, or pass it directly:
#
#   SYST="--syst" ./run.sh
#
# Produces one .coffea file per script under Outputs/.

CONDA_ENV="latestcoffea"
PYTHON="conda run -n ${CONDA_ENV} python3"

# Set to "--syst" to activate systematics mode, empty string for nominal only.
SYST="${SYST:-}"

$PYTHON scripts/getObservables_reco.py \
    --era UL2016preVFP \
    --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \
    --output_folder Outputs/reco_bdt \
    --bdt_cut 0.55 \
    $SYST


$PYTHON scripts/getObservables_reco_woutBDT.py \
    --era UL2016preVFP \
    --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \
    --output_folder Outputs/reco_woutbdt \
    $SYST


$PYTHON scripts/getObservables_gen.py \
    --era UL2016preVFP \
    --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \
    --output_folder Outputs/gen \
    $SYST

#!/bin/bash
CONDA_ENV="latestcoffea"
PYTHON="conda run -n ${CONDA_ENV} python3"

$PYTHON scripts/getObservables_reco.py \
    --era UL2016preVFP \
    --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \
    --output_folder Outputs \
    --output_name reco_bdt_UL2016preVFP \
    --bdt_cut 0.55 \


$PYTHON scripts/getObservables_reco_woutBDT.py \
    --era UL2016preVFP \
    --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \
    --output_folder Outputs \
    --output_name reco_woutbdt_UL2016preVFP 


$PYTHON scripts/getObservables_gen.py \
    --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \
    --output_folder Outputs \
    --output_name gen_UL2016preVFP 
#!/bin/bash

echo "ENV..................................."
env

echo "VOMS"
voms-proxy-info -all

echo "CMSSW BASE, python path, pwd"
echo $CMSSW_BASE
echo $PYTHONPATH
echo $PWD

echo "Running NanoAODTools preselection..."
python3 crab_script_preselection.py $@

# CRAB expects tree.root — rename whatever _Skim.root was produced
SKIM=$(ls *_Skim.root 2>/dev/null | head -1)
if [ -n "$SKIM" ]; then
    echo "Renaming $SKIM → tree.root"
    mv "$SKIM" tree.root
fi

echo "DONE with NanoAODTools preselection"

#!/bin/bash

_BASE=/home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018
mkdir -p \
    "$_BASE/Data_mu/SingleMuon" \
    "$_BASE/MC_alt/FullyLeptonic" \
    "$_BASE/MC_alt/SemiLeptonic" \
    "$_BASE/MC_alt/Tbarchannel" \
    "$_BASE/MC_alt/Tchannel" \
    "$_BASE/MC_mu/Diboson" \
    "$_BASE/MC_mu/DrellYan" \
    "$_BASE/MC_mu/FullyLeptonic" \
    "$_BASE/MC_mu/QCD" \
    "$_BASE/MC_mu/SemiLeptonic" \
    "$_BASE/MC_mu/SingleTop" \
    "$_BASE/MC_mu/WJets"

# Copy SFs into the run folder so relative paths in the processListJSON resolve correctly
_RUN_DIR=/home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4
_SFS_SRC=/home/mukund/Projects/PhysicsTools/NanoAODTools/SFs
cp -rn "$_SFS_SRC" "$_RUN_DIR/SFs"

python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/Data_mu/SingleMuon 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/Data_mu/SingleMuon/earlyApril_UL2018_Data_mu_SingleMuon.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_mu/Diboson 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_mu/Diboson/earlyApril_UL2018_MC_mu_Diboson.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_mu/DrellYan 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_mu/DrellYan/earlyApril_UL2018_MC_mu_DrellYan.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_mu/FullyLeptonic 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_mu/FullyLeptonic/earlyApril_UL2018_MC_mu_FullyLeptonic.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_mu/QCD 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_mu/QCD/earlyApril_UL2018_MC_mu_QCD.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_mu/SemiLeptonic 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_mu/SemiLeptonic/earlyApril_UL2018_MC_mu_SemiLeptonic.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_mu/SingleTop 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_mu/SingleTop/earlyApril_UL2018_MC_mu_SingleTop.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_mu/WJets 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_mu/WJets/earlyApril_UL2018_MC_mu_WJets.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_alt/SemiLeptonic 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_alt/SemiLeptonic/earlyApril_UL2018_MC_alt_SemiLeptonic.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_alt/Tbarchannel 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_alt/Tbarchannel/earlyApril_UL2018_MC_alt_Tbarchannel.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_alt/Tchannel 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_alt/Tchannel/earlyApril_UL2018_MC_alt_Tchannel.log
python /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/scripts/runSelectionII.py --processListJSON /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/earlyApril_UL2018_processListJSON.json --workers 15 --filter UL2018/MC_alt/FullyLeptonic 2>&1 | tee -a /home/mukund/Projects/PhysicsTools/NanoAODTools/003-ObjectSelectionII/outputs/earlyApril/ee9987b872c4/UL2018/MC_alt/FullyLeptonic/earlyApril_UL2018_MC_alt_FullyLeptonic.log

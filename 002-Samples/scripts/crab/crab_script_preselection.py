#!/usr/bin/env python
"""
CRAB worker script for the preselection stage.
Applies kinematic cuts + keep/drop branch selection to NanoAOD files.
No analysis modules are run at this stage.

This script is sent to the grid worker node as an inputFile and executed by
crab_preselection.sh.  The PostProcessor framework is available because
sendPythonFolder=True installs it into the CMSSW environment.

For Data jobs, runsAndLumis() returns the per-job lumi list (from the golden
JSON lumi mask set in the CRAB config), so jsonInput provides within-file
lumisection filtering.  For MC jobs, runsAndLumis() returns None, which
means no lumi filtering is applied.
"""

import os
import sys
import yaml
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from PhysicsTools.NanoAODTools.postprocessing.framework.crabhelper import inputFiles, runsAndLumis

print("Running crab_script_preselection.py")


files = inputFiles()
print("INPUT FILES:", files)

# Select cuts based on era (passed as 'era=VALUE' argument from crab_preselection.sh)
era = "UL2016preVFP"
for arg in sys.argv[1:]:
    if arg.startswith("era="):
        era = arg.split("=", 1)[1]
        break

# Load cuts from config.yaml (sent as an inputFile by submit_preselection_flexible.py)
with open("config.yaml") as _f:
    _config = yaml.safe_load(_f)

_presel_cuts = _config.get("Pre-SelectionCuts", {})
if era not in _presel_cuts:
    print(f"[WARNING] Era '{era}' not found in config.yaml Pre-SelectionCuts, falling back to UL2016preVFP.")
    era = "UL2016preVFP"

_era_cuts = _presel_cuts[era]
cuts = [cut for cut in _era_cuts.values()]

print("Cuts for era", era)
for cut in cuts:
    print(" ", cut)

# Generate keep_and_drop.txt from config.yaml branch_selection
_branchsel = _config.get("branch_selection", {})
with open("keep_and_drop.txt", "w") as _kd:
    for branch in _branchsel.get("drop", []):
        _kd.write(f"drop {branch}\n")
    for branch in _branchsel.get("keep", []):
        _kd.write(f"keep {branch}\n")
print("Generated keep_and_drop.txt from config.yaml")

p = PostProcessor(
    ".",
    inputFiles(),
    cut=" && ".join(cuts),
    # outputfile="tree.root",
    branchsel="keep_and_drop.txt",
    provenance=True,
    fwkJobReport=True,  
    modules=[],
    jsonInput=runsAndLumis(),
)
print("Starting PostProcessor")
p.run()
print("Finished PostProcessor")
print("DONE crab_script_preselection.py")

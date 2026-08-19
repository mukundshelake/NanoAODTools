#!/usr/bin/env python
"""
CRAB worker script for the BDT-variable stage (004B-BDT). Mirrors
scripts/runBDTVariables.py's process_file() exactly: no cut string, no
golden JSON (both already applied upstream) -- just runs BDTvariableModule
over the reconstruction skim.

This script is sent to the grid worker node as an inputFile and executed by
crab_bdt.sh. BDTvariableModule.py is shipped alongside it (flat, no
modules/ subpackage) since it isn't part of the installed NanoAODTools
package.

NOTE on input file resolution: same as crab_script_selection.py in
003-ObjectSelectionI -- the /store/... LFN CRAB assigns us is translated
directly to its CERNBox xrootd door (root://eosuser.cern.ch/) using this
chapter's own LFN_Base, bypassing crabhelper.inputFiles() (which assumes
/store/... resolves to /eos/cms/..., not CERNBox).

Unlike 004A-Reconstruction's crab_script_reconstruction.py, no LCG sys.path
fix is needed here -- BDTvariableModule only imports numpy, which is part of
the stock CMSSW python environment (unlike scipy/correctionlib/coffea/
awkward, which are not).
"""

import sys
import yaml

import PSet
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from BDTvariableModule import BDTvariableModule

print("Running crab_script_bdt.py")

with open("config.yaml") as _f:
    _config = yaml.safe_load(_f)


def lfn_to_eosuser_xrootd(lfn, lfn_base):
    """Convert a /store/user/<username>/... LFN to root://eosuser.cern.ch/..."""
    lfn_base = lfn_base.rstrip('/')
    if not lfn.startswith(lfn_base):
        raise ValueError(f"LFN '{lfn}' does not start with configured LFN_Base '{lfn_base}'")
    parts = lfn_base.strip('/').split('/')  # ['store', 'user', '<username>', ...rest]
    username = parts[2]
    eos_base = '/'.join(['/eos/user', username[0], username] + parts[3:])
    return f"root://eosuser.cern.ch/{eos_base}{lfn[len(lfn_base):]}"


raw_lfns = list(PSet.process.source.fileNames)
files = [lfn_to_eosuser_xrootd(lfn, _config["LFN_Base"]) for lfn in raw_lfns]
print("INPUT FILES (raw LFNs):", raw_lfns)
print("INPUT FILES (resolved):", files)

# scriptArgs: isData=True/False (passed by submit_bdt_flexible.py); bdt_variables
# takes no config, so era isn't needed at runtime, but kept for parity/logging.
is_data = False
for arg in sys.argv[1:]:
    if arg.startswith("isData="):
        is_data = arg.split("=", 1)[1].strip().lower() == "true"

print(f"isData={is_data}")

p = PostProcessor(
    ".",
    files,
    cut=None,
    jsonInput=None,
    branchsel=None,
    modules=[BDTvariableModule()],
    noOut=False,
    justcount=False,
    compression="ZLIB:9",
    provenance=True,
    fwkJobReport=True,
)
print("Starting PostProcessor")
p.run()
print("Finished PostProcessor")
print("DONE crab_script_bdt.py")

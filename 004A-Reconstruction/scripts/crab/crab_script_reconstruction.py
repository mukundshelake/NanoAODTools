#!/usr/bin/env python
"""
CRAB worker script for the reconstruction stage (004A-Reconstruction).
Mirrors scripts/runReco.py's process_file() exactly: no cut string, no golden
JSON (both already applied upstream in selectionII) -- just runs RecoModule
over the selectionII skim.

This script is sent to the grid worker node as an inputFile and executed by
crab_reconstruction.sh. RecoModule.py is shipped alongside it (flat, no
modules/ subpackage) since it isn't part of the installed NanoAODTools
package.

NOTE on input file resolution: same as crab_script_selection.py in
003-ObjectSelectionI -- the /store/... LFN CRAB assigns us is translated
directly to its CERNBox xrootd door (root://eosuser.cern.ch/) using this
chapter's own LFN_Base, bypassing crabhelper.inputFiles() (which assumes
/store/... resolves to /eos/cms/..., not CERNBox).

NOTE on scipy: RecoModule's chi2 fit uses scipy.optimize.minimize, which is
NOT part of the stock CMSSW release's python environment -- same situation as
correctionlib/coffea/awkward in 003-ObjectSelectionII's CRAB worker script
(only available on lxplus via the user's own AFS pip install, which a grid
worker node cannot see). Reuse the same fix: prepend the CVMFS-hosted LCG
software stack's site-packages to sys.path (not a full `source .../setup.sh`,
which would risk swapping out CMSSW's own ROOT build).
"""

import os
import sys
import yaml

_LCG_VIEW = "/cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc12-opt"
for _lib in ("lib64", "lib"):
    _p = f"{_LCG_VIEW}/{_lib}/python3.9/site-packages"
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

import PSet
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from RecoModule import RecoModule

print("Running crab_script_reconstruction.py")

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

# scriptArgs: era=VALUE, isData=True/False (passed by submit_reconstruction_flexible.py)
era = None
is_data = False
for arg in sys.argv[1:]:
    if arg.startswith("era="):
        era = arg.split("=", 1)[1]
    elif arg.startswith("isData="):
        is_data = arg.split("=", 1)[1].strip().lower() == "true"

if era is None:
    raise RuntimeError("crab_script_reconstruction.py requires scriptArgs era=<era>")

print(f"era={era}, isData={is_data}")

# Same module config the local process-list JSON carries. reconstruction runs
# identically for MC and Data (ModuleList.MC == ModuleList.Data == [reconstruction]).
mod_cfg = _config["Modules"]["reconstruction"]

p = PostProcessor(
    ".",
    files,
    cut=None,
    jsonInput=None,
    branchsel=None,
    modules=[RecoModule(era, mod_cfg)],
    noOut=False,
    justcount=False,
    compression="ZLIB:9",
    provenance=True,
    fwkJobReport=True,
)
print("Starting PostProcessor")
p.run()
print("Finished PostProcessor")
print("DONE crab_script_reconstruction.py")

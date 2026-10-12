#!/usr/bin/env python
"""
CRAB worker script for the reconstruction stage (004A-Reconstruction).
Mirrors scripts/runReco.py's process_file() exactly: no cut string, no golden
JSON (both already applied upstream in selectionII) -- just runs RecoModule
over the selectionII skim.

This script is sent to the grid worker node as an inputFile and executed by
crab_reconstruction.sh. Shipped alongside it, flat in the job directory (see
submit_reconstruction_flexible.py's sandbox_files()):
  * RecoModule.py -- not part of the installed NanoAODTools package;
  * config.yaml;
  * HitFit: hitfit_loader.py, libHitFit.so, HitFitWrapper.h and the resolution
    tables named in config.yaml. libHitFit.so must have been built with
    `cmsenv && external/HitFit/build.sh` in the CMSSW area the jobs are
    submitted from: the job runs in that same release, whose CLHEP the library
    links against (via RPATH on CVMFS).

NOTE on input file resolution: same as crab_script_selection.py in
003-ObjectSelectionI -- the /store/... LFN CRAB assigns us is translated
directly to its CERNBox xrootd door (root://eosuser.cern.ch/) using this
chapter's own LFN_Base, bypassing crabhelper.inputFiles() (which assumes
/store/... resolves to /eos/cms/..., not CERNBox).

RecoModule needs only numpy, PyYAML and PyROOT, all in the CMSSW release's
python (checked in CMSSW_13_3_0: numpy 1.24, PyYAML 6.0, ROOT 6.26). The
earlier scipy-based fit needed the CVMFS LCG site-packages on sys.path; that
workaround is gone with it.

A PSet entry that is an existing local path is used as-is (local testing with
the fake PSet.py); everything else is treated as an LFN.
"""

import os
import sys
import yaml

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
files = [lfn if os.path.isfile(lfn) else lfn_to_eosuser_xrootd(lfn, _config["LFN_Base"])
         for lfn in raw_lfns]
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

# config.yaml names the HitFit resolution tables by repo-relative path, with an
# {era} placeholder; in the job this era's tables were shipped flat into the
# working directory.
_res = mod_cfg["hitfit"]["resolutions"]
for _kind, _path in list(_res.items()):
    _local = os.path.abspath(os.path.basename(_path.format(era=era)))
    if not os.path.isfile(_local):
        raise RuntimeError(f"HitFit resolution table {_path} was not shipped with the job")
    _res[_kind] = _local

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

#!/usr/bin/env python3
"""
004A-Reconstruction/scripts/condor/condor_script_reconstruction.py
====================================================================
HTCondor worker script for the reconstruction stage -- the condor
equivalent of crab/crab_script_reconstruction.py, running the same
config.yaml ModuleList over the same inputs and writing the same output.

Differences from the CRAB worker, all simplifications from lxbatch worker
nodes having EOS mounted as a normal filesystem: input files are read by
absolute /eos/... path out of files.json (no LFN translation), and no grid
proxy is needed -- these are private skims already on EOS, not
DBS-registered files behind the xrootd redirector.

NOTE on scipy: RecoModule's chi2 fit uses scipy.optimize.minimize, which is
NOT part of the stock CMSSW release's python environment. The LCG stack on
CVMFS bundles it and CVMFS is mounted on lxbatch workers, so its
site-packages is prepended to sys.path below -- same approach and reason as
the CRAB worker (a sys.path prepend rather than a full `source setup.sh`,
which would risk swapping out CMSSW's own ROOT build).

Args: era files_json is_data chapter_dir
"""

import json
import os
import sys

import ROOT
ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True

_LCG_VIEW = "/cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc12-opt"
for _lib in ("lib64", "lib"):
    _p = f"{_LCG_VIEW}/{_lib}/python3.9/site-packages"
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

import yaml
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from RecoModule import RecoModule

print("Running condor_script_reconstruction.py")

if len(sys.argv) != 5:
    raise SystemExit(
        "usage: condor_script_reconstruction.py <era> <files_json> <is_data 0|1> <chapter_dir>")

era, files_json, is_data_arg, chapter_dir = sys.argv[1:5]
is_data = is_data_arg == "1"
print(f"era={era}, isData={is_data}, chapter_dir={chapter_dir}")

with open("config.yaml") as f:
    config = yaml.safe_load(f)

with open(files_json) as f:
    files = json.load(f)
print("INPUT FILES:", files)

module_names = config["ModuleList"]["Data" if is_data else "MC"]
print("ModuleList:", module_names)


def build_modules(names):
    """Instantiate ModuleList entries, mirroring runReco.py's dispatch.

    Raises on an unrecognised name rather than skipping it, so a module added
    to ModuleList without being wired here fails the job instead of quietly
    producing output that differs from the local path's.
    """
    built = []
    for mod_name in names:
        mod_cfg = config["Modules"].get(mod_name, {})
        if mod_name == "reconstruction":
            built.append(RecoModule(era, mod_cfg))
        else:
            raise RuntimeError(
                f"Unknown module '{mod_name}' in ModuleList. Add it here and to "
                f"submit_reconstruction_condor.py's MODULE_FILES.")
    return built


p = PostProcessor(
    ".",
    files,
    cut=None,
    jsonInput=None,
    branchsel=None,
    modules=build_modules(module_names),
    noOut=False,
    justcount=False,
    compression="ZLIB:9",
    provenance=True,
)
print("Starting PostProcessor")
p.run()
print("Finished PostProcessor")
print("DONE condor_script_reconstruction.py")

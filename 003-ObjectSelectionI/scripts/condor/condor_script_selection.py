#!/usr/bin/env python3
"""
003-ObjectSelectionI/scripts/condor/condor_script_selection.py
===============================================================
HTCondor worker script for the object-selection stage -- the condor
equivalent of crab/crab_script_selection.py, running the same
config.yaml ModuleList over the same inputs and writing the same output.

Differences from the CRAB worker, all of them simplifications that come
from lxbatch worker nodes having EOS mounted as a normal filesystem
(confirmed live for the 002-Samples condor path):

  - Input files are read by absolute /eos/... path, straight out of the
    files.json this job was handed. The CRAB worker has to translate the
    /store/... LFN CRAB assigns into root://eosuser.cern.ch/..., because a
    grid worker cannot see CERNBox as a filesystem. Nothing here needs
    that, and nothing here needs a grid proxy either -- these are private
    files on EOS, not DBS-registered ones reached through the xrootd
    redirector (which is the only reason 002-Samples' condor jobs carry
    an x509userproxy).

  - Correction files keep their config.yaml-relative paths, resolved
    against the chapter directory passed in as an argument. The CRAB
    worker has to rebuild the inputs/SFs/... tree out of a flattened
    sandbox, since CRAB flattens every JobType.inputFiles into one
    directory. Here the real directory is simply visible.

NOTE on correctionlib: it is NOT part of the stock CMSSW release's python
environment. The LCG stack on CVMFS bundles it, and CVMFS is mounted on
lxbatch workers, so its site-packages is prepended to sys.path below --
same approach as the CRAB workers in this repo, and for the same reason.

Args: era files_json golden_json_or_NONE is_data chapter_dir
"""

import json
import os
import sys

import ROOT
ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True

# Must precede any import that reaches correctionlib (METXYCorr does).
_LCG_VIEW = "/cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc12-opt"
for _lib in ("lib64", "lib"):
    _p = f"{_LCG_VIEW}/{_lib}/python3.9/site-packages"
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

import yaml
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from SelectedObjects import SelectedObjectsProducer
from METXYCorr import METXYCorrModule

print("Running condor_script_selection.py")

if len(sys.argv) != 6:
    raise SystemExit(
        "usage: condor_script_selection.py <era> <files_json> <golden_json|NONE> "
        "<is_data 0|1> <chapter_dir>")

era, files_json, golden_json, is_data_arg, chapter_dir = sys.argv[1:6]
is_data = is_data_arg == "1"
print(f"era={era}, isData={is_data}, chapter_dir={chapter_dir}")

with open("config.yaml") as f:
    config = yaml.safe_load(f)

with open(files_json) as f:
    files = json.load(f)
print("INPUT FILES:", files)

# Same combined cut string the local path and the CRAB worker build.
era_cuts = config["SelectionCuts"][era]
cut_string = " && ".join(v for v in era_cuts.values() if v and v.strip())
print("Cut string:", cut_string)

json_input = golden_json if (is_data and golden_json != "NONE") else None
if is_data and json_input is None:
    print("[WARNING] Data job with no golden JSON -- running without lumi filtering.")

# Same per-module injections run_all.py applies locally, so all three paths
# (local, CRAB, condor) hand identical config to the modules. Correction file
# paths are chapter-relative in config.yaml; resolve them against the real
# chapter directory rather than the job's scratch cwd.
module_names = config["ModuleList"]["Data" if is_data else "MC"]
print("ModuleList:", module_names)

module_configs = []
for mod_name in module_names:
    raw = config["Modules"].get(mod_name, {})
    mod_cfg = dict(raw.get(era, raw))
    if mod_name == "selectedObjects":
        mod_cfg["is_mc"] = not is_data
    elif mod_name == "metXYCorr":
        mod_cfg["isData"] = is_data
        mod_cfg["metFile"] = os.path.join(chapter_dir, mod_cfg["metFile"])
        if not os.path.isfile(mod_cfg["metFile"]):
            raise RuntimeError(
                f"metXYCorr: correction file not found: {mod_cfg['metFile']}. "
                f"Run run_all.py --fetchSFFiles for era {era} first.")
    module_configs.append((mod_name, mod_cfg))


def build_modules(configs):
    """Instantiate ModuleList entries, mirroring runSelection.py's dispatch.

    Raises on an unrecognised name rather than skipping it, so a module added
    to ModuleList without being wired here fails the job instead of quietly
    producing output that differs from the local path's.
    """
    built = []
    for mod_name, mod_cfg in configs:
        if mod_name == "selectedObjects":
            built.append(SelectedObjectsProducer(mod_cfg))
        elif mod_name == "metXYCorr":
            built.append(METXYCorrModule(mod_cfg))
        else:
            raise RuntimeError(
                f"Unknown module '{mod_name}' in ModuleList. Add it here and to "
                f"submit_selection_condor.py's MODULE_FILES.")
    return built


def passes_cut_precheck(filepath, cut):
    """Mirrors runSelection.py's process_file() pre-check.

    An empty TEntryList (GetN()==0) makes PostProcessor's eventLoop() skip
    beginFile() on modules entirely; CopyTree() then segfaults in
    FullOutput.write() walking branch buffers that were never initialised.

    An unreadable input is NOT that case and must not be quietly folded into
    it. Treating "file missing" as "no events passed" makes a job whose input
    has gone away exit 0 with no output, which is indistinguishable downstream
    from a dataset that legitimately selected nothing -- the job looks done
    and the file is simply absent from the output. Raise instead, so the job
    fails visibly and checkCondorStatus reports it.
    """
    f = ROOT.TFile.Open(filepath)
    if not f or f.IsZombie():
        raise RuntimeError(
            f"could not open input file {filepath} -- it is missing or unreadable, "
            f"not empty. Check the dataset JSON still points at files that exist.")
    tree = f.Get("Events")
    if not tree:
        f.Close()
        raise RuntimeError(f"input file {filepath} has no Events tree")
    if not cut:
        f.Close()
        return True
    n = tree.Draw(">>elist", cut, "entrylist")
    f.Close()
    return n > 0


files_to_process = [f for f in files if passes_cut_precheck(f, cut_string)]
if not files_to_process:
    print(f"0 events pass cut string in all {len(files)} input file(s); "
          "skipping PostProcessor (no output produced) to avoid ROOT segfault.")
    print("DONE condor_script_selection.py")
    sys.exit(0)

p = PostProcessor(
    ".",
    files_to_process,
    cut=cut_string,
    branchsel=None,
    modules=build_modules(module_configs),
    jsonInput=json_input,
    provenance=True,
)
print("Starting PostProcessor")
p.run()
print("Finished PostProcessor")
print("DONE condor_script_selection.py")

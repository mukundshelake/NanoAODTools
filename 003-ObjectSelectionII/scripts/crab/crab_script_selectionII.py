#!/usr/bin/env python
"""
CRAB worker script for the scale-factor weight stage (003-ObjectSelectionII).
Mirrors scripts/runSelectionII.py's process_file() exactly: no cut string
currently (SelectionCuts is empty for this chapter today, same as the local
path), no new kinematic selection -- just adds MC-only weight branches (or,
for Data, runs with an empty module list -- a straight pass-through, still
going through PostProcessor for parity with the local path).

This script is sent to the grid worker node as an inputFile and executed by
crab_selectionII.sh. The module .py files (LHEWeightSign.py, MuonIDWeight.py,
MuonHLTWeight.py, bTaggingWeight.py, ABCDTransferWeight.py) are shipped
alongside it (flat, no modules/ subpackage) since they aren't part of the
installed NanoAODTools package.

NOTE on input file resolution: same as crab_script_selection.py in
003-ObjectSelectionI -- the /store/... LFN CRAB assigns us is translated
directly to its CERNBox xrootd door (root://eosuser.cern.ch/) using this
chapter's own LFN_Base, bypassing crabhelper.inputFiles() (which assumes
/store/... resolves to /eos/cms/..., not CERNBox).

NOTE on SF file layout: correctionlib file paths in config.yaml
(Modules.muonID.IDSFFile etc.) are relative, e.g. "inputs/SFs/UL2018_mu_ID.json"
(fetched by run_all.py --fetchSFFiles) -- the shared module code (same files
the local Pool-based path uses, unmodified) expects that literal relative
layout. CRAB flattens all JobType.inputFiles into the sandbox root, so this
script recreates the expected inputs/SFs/ and SFs/Efficiency/<era>/ layouts
(the latter still repo-root relative -- a shared, self-computed artifact, not
a fetched input) by moving the flat-shipped files into place before
instantiating any module.

NOTE on correctionlib/coffea/awkward: these are NOT part of the stock CMSSW
release's python environment -- on lxplus they only import because the user
has them pip-installed under their own AFS home (~/.local/...), which a grid
worker node has no access to. Verified this directly (a plain `cmsenv` on a
fresh CVMFS-only VM, no AFS, fails to import correctionlib/coffea at all).
The grid-worker-safe equivalent is the standard LCG software stack on CVMFS,
which bundles the exact same versions -- prepend its site-packages to
sys.path (not a full `source .../setup.sh`, which would risk swapping out
CMSSW's own ROOT build and breaking the PhysicsTools.NanoAODTools import;
verified the sys.path-only approach keeps CMSSW's ROOT intact).
"""

import os
import sys
import shutil
import yaml
import ROOT
ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True

_LCG_VIEW = "/cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc12-opt"
for _lib in ("lib64", "lib"):
    _p = f"{_LCG_VIEW}/{_lib}/python3.9/site-packages"
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

import PSet
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from LHEWeightSign import LHEWeightSignProducer
from MuonIDWeight import MuonIDWeightProducer
from MuonHLTWeight import MuonHLTWeightProducer
from bTaggingWeight import bTaggingWeightProducer
from ABCDTransferWeight import ABCDTransferWeightProducer

print("Running crab_script_selectionII.py")

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

# scriptArgs: era=VALUE, isData=True/False, dataset=<key> (passed by submit_selectionII_flexible.py)
era = None
is_data = False
dataset_key = None
for arg in sys.argv[1:]:
    if arg.startswith("era="):
        era = arg.split("=", 1)[1]
    elif arg.startswith("isData="):
        is_data = arg.split("=", 1)[1].strip().lower() == "true"
    elif arg.startswith("dataset="):
        dataset_key = arg.split("=", 1)[1]

if era is None:
    raise RuntimeError("crab_script_selectionII.py requires scriptArgs era=<era>")
if not is_data and dataset_key is None:
    raise RuntimeError("crab_script_selectionII.py requires scriptArgs dataset=<key> for MC jobs "
                        "(used as the b-tagging efficiency file stem)")

print(f"era={era}, isData={is_data}, dataset={dataset_key}")

# Build the same cut string run_all.py's --generateProcessListJSON derives locally
# (config['SelectionCuts'][era], "" if unset for this chapter). Currently always
# empty here since 003-ObjectSelectionII applies no event-level cut by design, but
# deriving it from config.yaml -- rather than hardcoding cut=None -- keeps this
# script from silently diverging from the local path if SelectionCuts[era] is ever
# populated in config.yaml.
_era_cuts = _config.get("SelectionCuts", {}).get(era, {})
cut_string = " && ".join(v for v in _era_cuts.values() if v and v.strip()) or None
print("Cut string:", cut_string)

# Same module list / era-resolved config the local process-list JSON carries.
# Read from config.yaml's ModuleList.Data/.MC rather than assuming Data is always
# empty -- ABCDTransferWeight is the first module ever added to ModuleList.Data.
module_names = _config["ModuleList"]["Data" if is_data else "MC"]
module_configs = []
for mod_name in module_names:
    mod_cfg_raw = _config["Modules"].get(mod_name, {})
    mod_cfg = mod_cfg_raw.get(era, mod_cfg_raw)
    module_configs.append((mod_name, mod_cfg))

# Recreate the SFs/ layout the shared module code expects, from the flat-shipped files.
os.makedirs(f"SFs/Efficiency/{era}", exist_ok=True)
for mod_name, mod_cfg in module_configs:
    for file_key in ("IDSFFile", "HLTSFFile", "bTagSFFile", "scaleFactorFile"):
        if file_key in mod_cfg:
            expected_path = mod_cfg[file_key]  # e.g. "inputs/SFs/UL2018_mu_ID.json"
            flat_name = os.path.basename(expected_path)
            if os.path.isfile(flat_name) and not os.path.isfile(expected_path):
                os.makedirs(os.path.dirname(expected_path), exist_ok=True)
                shutil.move(flat_name, expected_path)
    if mod_name == "bTagging":
        eff_flat = f"{dataset_key}.root"
        eff_expected = f"{mod_cfg['efficiencyFolder']}/{era}/{dataset_key}.root"
        if os.path.isfile(eff_flat) and not os.path.isfile(eff_expected):
            shutil.move(eff_flat, eff_expected)

# Golden JSON: shipped flat as an inputFile only for data jobs (same as 003-ObjectSelectionI).
json_input = None
if is_data:
    golden_json_name = f"{era}_goldenJSON.json"
    if os.path.isfile(golden_json_name):
        json_input = golden_json_name
    else:
        print(f"[WARNING] Data job but golden JSON '{golden_json_name}' not found in sandbox.")


def _passes_cut_precheck(filepath, cut_string):
    """Mirrors scripts/runSelectionII.py's process_file() pre-check (only
    matters if SelectionCuts[era] is ever populated -- see cut_string above).
    An empty TEntryList (GetN()==0) makes PostProcessor's eventLoop() skip
    beginFile() on modules entirely; CopyTree() then segfaults in
    FullOutput.write() walking branch buffers that were never initialised.
    """
    try:
        _cf = ROOT.TFile.Open(filepath, "READ")
        if not _cf or _cf.IsZombie():
            return True  # let PostProcessor raise its own clear file-open error
        _ct = _cf.Get("Events")
        _n = int(_ct.GetEntries(cut_string)) if (_ct is not None) else 0
        _ct = None
        _cf.Close()
        del _cf
        return _n > 0
    except Exception as _e:
        print(f"[WARNING] Cut pre-check failed for {filepath}: {_e}; proceeding anyway.")
        return True


files_to_process = files
if cut_string is not None:
    files_to_process = [f for f in files if _passes_cut_precheck(f, cut_string)]
    if not files_to_process:
        print(f"0 events pass cut string in all {len(files)} input file(s); "
              "skipping PostProcessor (no output produced) to avoid ROOT segfault.")
        print("DONE crab_script_selectionII.py")
        sys.exit(0)

modules = []
for mod_name, mod_cfg in module_configs:
    if mod_name == "lheWeightSign":
        modules.append(LHEWeightSignProducer(mod_cfg))
    elif mod_name == "muonID":
        modules.append(MuonIDWeightProducer(mod_cfg))
    elif mod_name == "muonHLT":
        modules.append(MuonHLTWeightProducer(mod_cfg))
    elif mod_name == "bTagging":
        modules.append(bTaggingWeightProducer(mod_cfg, dataset_key))
    elif mod_name == "ABCDTransferWeight":
        modules.append(ABCDTransferWeightProducer(mod_cfg))
    else:
        raise RuntimeError(f"Unknown module: {mod_name}")

p = PostProcessor(
    ".",
    files_to_process,
    cut=cut_string,
    jsonInput=json_input,
    branchsel=None,
    modules=modules,
    noOut=False,
    justcount=False,
    compression="ZLIB:9",
    provenance=True,
    fwkJobReport=True,
)
print("Starting PostProcessor")
p.run()
print("Finished PostProcessor")
print("DONE crab_script_selectionII.py")

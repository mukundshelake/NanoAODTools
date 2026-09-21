#!/usr/bin/env python
"""
CRAB worker script for the object-selection stage (003-ObjectSelectionI).
Applies the era's event-level cut string and runs config.yaml's ModuleList,
mirroring scripts/runSelection.py's local per-file event loop exactly.

This script is sent to the grid worker node as an inputFile and executed by
crab_selection.sh. Each ModuleList module's source is shipped alongside it
(flat, no modules/ subpackage) since they aren't part of the installed
NanoAODTools package.

NOTE on ModuleList: the module set is read from config.yaml rather than
hardcoded here. It used to be hardcoded to SelectedObjectsProducer alone,
which silently diverged from the local path the moment metXYCorr was added
to ModuleList -- CRAB kept producing output with no MET xy-shift correction
applied while runSelection.py applied it, with nothing anywhere reporting a
problem. Reading the same list both paths read is what keeps them honest;
an unknown name raises rather than being skipped.

NOTE on SF file layout: correctionlib paths in config.yaml (Modules.
metXYCorr.metFile etc.) are chapter-relative, e.g. "inputs/SFs/UL2018_met.
json.gz", and the shared module code expects that literal layout. CRAB
flattens all JobType.inputFiles into the sandbox root, so this script
recreates the expected inputs/SFs/... layout before instantiating any
module -- same approach as 003-ObjectSelectionII's worker script.

NOTE on correctionlib: it is NOT part of the stock CMSSW release's python
environment -- on lxplus it only imports because of a user-local pip install
under AFS, which a grid worker node cannot see. The grid-safe equivalent is
the LCG stack on CVMFS, whose site-packages is prepended to sys.path below
(not a full `source setup.sh`, which would risk swapping out CMSSW's own
ROOT build and breaking the PhysicsTools.NanoAODTools import).

Unlike the preselection stage, this job's input files are not a DBS-registered
dataset (Data.userInputFiles was used, not Data.inputDataset), so CRAB has no
lumi mask to give us: runsAndLumis() returns None here. Golden-JSON filtering
is instead applied the same way the local Pool-based runSelection.py already
does it -- jsonInput=<path to the shipped golden JSON file>, only for data.

NOTE on input file resolution: we deliberately do NOT use
PhysicsTools.NanoAODTools.postprocessing.framework.crabhelper.inputFiles().
That helper resolves /store/... LFNs via the site-local config (which maps
/store/... to /eos/cms/..., the official CMS T2_CH_CERN storage), falling
back to the global AAA redirector (root://cms-xrd-global.cern.ch/, which only
knows about DBS/Rucio-registered files) if the local open fails. Our
preselection input files are private and live in CERNBox (/eos/user/...), a
different physical EOS instance from /eos/cms -- neither of crabhelper's
resolution paths can ever find them. So instead we translate the /store/...
LFN CRAB assigns us directly to its CERNBox xrootd door (root://eosuser.cern.ch/),
using this chapter's own LFN_Base (shipped via config.yaml) to know the mapping.
"""

import os
import sys
import shutil
import yaml
import ROOT
ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True

# Must precede any import that reaches correctionlib (METXYCorr does, at its
# own module top) -- see the correctionlib note in this script's docstring.
_LCG_VIEW = "/cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc12-opt"
for _lib in ("lib64", "lib"):
    _p = f"{_LCG_VIEW}/{_lib}/python3.9/site-packages"
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

import PSet
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from SelectedObjects import SelectedObjectsProducer
from METXYCorr import METXYCorrModule

print("Running crab_script_selection.py")

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

# scriptArgs: era=VALUE, isData=True/False (passed by submit_selection_flexible.py)
era = None
is_data = False
for arg in sys.argv[1:]:
    if arg.startswith("era="):
        era = arg.split("=", 1)[1]
    elif arg.startswith("isData="):
        is_data = arg.split("=", 1)[1].strip().lower() == "true"

if era is None:
    raise RuntimeError("crab_script_selection.py requires scriptArgs era=<era>")

print(f"era={era}, isData={is_data}")

# Build the same combined cut string run_all.py --generateProcessListJSON builds locally
_era_cuts = _config["SelectionCuts"][era]
cut_string = " && ".join(v for v in _era_cuts.values() if v and v.strip())
print("Cut string:", cut_string)

# Same module configs the local process-list JSON carries, era-resolved with the
# same per-module injections run_all.py applies locally (selectedObjects' is_mc,
# metXYCorr's isData/metFile) so both paths hand identical config to the modules.
_module_names = _config["ModuleList"]["Data" if is_data else "MC"]
print("ModuleList:", _module_names)

module_configs = []
for _mod_name in _module_names:
    _raw = _config["Modules"].get(_mod_name, {})
    _cfg = dict(_raw.get(era, _raw))
    if _mod_name == "selectedObjects":
        _cfg["is_mc"] = not is_data
    elif _mod_name == "metXYCorr":
        _cfg["isData"] = is_data
    module_configs.append((_mod_name, _cfg))

# Rebuild the chapter-relative inputs/SFs/... layout the module code expects
# out of the flat-shipped sandbox copies (see the SF file layout note above).
for _mod_name, _cfg in module_configs:
    for _file_key in ("metFile",):
        if _file_key in _cfg:
            _expected = _cfg[_file_key]          # e.g. "inputs/SFs/UL2018_met.json.gz"
            _flat = os.path.basename(_expected)
            if os.path.isfile(_flat) and not os.path.isfile(_expected):
                os.makedirs(os.path.dirname(_expected), exist_ok=True)
                shutil.move(_flat, _expected)
            if not os.path.isfile(_expected):
                raise RuntimeError(
                    f"{_mod_name}: correction file '{_expected}' not in the sandbox "
                    f"(looked for flat-shipped '{_flat}' too). It must be added to "
                    f"JobType.inputFiles by submit_selection_flexible.py.")

# Golden JSON: shipped flat as an inputFile only for data jobs
json_input = None
if is_data:
    golden_json_name = f"{era}_goldenJSON.json"
    if os.path.isfile(golden_json_name):
        json_input = golden_json_name
    else:
        print(f"[WARNING] Data job but golden JSON '{golden_json_name}' not found in sandbox.")


def _build_modules(configs):
    """Instantiate ModuleList entries, mirroring runSelection.py's dispatch.

    Raises on an unrecognised name rather than skipping it: a module that is
    in ModuleList but missing here means CRAB output would silently differ
    from what the local path produces, which is exactly the failure this
    dispatch replaced.
    """
    built = []
    for mod_name, mod_cfg in configs:
        if mod_name == "selectedObjects":
            built.append(SelectedObjectsProducer(mod_cfg))
        elif mod_name == "metXYCorr":
            built.append(METXYCorrModule(mod_cfg))
        else:
            raise RuntimeError(
                f"Unknown module '{mod_name}' in ModuleList. Add it here and ship "
                f"its source via submit_selection_flexible.py's MODULE_FILES.")
    return built


def _passes_cut_precheck(filepath, cut_string):
    """Mirrors scripts/runSelection.py's process_file() pre-check.

    An empty TEntryList (GetN()==0) makes PostProcessor's eventLoop() skip
    beginFile() on modules entirely; CopyTree() then segfaults in
    FullOutput.write() walking branch buffers that were never initialised.
    Detect cheaply before spawning PostProcessor instead of crashing the job.
    """
    try:
        _cf = ROOT.TFile.Open(filepath, "READ")
        if not _cf or _cf.IsZombie():
            return True  # let PostProcessor raise its own clear file-open error
        _ct = _cf.Get("Events")
        _n = int(_ct.GetEntries(cut_string)) if (_ct is not None) else 0
        # Release _ct BEFORE Close(): TFile::Close() frees the TTree C++ object.
        _ct = None
        _cf.Close()
        del _cf
        return _n > 0
    except Exception as _e:
        print(f"[WARNING] Cut pre-check failed for {filepath}: {_e}; proceeding anyway.")
        return True


files_to_process = [f for f in files if _passes_cut_precheck(f, cut_string)]
if not files_to_process:
    print(f"0 events pass cut string in all {len(files)} input file(s); "
          "skipping PostProcessor (no output produced) to avoid ROOT segfault.")
    print("DONE crab_script_selection.py")
    sys.exit(0)

p = PostProcessor(
    ".",
    files_to_process,
    cut=cut_string,
    branchsel=None,
    provenance=True,
    fwkJobReport=True,
    modules=_build_modules(module_configs),
    jsonInput=json_input,
)
print("Starting PostProcessor")
p.run()
print("Finished PostProcessor")
print("DONE crab_script_selection.py")

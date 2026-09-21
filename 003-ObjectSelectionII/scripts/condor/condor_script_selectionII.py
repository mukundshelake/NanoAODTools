#!/usr/bin/env python3
"""
003-ObjectSelectionII/scripts/condor/condor_script_selectionII.py
==================================================================
HTCondor worker script for the weight/SF stage -- the condor equivalent of
crab/crab_script_selectionII.py, running the same config.yaml ModuleList
over the same inputs and writing the same output.

Differences from the CRAB worker, all simplifications from lxbatch worker
nodes having EOS mounted as a normal filesystem:

  - Input files are read by absolute /eos/... path out of files.json. No
    LFN translation, and no grid proxy: these are private skims on EOS,
    not DBS-registered files behind the xrootd redirector.
  - Correction files (and the b-tagging efficiency map) keep their
    config.yaml-relative paths, resolved against the chapter directory
    passed in as an argument. The CRAB worker has to rebuild the
    inputs/SFs/... tree out of a flattened sandbox; here it is just there.

NOTE on correctionlib/coffea/awkward: not part of the stock CMSSW release's
python environment. The LCG stack on CVMFS bundles them and CVMFS is mounted
on lxbatch workers, so its site-packages is prepended to sys.path below --
same approach and reason as the CRAB worker.

Args: era files_json golden_json_or_NONE is_data chapter_dir dataset_key
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
from LHEWeightSign import LHEWeightSignProducer
from MuonIDWeight import MuonIDWeightProducer
from MuonIsoWeight import MuonIsoWeightProducer
from MuonHLTWeight import MuonHLTWeightProducer
from bTaggingWeight import bTaggingWeightProducer
from PUWeight import PUWeightProducer
from TopPtWeight import TopPtWeightProducer

print("Running condor_script_selectionII.py")

if len(sys.argv) != 7:
    raise SystemExit(
        "usage: condor_script_selectionII.py <era> <files_json> <golden_json|NONE> "
        "<is_data 0|1> <chapter_dir> <dataset_key>")

era, files_json, golden_json, is_data_arg, chapter_dir, dataset_key = sys.argv[1:7]
is_data = is_data_arg == "1"
print(f"era={era}, isData={is_data}, dataset={dataset_key}, chapter_dir={chapter_dir}")

with open("config.yaml") as f:
    config = yaml.safe_load(f)

with open(files_json) as f:
    files = json.load(f)
print("INPUT FILES:", files)

# SelectionCuts may be empty for this stage (the cuts were already applied in
# selectionI) -- keep the same "join whatever is there" shape the other paths use.
era_cuts = config.get("SelectionCuts", {}).get(era, {}) or {}
cut_string = " && ".join(v for v in era_cuts.values() if v and v.strip()) or None
print("Cut string:", cut_string)

json_input = golden_json if (is_data and golden_json != "NONE") else None

module_names = config["ModuleList"]["Data" if is_data else "MC"]
print("ModuleList:", module_names)

# Resolve every chapter-relative correction path against the real chapter
# directory. Same file_key set the CRAB submit script ships.
_FILE_KEYS = ("IDSFFile", "HLTSFFile", "bTagSFFile", "StandardSFFile",
              "TightIsoSFFile", "PUSFFile")

module_configs = []
for mod_name in module_names:
    raw = config["Modules"].get(mod_name, {})
    mod_cfg = dict(raw.get(era, raw))
    for file_key in _FILE_KEYS:
        if file_key in mod_cfg:
            resolved = os.path.join(chapter_dir, mod_cfg[file_key])
            if not os.path.isfile(resolved):
                raise RuntimeError(
                    f"{mod_name}: correction file not found: {resolved}. "
                    f"Run run_all.py --fetchSFFiles for era {era} first.")
            mod_cfg[file_key] = resolved
    if mod_name == "bTagging":
        eff = os.path.join(chapter_dir, mod_cfg["efficiencyFolder"], era, f"{dataset_key}.root")
        if not os.path.isfile(eff):
            raise RuntimeError(
                f"bTagging: efficiency file not found: {eff}. "
                f"Run run_all.py --computeBTaggingEfficiency for era {era} first.")
        # The module reads efficiencyFolder/<era>/<dataset>.root itself, so hand it
        # an absolute folder rather than the resolved file.
        mod_cfg["efficiencyFolder"] = os.path.join(chapter_dir, mod_cfg["efficiencyFolder"])
    module_configs.append((mod_name, mod_cfg))


def build_modules(configs):
    """Instantiate ModuleList entries, mirroring runSelectionII.py's dispatch.

    Raises on an unrecognised name rather than skipping it, so a module added
    to ModuleList without being wired here fails the job instead of quietly
    producing output that differs from the local path's.
    """
    built = []
    for mod_name, mod_cfg in configs:
        if mod_name == "lheWeightSign":
            built.append(LHEWeightSignProducer(mod_cfg))
        elif mod_name == "muonID":
            built.append(MuonIDWeightProducer(mod_cfg))
        elif mod_name == "muonIso":
            built.append(MuonIsoWeightProducer(mod_cfg))
        elif mod_name == "muonHLT":
            built.append(MuonHLTWeightProducer(mod_cfg))
        elif mod_name == "bTagging":
            built.append(bTaggingWeightProducer(mod_cfg, dataset_key))
        elif mod_name == "puWeight":
            built.append(PUWeightProducer(mod_cfg))
        elif mod_name == "topPtWeight":
            built.append(TopPtWeightProducer(mod_cfg))
        else:
            raise RuntimeError(
                f"Unknown module '{mod_name}' in ModuleList. Add it here and to "
                f"submit_selectionII_condor.py's MODULE_FILES.")
    return built


def passes_cut_precheck(filepath, cut):
    """Mirrors runSelectionII.py's process_file() pre-check (only matters if
    SelectionCuts[era] is ever populated). An empty TEntryList makes
    PostProcessor skip beginFile(); CopyTree() then segfaults walking branch
    buffers that were never initialised.

    An unreadable input is NOT that case and must not be quietly folded into
    it: treating "file missing" as "no events passed" makes a job whose input
    has gone away exit 0 with no output, indistinguishable downstream from a
    dataset that legitimately selected nothing. Raise instead.
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
    print("DONE condor_script_selectionII.py")
    sys.exit(0)

p = PostProcessor(
    ".",
    files_to_process,
    cut=cut_string,
    jsonInput=json_input,
    branchsel=None,
    modules=build_modules(module_configs),
    noOut=False,
    justcount=False,
    provenance=True,
)
print("Starting PostProcessor")
p.run()
print("Finished PostProcessor")
print("DONE condor_script_selectionII.py")

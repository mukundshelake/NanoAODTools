#!/usr/bin/env python
"""
CRAB worker script for the object-selection stage (003-ObjectSelectionI).
Applies the era's event-level cut string and runs SelectedObjectsProducer,
mirroring scripts/runSelection.py's local per-file event loop exactly.

This script is sent to the grid worker node as an inputFile and executed by
crab_selection.sh. SelectedObjects.py is shipped alongside it (flat, no
modules/ subpackage) since it isn't part of the installed NanoAODTools
package.

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
import yaml
import PSet
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from SelectedObjects import SelectedObjectsProducer

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

# Same module config the local process-list JSON carries, era-resolved + is_mc flag
_mod_cfg_raw = _config["Modules"]["selectedObjects"]
mod_cfg = dict(_mod_cfg_raw.get(era, _mod_cfg_raw), is_mc=not is_data)

# Golden JSON: shipped flat as an inputFile only for data jobs
json_input = None
if is_data:
    golden_json_name = f"{era}_goldenJSON.json"
    if os.path.isfile(golden_json_name):
        json_input = golden_json_name
    else:
        print(f"[WARNING] Data job but golden JSON '{golden_json_name}' not found in sandbox.")

p = PostProcessor(
    ".",
    files,
    cut=cut_string,
    branchsel=None,
    provenance=True,
    fwkJobReport=True,
    modules=[SelectedObjectsProducer(mod_cfg)],
    jsonInput=json_input,
)
print("Starting PostProcessor")
p.run()
print("Finished PostProcessor")
print("DONE crab_script_selection.py")

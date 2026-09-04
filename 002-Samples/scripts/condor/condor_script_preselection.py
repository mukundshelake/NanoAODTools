#!/usr/bin/env python3
"""
002-Samples/scripts/condor/condor_script_preselection.py
==========================================================
HTCondor worker script for the preselection stage -- the condor equivalent
of scripts/crab/crab_script_preselection.py.

CRAB's worker script leans on crabhelper.inputFiles()/runsAndLumis(), which
read a CRAB-generated PSet.py that only exists inside a CRAB job (CRAB's own
splitting engine resolves each job's file list and per-job lumi mask into it
at submission time). There is no such PSet under condor, so this script
takes its file list directly as a JSON array of LFNs (one per job by default,
see --files-per-job in submit_preselection_condor.py) and, for Data, passes
the *entire* golden JSON as jsonInput rather than a job-scoped lumi slice --
PostProcessor's own JSON filtering still applies it correctly, just without
CRAB's per-job lumi-mask narrowing (a purely internal efficiency difference,
not a correctness one).

Positional args (all required, passed by condor_preselection.sh):
    1. era            e.g. UL2016preVFP
    2. files_json      path to a JSON file: a list of LFNs to process
    3. golden_json      path to golden JSON, or the literal string NONE for MC
    4. is_data          "1" or "0"
"""

import json
import sys

import yaml
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor

XROOTD_REDIRECTOR = "root://cms-xrd-global.cern.ch/"


def main():
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <era> <files_json> <golden_json|NONE> <is_data:0|1>", file=sys.stderr)
        sys.exit(1)

    era, files_json_path, golden_json_path, is_data_flag = sys.argv[1:5]
    is_data = is_data_flag == "1"

    with open(files_json_path) as f:
        lfns = json.load(f)
    files = [XROOTD_REDIRECTOR + lfn if not lfn.startswith("root://") else lfn for lfn in lfns]
    print("INPUT FILES:", files)

    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    presel_cuts = config.get("Pre-SelectionCuts", {})
    if era not in presel_cuts:
        print(f"[WARNING] Era '{era}' not found in config.yaml Pre-SelectionCuts, falling back to UL2016preVFP.")
        era = "UL2016preVFP"
    cuts = list(presel_cuts[era].values())
    print("Cuts for era", era)
    for cut in cuts:
        print(" ", cut)

    branchsel = config.get("branch_selection", {})
    with open("keep_and_drop.txt", "w") as kd:
        for branch in branchsel.get("drop", []):
            kd.write(f"drop {branch}\n")
        for branch in branchsel.get("keep", []):
            kd.write(f"keep {branch}\n")
    print("Generated keep_and_drop.txt from config.yaml")

    json_input = golden_json_path if (is_data and golden_json_path != "NONE") else None
    if is_data and json_input is None:
        print("[WARNING] Data job but no golden JSON provided -- running without lumi filtering.")

    p = PostProcessor(
        ".",
        files,
        cut=" && ".join(cuts),
        branchsel="keep_and_drop.txt",
        provenance=True,
        fwkJobReport=True,
        modules=[],
        jsonInput=json_input,
    )
    print("Starting PostProcessor")
    p.run()
    print("Finished PostProcessor")
    print("DONE condor_script_preselection.py")


if __name__ == "__main__":
    print("Running condor_script_preselection.py")
    main()

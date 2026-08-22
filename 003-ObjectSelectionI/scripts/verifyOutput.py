#!/usr/bin/env python3
"""
Verify object-selection (003-ObjectSelectionI) skim output ROOT files: checks
each file opens cleanly and has the "Events" tree, and cross-checks the
tree's actual branch list against the expected set -- everything inherited
from 002-Samples' branch_selection.keep (this stage's skims retain all
original NanoAOD branches) PLUS the derived branches this stage's own
selectedObjects module writes (SelMuon_*, leading(b)Jet_*, sel_nJet,
sel_nbjet -- names taken from config.yaml's Modules.selectedObjects.{era}
.branchNames, not hardcoded, since a config change would otherwise silently
desync this check).

Input is a selectionI_{tag}_{era}_datasets.json (from --generateDatasetJSON),
keyed {DataMC: {group: {dataset: {filepath: "Events"}}}}.

Usage:
    python3 verifyOutput.py --datasetJSON <selectionI_{tag}_{era}_datasets.json> \\
        --config <003-ObjectSelectionI/config.yaml> \\
        --previousConfig <002-Samples/config.yaml> \\
        --era <era> --outputReport <report.json> [--filter ...]
"""

import argparse
import json
import re
import sys
from pathlib import Path

import ROOT
import yaml

ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gErrorIgnoreLevel = ROOT.kWarning

# Standard NanoAOD branches that only exist for MC (generator-level truth /
# MC-only weights), inherited unchanged from 002-Samples' preselection output.
MC_ONLY_EXACT = {
    "Jet_hadronFlavour", "Jet_partonFlavour", "Jet_genJetIdx",
    "Generator_x1", "Generator_x2", "Generator_weight",
    "Generator_xpdf1", "Generator_xpdf2", "Generator_id1", "Generator_id2",
    "LHEWeight_originalXWGTUP", "PSWeight", "LHEScaleWeight", "LHEPdfWeight",
}
MC_ONLY_PREFIXES = ("GenPart", "puWeight")

# 003-ObjectSelectionI's own selectedObjects module always writes these
# fields for the muon it selects (or its -1/0 sentinel if none found), and
# for each jet_key below (or ITS sentinel) -- see modules/SelectedObjects.py.
# hadronFlavour is MC-only, matching Jet_hadronFlavour's MC-only status.
_MUON_FLOAT_FIELDS = ["pt", "eta", "phi", "mass", "pfRelIso04_all"]
_MUON_INT_FIELDS = ["charge"]
_MUON_BOOL_FIELDS = ["tightId"]
_JET_FLOAT_FIELDS = ["pt", "eta", "phi", "mass", "btagDeepFlavB"]
_JET_INT_FIELDS = ["jetId", "puId"]
_JET_INT_FIELDS_MC = ["hadronFlavour"]
_JET_KEYS = ["leadingbJet", "subleadingbJet", "leadingJet", "subleadingJet"]
_ALWAYS_PRESENT_EXTRA = ["sel_nJet", "sel_nbjet"]


def expand_keep_list(keep_list):
    """Split branch_selection.keep into (exact_names, wildcard_prefixes)."""
    exact, prefixes = set(), []
    for entry in keep_list:
        if entry.endswith("*"):
            prefixes.append(entry[:-1])
        else:
            exact.add(entry)
    return exact, prefixes


def derived_branch_names(selected_objects_cfg):
    """Exact names of the flat scalar branches selectedObjects always writes
    (present for every event, MC or Data, real object or sentinel)."""
    names = set(_ALWAYS_PRESENT_EXTRA)
    branch_names = selected_objects_cfg["branchNames"]

    muon_prefix = branch_names["muon"]
    for field in _MUON_FLOAT_FIELDS + _MUON_INT_FIELDS + _MUON_BOOL_FIELDS:
        names.add(f"{muon_prefix}_{field}")

    for jet_key in _JET_KEYS:
        prefix = branch_names[jet_key]
        for field in _JET_FLOAT_FIELDS + _JET_INT_FIELDS:
            names.add(f"{prefix}_{field}")
        # _JET_INT_FIELDS_MC handled separately per-dataset (is_data gate),
        # same treatment as inherited Jet_hadronFlavour.
    return names


def derived_branch_names_mc_only(selected_objects_cfg):
    """The MC-only subset of derived branches (hadronFlavour per jet_key)."""
    branch_names = selected_objects_cfg["branchNames"]
    names = set()
    for jet_key in _JET_KEYS:
        prefix = branch_names[jet_key]
        for field in _JET_INT_FIELDS_MC:
            names.add(f"{prefix}_{field}")
    return names


def branch_is_expected(name, exact, prefixes):
    if name in exact or any(name.startswith(p) for p in prefixes):
        return True
    # NanoAOD's collection-size counter convention: "nJet" alongside "Jet_*",
    # etc. -- inherited unchanged from 002-Samples' verifyOutput.py.
    if name.startswith("n") and len(name) > 1:
        stripped = name[1:]
        if stripped in exact or stripped in prefixes:
            return True
    return False


def check_file_structure(filepath, exact, prefixes, is_data, mc_only_exact, mc_only_prefixes):
    """Open one file, check tree existence/health and branch completeness.

    Returns (ok: bool, info: dict). ok is False only for structural problems
    (won't open, no Events tree, unexpected extra branches). Missing expected
    branches and 0 entries are reported as warnings, not hard failures --
    both can be legitimate (Data lacks MC-only branches; a tightly-cut skim
    can genuinely have 0 events survive).
    """
    info = {"file": filepath, "errors": [], "warnings": []}
    f = ROOT.TFile.Open(filepath)
    if not f or f.IsZombie():
        info["errors"].append("file did not open / is zombie")
        return False, info

    tree = f.Get("Events")
    if not tree:
        info["errors"].append("no 'Events' tree")
        f.Close()
        return False, info

    n_entries = tree.GetEntries()
    info["n_entries"] = n_entries
    if n_entries == 0:
        info["warnings"].append("0 entries (may be legitimate for a tightly-cut skim)")

    actual_branches = {b.GetName() for b in tree.GetListOfBranches()}
    info["n_branches"] = len(actual_branches)

    unexpected = sorted(b for b in actual_branches if not branch_is_expected(b, exact, prefixes))
    if unexpected:
        info["errors"].append(f"unexpected branches not covered by the expected set: {unexpected}")

    missing = []
    for name in sorted(exact):
        if is_data and (name in mc_only_exact or any(name.startswith(p) for p in mc_only_prefixes)):
            continue
        if name not in actual_branches:
            missing.append(name)
    if missing:
        info["warnings"].append(f"expected branches not found: {missing}")

    f.Close()
    return (len(info["errors"]) == 0), info


def main():
    parser = argparse.ArgumentParser(description="Verify 003-ObjectSelectionI skim ROOT files.")
    parser.add_argument("--datasetJSON", required=True, help="Path to selectionI_{tag}_{era}_datasets.json.")
    parser.add_argument("--config", required=True, help="Path to 003-ObjectSelectionI/config.yaml.")
    parser.add_argument("--previousConfig", required=True, help="Path to 002-Samples/config.yaml (for the inherited branch_selection.keep).")
    parser.add_argument("--era", required=True, help="Era (e.g. UL2016postVFP), to look up Modules.selectedObjects.{era}.")
    parser.add_argument("--outputReport", required=True, help="Path to write the JSON verification report.")
    parser.add_argument("--include", help='Regex applied to "DataMC/group/dataset"; only matching triples are checked.')
    parser.add_argument("--exclude", help='Regex applied to "DataMC/group/dataset"; matching triples are skipped.')
    args = parser.parse_args()

    with open(args.previousConfig) as f:
        prev_config = yaml.safe_load(f)
    keep_list = prev_config.get("branch_selection", {}).get("keep", [])
    if not keep_list:
        print("ERROR: previousConfig has no branch_selection.keep list -- nothing to verify the inherited branches against.", file=sys.stderr)
        sys.exit(1)
    exact, prefixes = expand_keep_list(keep_list)

    with open(args.config) as f:
        config = yaml.safe_load(f)
    try:
        selected_objects_cfg = config["Modules"]["selectedObjects"][args.era]
    except KeyError:
        print(f"ERROR: config.yaml has no Modules.selectedObjects.{args.era} -- can't derive expected branch names.", file=sys.stderr)
        sys.exit(1)

    exact = exact | derived_branch_names(selected_objects_cfg)
    mc_only_derived = derived_branch_names_mc_only(selected_objects_cfg)
    exact = exact | mc_only_derived
    mc_only_exact = MC_ONLY_EXACT | mc_only_derived

    with open(args.datasetJSON) as f:
        dataset_data = json.load(f)

    include_pat = re.compile(args.include) if args.include else None
    exclude_pat = re.compile(args.exclude) if args.exclude else None

    report = {"datasets": {}}
    total_files, total_ok, total_errors, total_warnings = 0, 0, 0, 0

    for DataMC, groups in dataset_data.items():
        is_data = DataMC.lower().startswith("data")
        for group, datasets in groups.items():
            for dataset, files in datasets.items():
                label = f"{DataMC}/{group}/{dataset}"
                if include_pat and not include_pat.search(label):
                    continue
                if exclude_pat and exclude_pat.search(label):
                    continue
                filepaths = list(files.keys()) if isinstance(files, dict) else list(files)
                if not filepaths:
                    continue

                print(f"\n=== {label} ({len(filepaths)} files) ===")
                file_reports = []
                dataset_ok = True
                for fp in filepaths:
                    total_files += 1
                    ok, info = check_file_structure(fp, exact, prefixes, is_data, mc_only_exact, MC_ONLY_PREFIXES)
                    file_reports.append(info)
                    if ok:
                        total_ok += 1
                    else:
                        dataset_ok = False
                    total_errors += len(info["errors"])
                    total_warnings += len(info["warnings"])
                    for e in info["errors"]:
                        print(f"  [ERROR] {fp}: {e}")
                    for w in info["warnings"]:
                        print(f"  [WARN]  {fp}: {w}")
                    if not info["errors"] and not info["warnings"]:
                        print(f"  [OK]    {fp} ({info.get('n_entries', '?')} entries, {info.get('n_branches', '?')} branches)")

                report["datasets"][label] = {
                    "is_data": is_data,
                    "ok": dataset_ok,
                    "files": file_reports,
                }

    report["summary"] = {
        "total_files": total_files,
        "files_ok": total_ok,
        "files_with_errors": total_files - total_ok,
        "total_errors": total_errors,
        "total_warnings": total_warnings,
    }

    Path(args.outputReport).parent.mkdir(parents=True, exist_ok=True)
    with open(args.outputReport, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Summary: {total_ok}/{total_files} files OK, {total_errors} errors, {total_warnings} warnings.")
    print(f"Full report written to: {args.outputReport}")

    if total_files == 0:
        print("ERROR: no files found to verify.", file=sys.stderr)
        sys.exit(1)
    if total_ok < total_files:
        sys.exit(1)


if __name__ == "__main__":
    print("Running verifyOutput.py")
    main()
    print("Finished verifyOutput.py")

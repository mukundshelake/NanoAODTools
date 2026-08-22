#!/usr/bin/env python3
"""
Verify preselection output ROOT files: checks each file opens cleanly and has
the "Events" tree, and cross-checks the tree's actual branch list against
config.yaml's branch_selection (catching both missing-expected and
unexpected-extra branches -- the latter usually means keep_and_drop.txt
wasn't applied correctly on the CRAB worker).

Input is a preselection_{era}_datasets.json (from --generatePreselectionDatasetJSON),
keyed {DataMC: {group: {dataset: {filepath: "Events"}}}}.

Usage:
    python3 verifyOutput.py --datasetJSON <preselection_{era}_datasets.json> \\
        --config <config.yaml> --outputReport <report.json> [--filter ...]
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
# MC-only weights) -- real, well-known conventions, not a guess. Data files
# legitimately never have these; treating their absence there as an error
# would just spam false positives on every Data file.
MC_ONLY_EXACT = {
    "Jet_hadronFlavour", "Jet_partonFlavour", "Jet_genJetIdx",
    "Generator_x1", "Generator_x2", "Generator_weight",
    "Generator_xpdf1", "Generator_xpdf2", "Generator_id1", "Generator_id2",
    "LHEWeight_originalXWGTUP", "PSWeight", "LHEScaleWeight", "LHEPdfWeight",
}
MC_ONLY_PREFIXES = ("GenPart", "puWeight")


def expand_keep_list(keep_list):
    """Split branch_selection.keep into (exact_names, wildcard_prefixes)."""
    exact, prefixes = set(), []
    for entry in keep_list:
        if entry.endswith("*"):
            prefixes.append(entry[:-1])
        else:
            exact.add(entry)
    return exact, prefixes


def branch_is_expected(name, exact, prefixes):
    if name in exact or any(name.startswith(p) for p in prefixes):
        return True
    # NanoAOD's collection-size counter convention: "nJet" alongside "Jet_*",
    # "nPSWeight" alongside the "PSWeight" array branch, etc. -- ROOT/
    # NanoAODTools writes these automatically for any jagged branch that's
    # kept, whether or not the "nX" form is itself spelled out in
    # branch_selection.keep (nJet/nMuon happen to be explicit already, but
    # nGenPart/nLHEPdfWeight/nLHEScaleWeight/nPSWeight are not).
    if name.startswith("n") and len(name) > 1:
        stripped = name[1:]
        if stripped in exact or stripped in prefixes:
            return True
    return False


def check_file_structure(filepath, exact, prefixes, is_data):
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
        info["errors"].append(f"unexpected branches not covered by branch_selection.keep: {unexpected}")

    missing = []
    for name in sorted(exact):
        if is_data and (name in MC_ONLY_EXACT or any(name.startswith(p) for p in MC_ONLY_PREFIXES)):
            continue
        if name not in actual_branches:
            missing.append(name)
    if missing:
        info["warnings"].append(f"expected branches not found: {missing}")

    f.Close()
    return (len(info["errors"]) == 0), info


def main():
    parser = argparse.ArgumentParser(description="Verify preselection output ROOT files against config.yaml's branch_selection.")
    parser.add_argument("--datasetJSON", required=True, help="Path to preselection_{era}_datasets.json.")
    parser.add_argument("--config", required=True, help="Path to config.yaml (for branch_selection.keep).")
    parser.add_argument("--outputReport", required=True, help="Path to write the JSON verification report.")
    parser.add_argument("--include", help='Regex applied to "DataMC/group/dataset"; only matching triples are checked.')
    parser.add_argument("--exclude", help='Regex applied to "DataMC/group/dataset"; matching triples are skipped.')
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)
    keep_list = config.get("branch_selection", {}).get("keep", [])
    if not keep_list:
        print(f"ERROR: config.yaml has no branch_selection.keep list -- nothing to verify against.", file=sys.stderr)
        sys.exit(1)
    exact, prefixes = expand_keep_list(keep_list)

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
                    ok, info = check_file_structure(fp, exact, prefixes, is_data)
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

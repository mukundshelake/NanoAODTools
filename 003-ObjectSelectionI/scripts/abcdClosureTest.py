#!/usr/bin/env python3
"""
ABCD closure test: checks N_A ~= N_B*N_C/N_D for a background process (default: QCD)
on already-produced selectionI skims, using the ABCD_region branch SelectedObjectsProducer
already writes (scripts/modules/SelectedObjects.py) -- no re-derivation of the region
boundaries from SelMuon_pfRelIso04_all/MET_pt needed, just a sum over the existing tag.

This is the standard sanity check for whether the two ABCD variables (muon isolation,
MET) are independent enough within the target process for the method to be valid: if
they were perfectly independent, N_A/N_C would equal N_B/N_D exactly, i.e.
N_A == N_B*N_C/N_D. Any large deviation means the two variables are correlated within
this process and the estimate would be biased.

Datasets within a group are combined with the same Lumi*Xsec/Ngen per-dataset weighting
(and sign(LHEWeight_originalXWGTUP) correction) as scripts/plotABCDVariables.py, for
consistency -- see that script's build_group_histogram() docstring for why.

Input is a selectionI_{tag}_{era}_datasets.json (from --generateDatasetJSON), keyed
{DataMC: {group: {dataset: {filepath: "Events"}}}}.

Usage:
    python3 abcdClosureTest.py --datasetJSON <selectionI_{tag}_{era}_datasets.json> \\
        --config <config.yaml> --era <era> --outputReport <report.json> \\
        [--dataMC MC_mu] [--groups QCD]
"""

import argparse
import json
import sys
from pathlib import Path

import ROOT
import yaml

ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gErrorIgnoreLevel = ROOT.kWarning

_REGION_CODES = {"A": 0, "B": 1, "C": 2, "D": 3}


def per_dataset_counts(filepaths, abcd_prefix, weight):
    """Weighted and raw (unweighted) event counts per ABCD region for one dataset.

    Returns (weighted: {region: float}, raw: {region: int}) or (None, None) if the
    ABCD_region branch isn't present (shouldn't happen for a healthy selectionI skim).
    """
    rdf = ROOT.RDataFrame("Events", filepaths)
    available = {str(c) for c in rdf.GetColumnNames()}
    region_branch = f"{abcd_prefix}_region"
    if region_branch not in available:
        return None, None

    if "LHEWeight_originalXWGTUP" in available:
        sign_expr = "(LHEWeight_originalXWGTUP > 0) ? 1.0 : ((LHEWeight_originalXWGTUP < 0) ? -1.0 : 0.0)"
    else:
        sign_expr = "1.0"
    rdf = rdf.Define("_abcd_w", f"({sign_expr}) * {weight}")

    weighted, raw = {}, {}
    for label, code in _REGION_CODES.items():
        filt = rdf.Filter(f"{region_branch} == {code}")
        weighted[label] = filt.Sum("_abcd_w").GetValue()
        raw[label] = filt.Count().GetValue()
    return weighted, raw


def closure_for_group(files_by_dataset, abcd_prefix, ngen_xsec, lumi):
    """Combine all datasets in a group (e.g. QCD's pT bins) into total weighted and
    raw region counts, mirroring plotABCDVariables.build_group_histogram()'s
    per-dataset Lumi*Xsec/Ngen weighting convention.
    """
    weighted_total = {label: 0.0 for label in _REGION_CODES}
    raw_total = {label: 0 for label in _REGION_CODES}
    per_dataset = {}

    for dataset, files in files_by_dataset.items():
        filepaths = list(files.keys()) if isinstance(files, dict) else list(files)
        if not filepaths:
            continue
        if dataset not in ngen_xsec:
            print(f"    [WARN] No Ngen/Xsec entry for dataset '{dataset}'; skipping.")
            continue
        ngen = ngen_xsec[dataset]['Ngen']
        xsec = ngen_xsec[dataset]['Xsec']
        if ngen <= 0:
            print(f"    [WARN] Ngen <= 0 for dataset '{dataset}'; skipping.")
            continue
        weight = lumi * xsec / ngen

        weighted, raw = per_dataset_counts(filepaths, abcd_prefix, weight)
        if weighted is None:
            print(f"    [WARN] '{abcd_prefix}_region' not found for dataset '{dataset}'; skipping.")
            continue

        per_dataset[dataset] = {"weighted": weighted, "raw": raw, "n_files": len(filepaths), "weight": weight}
        for label in _REGION_CODES:
            weighted_total[label] += weighted[label]
            raw_total[label] += raw[label]
        print(f"    {dataset}: raw A/B/C/D = {raw['A']}/{raw['B']}/{raw['C']}/{raw['D']} "
              f"(weight={weight:.6g})")

    return weighted_total, raw_total, per_dataset


def compute_closure(weighted_total, raw_total):
    n_a, n_b, n_c, n_d = (weighted_total[l] for l in ("A", "B", "C", "D"))
    result = {
        "N_A": n_a, "N_B": n_b, "N_C": n_c, "N_D": n_d,
        "raw_counts": raw_total,
    }
    if n_d == 0:
        result["N_A_predicted"] = None
        result["closure_ratio"] = None
        result["non_closure_pct"] = None
        result["warning"] = "N_D == 0 -- cannot compute N_A_predicted = N_B*N_C/N_D."
        return result

    n_a_pred = n_b * n_c / n_d
    result["N_A_predicted"] = n_a_pred
    if n_a == 0:
        result["closure_ratio"] = None
        result["non_closure_pct"] = None
        result["warning"] = "N_A == 0 -- cannot compute closure ratio N_A_predicted/N_A."
    else:
        result["closure_ratio"] = n_a_pred / n_a
        result["non_closure_pct"] = (n_a_pred - n_a) / n_a * 100.0

    low_stat = [l for l in ("A", "B", "C", "D") if raw_total[l] < 50]
    if low_stat:
        result["low_stat_warning"] = (
            f"Regions {low_stat} have < 50 raw (unweighted) events -- closure ratio may be "
            f"statistically unreliable, not just a real non-closure effect."
        )
    return result


def main():
    parser = argparse.ArgumentParser(
        description="ABCD closure test (N_A ~= N_B*N_C/N_D) on selectionI skims' ABCD_region branch.")
    parser.add_argument("--datasetJSON", required=True, help="Path to selectionI_{tag}_{era}_datasets.json.")
    parser.add_argument("--config", required=True, help="Path to config.yaml.")
    parser.add_argument("--era", required=True, help="Era whose DataLumiInfo/NgenandXsec/branchNames to use.")
    parser.add_argument("--outputReport", required=True, help="Path to write the JSON closure report.")
    parser.add_argument("--dataMC", default="MC_mu", help="DataMC key to test from (default: MC_mu).")
    parser.add_argument("--groups", nargs="+", default=["QCD"],
                        help="Groups to closure-test (default: QCD -- the actual ABCD estimate target).")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    mod_cfg_raw = config.get("Modules", {}).get("selectedObjects", {})
    mod_cfg = mod_cfg_raw.get(args.era, mod_cfg_raw)
    branch_names = mod_cfg.get("branchNames")
    if not branch_names:
        print(f"ERROR: config.yaml has no Modules.selectedObjects[{args.era}].branchNames.", file=sys.stderr)
        sys.exit(1)
    abcd_prefix = branch_names["abcdRegion"]

    lumi = config["DataLumiInfo"][args.era]["Lumi"]
    ngen_xsec_era = config["NgenandXsec"].get(args.era, {}).get(args.dataMC, {})

    with open(args.datasetJSON) as f:
        dataset_data = json.load(f)
    if args.dataMC not in dataset_data:
        print(f"ERROR: '{args.dataMC}' not found in {args.datasetJSON}.", file=sys.stderr)
        sys.exit(1)

    report = {"era": args.era, "dataMC": args.dataMC, "groups": {}}
    any_nonclosure_ge_20pct = False

    for group in args.groups:
        if group not in dataset_data[args.dataMC]:
            print(f"[WARN] Group '{group}' not found under {args.dataMC}; skipping.")
            continue
        print(f"\n=== {args.era} / {args.dataMC} / {group} ===")
        ngen_xsec_group = ngen_xsec_era.get(group, {})
        weighted_total, raw_total, per_dataset = closure_for_group(
            dataset_data[args.dataMC][group], abcd_prefix, ngen_xsec_group, lumi)
        closure = compute_closure(weighted_total, raw_total)
        closure["per_dataset"] = per_dataset
        report["groups"][group] = closure

        print(f"  Weighted: N_A={closure['N_A']:.4g}  N_B={closure['N_B']:.4g}  "
              f"N_C={closure['N_C']:.4g}  N_D={closure['N_D']:.4g}")
        print(f"  Raw counts: {closure['raw_counts']}")
        if closure.get("N_A_predicted") is not None:
            print(f"  N_A_predicted = N_B*N_C/N_D = {closure['N_A_predicted']:.4g}")
            print(f"  closure_ratio = N_A_predicted / N_A = {closure['closure_ratio']:.4f} "
                  f"({closure['non_closure_pct']:+.1f}% non-closure)")
            if abs(closure['non_closure_pct']) >= 20.0:
                any_nonclosure_ge_20pct = True
                print(f"  [WARN] Non-closure >= 20% for group '{group}'.")
        if closure.get("warning"):
            print(f"  [WARN] {closure['warning']}")
        if closure.get("low_stat_warning"):
            print(f"  [WARN] {closure['low_stat_warning']}")

    Path(args.outputReport).parent.mkdir(parents=True, exist_ok=True)
    with open(args.outputReport, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nFull report written to: {args.outputReport}")

    if any_nonclosure_ge_20pct:
        print("\nWARNING: at least one group shows >= 20% non-closure -- review before "
              "trusting the ABCD estimate for that group.", file=sys.stderr)


if __name__ == "__main__":
    main()

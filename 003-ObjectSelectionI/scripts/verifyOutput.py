#!/usr/bin/env python3
"""
Verify 003-ObjectSelectionI skim output ROOT files.

Unlike 002-Samples' preselection stage (which keeps a curated subset of
NanoAOD branches via branch_selection.keep, so its verifyOutput.py checks
every branch against that allowlist), this stage's skims retain ALL original
NanoAOD branches untouched and additionally write the flat SelMuon_*,
leading[b]Jet_*, subleading[b]Jet_* and sel_nJet/sel_nbjet branches produced
by SelectedObjectsProducer (scripts/modules/SelectedObjects.py). There's no
drop list here, so an "every branch against an allowlist" check isn't
meaningful. This script is instead scoped to the branches THIS stage
creates/updates:

  - structural check: file opens, Events tree exists
  - existence check: every branch SelectedObjectsProducer.beginFile() writes
    for this era/is_mc is present (era-resolved from config.yaml's
    Modules.selectedObjects.branchNames, so this doesn't reimplement any of
    the module's selection logic -- just its beginFile() branch list). A
    missing branch here is always a hard error: unlike 002's Data/MC-only
    NanoAOD branches, there's no legitimate reason this stage's own output
    branches would be absent from a successfully-produced skim.
  - min/max/mean/stddev per new branch, over the real event population
    (chained across a dataset's files via RDataFrame, same pattern as
    002-Samples' verifyOutput.py)
  - two invariants that must ALWAYS hold given SelectedObjectsProducer's
    deterministic greedy jet-assignment algorithm (_fill_jets in
    scripts/modules/SelectedObjects.py); any violation means a real bug,
    not a legitimate edge case:
      sel_nbjet <= sel_nJet
      leadingbJet/subleadingbJet slot filled iff sel_nbjet >= 1/2
      leadingJet/subleadingJet slot filled iff the number of light-jet slots
        the algorithm assigns -- TMath::Min(2, sel_nJet - TMath::Min(sel_nbjet, 2))
        -- is >= 1/2
  - sentinel-fraction reporting per object (*_pt == -1 rate): informative,
    not a hard failure. SelectionCuts already requires >=1 muon / >=4 jets /
    >=2 b-jets before this module ever runs, so in a healthy pipeline these
    fractions should be ~0; a nonzero rate is flagged as a warning since it
    can point at a cut-string vs module boundary-condition mismatch (e.g. a
    ">" vs ">=" difference) that utils.validate_selection_cuts_consistency
    in run_all.py can't catch since it only compares threshold *values*.

Input is a selectionI_{tag}_{era}_datasets.json (from --generateDatasetJSON),
keyed {DataMC: {group: {dataset: {filepath: "Events"}}}}.

Usage:
    python3 verifyOutput.py --datasetJSON <selectionI_{tag}_{era}_datasets.json> \\
        --config <config.yaml> --era <era> --outputReport <report.json> \\
        [--filter ...] [--maxEntriesForStats 500000]
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

# Mirrors SelectedObjectsProducer's own field lists exactly (scripts/modules/SelectedObjects.py).
_MUON_FLOAT_FIELDS = ["pt", "eta", "phi", "mass", "pfRelIso04_all"]
_MUON_INT_FIELDS   = ["charge"]
_JET_FLOAT_FIELDS  = ["pt", "eta", "phi", "mass", "btagDeepFlavB"]
_JET_INT_FIELDS    = ["jetId", "puId"]
_JET_INT_FIELDS_MC = ["hadronFlavour"]
_JET_KEYS          = ["leadingbJet", "subleadingbJet", "leadingJet", "subleadingJet"]

# Mirrors SelectedObjectsProducer._fill_jets' light-jet slot count exactly:
# ljets = next 2 highest-pT selected jets not already used as b-jets.
_LIGHT_JET_SLOTS_EXPR = "TMath::Min(2, sel_nJet - TMath::Min(sel_nbjet, 2))"


def expected_new_branches(branch_names, is_mc):
    """The exact branch set SelectedObjectsProducer.beginFile() writes."""
    expected = set()
    muon_prefix = branch_names["muon"]
    for field in _MUON_FLOAT_FIELDS + _MUON_INT_FIELDS:
        expected.add(f"{muon_prefix}_{field}")
    expected.add(f"{muon_prefix}_tightId")

    for jet_key in _JET_KEYS:
        prefix = branch_names[jet_key]
        for field in _JET_FLOAT_FIELDS + _JET_INT_FIELDS:
            expected.add(f"{prefix}_{field}")
        if is_mc:
            for field in _JET_INT_FIELDS_MC:
                expected.add(f"{prefix}_{field}")

    expected.add("sel_nJet")
    expected.add("sel_nbjet")

    abcd_prefix = branch_names["abcdRegion"]
    expected.add(f"{abcd_prefix}_isTightIso")
    expected.add(f"{abcd_prefix}_isHighMTW")
    expected.add(f"{abcd_prefix}_mTW")
    expected.add(f"{abcd_prefix}_region")
    return expected


def check_file_structure(filepath, expected_branches):
    """Open one file, check tree existence/health and that every branch this
    stage is supposed to write is actually present.

    Returns (ok: bool, info: dict). Unlike 002-Samples' verifyOutput.py,
    a missing expected branch is a hard error here, not a warning -- see
    module docstring.
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

    missing = sorted(expected_branches - actual_branches)
    if missing:
        info["errors"].append(f"SelectedObjectsProducer branches missing from output: {missing}")

    f.Close()
    return (len(info["errors"]) == 0), info


def compute_dataset_stats(filepaths, new_branches, max_entries_for_stats):
    """min/max/mean/stddev for just the branches this stage creates/updates
    -- not the hundreds of original NanoAOD branches passed through
    untouched, which are 002-Samples' concern, not this stage's.

    Uses RDataFrame's lazy Min/Max/Mean/StdDev actions for the same reason
    002-Samples' verifyOutput.py does: TTree::Draw + GetV1() needs
    SetEstimate() sized correctly per branch and got this wrong once,
    causing a real segfault. RDataFrame actions avoid that failure mode and
    batch all branches into one combined pass over the data.
    """
    rdf = ROOT.RDataFrame("Events", filepaths)
    n_entries = rdf.Count().GetValue()
    stats = {"n_files": len(filepaths), "n_entries": n_entries, "branches": {}}
    if n_entries == 0:
        return stats

    if n_entries > max_entries_for_stats:
        rdf = rdf.Range(max_entries_for_stats)
    stats["n_entries_used_for_stats"] = min(n_entries, max_entries_for_stats)

    available = {str(c) for c in rdf.GetColumnNames()}
    booked = {}
    for name in sorted(new_branches):
        if name not in available:
            continue  # already reported as a structural error per-file
        col_type = str(rdf.GetColumnType(name))
        try:
            booked[name] = {
                "type": col_type,
                "min": rdf.Min(name),
                "max": rdf.Max(name),
                "mean": rdf.Mean(name),
                "std": rdf.StdDev(name),
            }
        except Exception as e:
            stats["branches"][name] = {"type": col_type, "error": f"could not book stats action: {e}"}

    # Lazy: the event loop actually runs on the first GetValue() below, in
    # one combined pass across every booked action.
    for name, r in booked.items():
        try:
            stats["branches"][name] = {
                "type": r["type"],
                "min": r["min"].GetValue(),
                "max": r["max"].GetValue(),
                "mean": r["mean"].GetValue(),
                "std": r["std"].GetValue(),
            }
        except Exception as e:
            stats["branches"][name] = {"type": r["type"], "error": str(e)}

    return stats


def check_invariants(filepaths, branch_names, abcd_cfg, max_entries_for_stats):
    """Cross-branch invariants that must ALWAYS hold given
    SelectedObjectsProducer's deterministic algorithm -- see module
    docstring. Any nonzero violation count is a real bug.
    """
    rdf = ROOT.RDataFrame("Events", filepaths)
    n_entries = rdf.Count().GetValue()
    result = {"n_entries_checked": 0, "violations": {}, "sentinel_fraction": {}, "region_fraction": {}}
    if n_entries == 0:
        return result
    if n_entries > max_entries_for_stats:
        rdf = rdf.Range(max_entries_for_stats)
    n_used = min(n_entries, max_entries_for_stats)
    result["n_entries_checked"] = n_used

    lb, slb = branch_names["leadingbJet"], branch_names["subleadingbJet"]
    lj, slj = branch_names["leadingJet"], branch_names["subleadingJet"]
    muon_prefix = branch_names["muon"]
    abcd_prefix = branch_names["abcdRegion"]
    iso_low_max  = abcd_cfg["isoLowMax"]
    iso_high_min = abcd_cfg["isoHighMin"]
    mtw_low_max  = abcd_cfg["mTWLowMax"]
    mtw_high_min = abcd_cfg["mTWHighMin"]
    has_muon     = f"{muon_prefix}_pt > -0.5"

    # Recomputed directly from thresholds -- NOT from the persisted isTightIso/
    # isHighMTW booleans, so these checks actually catch a mistagged branch
    # rather than just checking self-consistency between two derived fields.
    is_tight_iso_expr = f"({muon_prefix}_pfRelIso04_all <= {iso_low_max})"
    is_loose_iso_expr = f"({muon_prefix}_pfRelIso04_all >= {iso_high_min})"
    is_low_mtw_expr   = f"({abcd_prefix}_mTW < {mtw_low_max})"
    is_high_mtw_expr  = f"({abcd_prefix}_mTW >= {mtw_high_min})"
    # With isoLowMax==isoHighMin and mTWLowMax==mTWHighMin (today's default)
    # every muon-having event lands in exactly one of the four branches below;
    # the final -1 only becomes reachable once the low/high pair for either
    # axis is set apart, opening a gap that's neither tight/loose nor low/high.
    expected_region_expr = (
        f"!({has_muon}) ? -1 : "
        f"(({is_tight_iso_expr}) && ({is_low_mtw_expr})) ? 2 : "
        f"(({is_tight_iso_expr}) && ({is_high_mtw_expr})) ? 0 : "
        f"(({is_loose_iso_expr}) && ({is_low_mtw_expr})) ? 3 : "
        f"(({is_loose_iso_expr}) && ({is_high_mtw_expr})) ? 1 : -1"
    )

    checks = {
        "sel_nbjet_exceeds_sel_nJet": "sel_nbjet > sel_nJet",
        f"{lb}_slot_inconsistent":  f"(sel_nbjet >= 1) != ({lb}_pt > -0.5)",
        f"{slb}_slot_inconsistent": f"(sel_nbjet >= 2) != ({slb}_pt > -0.5)",
        f"{lj}_slot_inconsistent":  f"({_LIGHT_JET_SLOTS_EXPR} >= 1) != ({lj}_pt > -0.5)",
        f"{slj}_slot_inconsistent": f"({_LIGHT_JET_SLOTS_EXPR} >= 2) != ({slj}_pt > -0.5)",
        # ABCD region tagging (SelectedObjectsProducer._fill_abcd_region): must
        # always hold given that module's deterministic region-code assignment.
        f"{abcd_prefix}_region_out_of_range":
            f"({abcd_prefix}_region < -1) || ({abcd_prefix}_region > 3)",
        # region == -1 no longer implies "no muon" one-to-one (it also covers
        # iso/mTW falling in a gap) -- only the one-directional implication
        # "no muon => region == -1" still always holds.
        f"{abcd_prefix}_region_undefined_without_muon":
            f"!({has_muon}) && ({abcd_prefix}_region != -1)",
        f"{abcd_prefix}_isTightIso_inconsistent":
            f"({has_muon}) && ({abcd_prefix}_isTightIso != {is_tight_iso_expr})",
        f"{abcd_prefix}_isHighMTW_inconsistent":
            f"({has_muon}) && ({abcd_prefix}_isHighMTW != {is_high_mtw_expr})",
        f"{abcd_prefix}_region_code_inconsistent":
            f"{abcd_prefix}_region != ({expected_region_expr})",
    }
    for label, expr in checks.items():
        try:
            result["violations"][label] = rdf.Filter(expr).Count().GetValue()
        except Exception as e:
            result["violations"][label] = f"check failed: {e}"

    sentinel_branches = {
        "muon": f"{muon_prefix}_pt",
        "leadingbJet": f"{lb}_pt", "subleadingbJet": f"{slb}_pt",
        "leadingJet": f"{lj}_pt", "subleadingJet": f"{slj}_pt",
    }
    for label, branch in sentinel_branches.items():
        try:
            n_missing = rdf.Filter(f"{branch} < -0.5").Count().GetValue()
            result["sentinel_fraction"][label] = n_missing / n_used
        except Exception as e:
            result["sentinel_fraction"][label] = f"check failed: {e}"

    region_labels = {-1: "undefined", 0: "A", 1: "B", 2: "C", 3: "D"}
    for code, label in region_labels.items():
        try:
            n = rdf.Filter(f"{abcd_prefix}_region == {code}").Count().GetValue()
            result["region_fraction"][label] = n / n_used
        except Exception as e:
            result["region_fraction"][label] = f"check failed: {e}"

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Verify 003-ObjectSelectionI skim output against the branches SelectedObjectsProducer creates/updates."
    )
    parser.add_argument("--datasetJSON", required=True, help="Path to selectionI_{tag}_{era}_datasets.json.")
    parser.add_argument("--config", required=True, help="Path to config.yaml.")
    parser.add_argument("--era", required=True, help="Era whose Modules.selectedObjects config to check against (e.g. UL2018).")
    parser.add_argument("--outputReport", required=True, help="Path to write the JSON verification report.")
    parser.add_argument("--include", help='Regex applied to "DataMC/group/dataset"; only matching triples are checked.')
    parser.add_argument("--exclude", help='Regex applied to "DataMC/group/dataset"; matching triples are skipped.')
    parser.add_argument("--maxEntriesForStats", type=int, default=500_000,
                        help="Cap on entries drawn per dataset for stats/invariants. Default: 500000.")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)
    mod_cfg_raw = config.get("Modules", {}).get("selectedObjects", {})
    mod_cfg = mod_cfg_raw.get(args.era, mod_cfg_raw)
    branch_names = mod_cfg.get("branchNames")
    if not branch_names:
        print(f"ERROR: config.yaml has no Modules.selectedObjects[{args.era}].branchNames -- nothing to verify against.", file=sys.stderr)
        sys.exit(1)
    abcd_cfg = mod_cfg.get("abcdRegion")
    if not abcd_cfg:
        print(f"ERROR: config.yaml has no Modules.selectedObjects[{args.era}].abcdRegion -- nothing to verify ABCD tagging against.", file=sys.stderr)
        sys.exit(1)

    with open(args.datasetJSON) as f:
        dataset_data = json.load(f)

    include_pat = re.compile(args.include) if args.include else None
    exclude_pat = re.compile(args.exclude) if args.exclude else None

    report = {"era": args.era, "datasets": {}}
    total_files, total_ok, total_errors, total_warnings = 0, 0, 0, 0

    for DataMC, groups in dataset_data.items():
        is_data = DataMC.lower().startswith("data")
        expected_branches = expected_new_branches(branch_names, is_mc=not is_data)
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
                    ok, info = check_file_structure(fp, expected_branches)
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
                        print(f"  [OK]    {fp} ({info.get('n_entries', '?')} entries)")

                stats = compute_dataset_stats(filepaths, expected_branches, args.maxEntriesForStats)
                invariants = check_invariants(filepaths, branch_names, abcd_cfg, args.maxEntriesForStats)

                bad_invariants = {k: v for k, v in invariants["violations"].items() if isinstance(v, int) and v > 0}
                if bad_invariants:
                    dataset_ok = False
                    total_errors += len(bad_invariants)
                    for k, v in bad_invariants.items():
                        print(f"  [ERROR] {label}: invariant '{k}' violated in {v} event(s)")
                for slabel, frac in invariants["sentinel_fraction"].items():
                    if isinstance(frac, float) and frac > 0:
                        total_warnings += 1
                        print(f"  [WARN]  {label}: {slabel} missing (sentinel) in {frac:.1%} of events")
                region_report = {k: f"{v:.1%}" for k, v in invariants["region_fraction"].items() if isinstance(v, float)}
                if region_report:
                    print(f"  [INFO]  {label}: ABCD region split -- {region_report}")

                report["datasets"][label] = {
                    "is_data": is_data,
                    "ok": dataset_ok,
                    "files": file_reports,
                    "stats": stats,
                    "invariants": invariants,
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

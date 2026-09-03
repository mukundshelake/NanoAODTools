#!/usr/bin/env python3
"""
Compute the data-driven ABCD (QCD multijet) transfer factor, binned in
(SelMuon_pt, |SelMuon_eta|), from selectionII skims -- i.e. after the muon
ID/HLT and b-tagging scale factors (this chapter's own SF modules) have
already been written, so the background subtraction below uses exactly the
same per-event weight definition as the rest of the analysis (this
chapter's config.yaml weightList.MC / 003-ObjectSelectionIII's weightList),
not just Lumi*Xsec/Ngen*sign(LHEWeight_originalXWGTUP).

This used to live in 003-ObjectSelectionI, using only Lumi*Xsec/Ngen and the
LHE-weight sign, because the SF branches didn't exist yet at that stage --
see that chapter's README. Moving it here, after the SF-writing modules run
but with no ABCDTransferWeight module left in this chapter's ModuleList
(the transfer factor R computed here is now looked up live by
003-ObjectSelectionIII at histogram-fill time instead of being pre-baked
into a skim branch), avoids the circular dependency that keeping R inside
this same PostProcessor pass would create: R can't be an input to the pass
that's also required to produce the branches R itself depends on.

Per (pt, |eta|) bin:
    N_data_X   = raw Data count in region X (weight = 1)
    N_bkg_X    = Lumi*Xsec/Ngen-weighted sum of every MC_mu group *except*
                 --qcdGroup (the known, non-QCD backgrounds), for region X --
                 each event additionally weighted by every branch in
                 config.yaml's weightList.MC that's present on the skim
                 (muonIDWeight, muonHLTWeight, muonIsoWeight, bTagWeight,
                 L1PreFiringWeight_Nom, lheWeightSign)
    N_qcd_X    = max(N_data_X - N_bkg_X, 0)   (floored at 0 -- a negative
                 data-driven QCD count is unphysical; floors get reported)
    R          = N_qcd_C / N_qcd_D             (the "transfer factor")
    N_A_pred   = N_qcd_B * N_qcd_C / N_qcd_D = R * N_qcd_B

Input is a selectionII_{tag}_{era}_datasets.json (from --generateDatasetJSON),
keyed {DataMC: {group: {dataset: {filepath: "Events"}}}}.

Usage:
    python3 computeABCDScaleFactor.py --datasetJSON <selectionII_{tag}_{era}_datasets.json> \\
        --config <config.yaml> --era <era> --outputDir <dir> \\
        [--dataDataMC Data_mu] [--dataGroup SingleMuon] [--mcDataMC MC_mu] [--qcdGroup QCD]
"""

import argparse
import array
import json
import sys
from pathlib import Path

import ROOT
import yaml

ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gErrorIgnoreLevel = ROOT.kWarning

# Fixed binning -- see module docstring. Deliberately coarse: 003-ObjectSelectionI's
# closure test (abcdClosureTest.py) already showed QCD MC statistics are thin even
# in a single flat bin, so a finer 2D grid would fragment them further.
PT_EDGES  = [0.0, 35.0, 50.0, 1.0e6]
ETA_EDGES = [0.0, 1.2, 2.4]

_REGION_CODES = {"B": 1, "C": 2, "D": 3}


def _make_model(name):
    pt_arr  = array.array('d', PT_EDGES)
    eta_arr = array.array('d', ETA_EDGES)
    return ROOT.RDF.TH2DModel(name, "", len(PT_EDGES) - 1, pt_arr, len(ETA_EDGES) - 1, eta_arr)


def region_hists_for_dataset(filepaths, muon_prefix, abcd_prefix, flat_weight, is_mc, unique_tag, mc_sf_branches):
    """2D (pt, |eta|) histograms of the muon-selection branches for each of
    B/C/D, for one dataset. `flat_weight` is the per-dataset scalar (1.0 for
    data, Lumi*Xsec/Ngen for MC); for MC it's additionally multiplied by every
    branch in `mc_sf_branches` (config.yaml's weightList.MC) that's present
    on this dataset -- see module docstring.
    """
    rdf = ROOT.RDataFrame("Events", filepaths)
    available = {str(c) for c in rdf.GetColumnNames()}
    region_branch = f"{abcd_prefix}_region"
    pt_branch, eta_branch = f"{muon_prefix}_pt", f"{muon_prefix}_eta"
    if region_branch not in available or pt_branch not in available:
        return None

    weight_terms = [str(flat_weight)]
    if is_mc:
        for branch in mc_sf_branches:
            if branch in available:
                weight_terms.append(branch)
            else:
                print(f"    [WARN] '{branch}' not found on this dataset; treating as 1.0.")
    weight_expr = " * ".join(weight_terms)

    rdf = rdf.Define("_sf_w", weight_expr).Define("_abs_eta", f"abs({eta_branch})")
    hists = {}
    for label, code in _REGION_CODES.items():
        model = _make_model(f"h_{label}_{unique_tag}")
        hists[label] = rdf.Filter(f"{region_branch} == {code}") \
                           .Histo2D(model, pt_branch, "_abs_eta", "_sf_w").GetValue()
        hists[label].SetDirectory(0)
    return hists


def sum_hists(hist_list):
    total = None
    for h in hist_list:
        if h is None:
            continue
        if total is None:
            total = h.Clone()
            total.SetDirectory(0)
        else:
            total.Add(h)
    return total


def combine_group(files_by_dataset, muon_prefix, abcd_prefix, ngen_xsec, lumi, is_mc, mc_sf_branches):
    """Combines every dataset in a group into per-region summed 2D histograms.
    ngen_xsec is None for Data (weight fixed at 1.0 per event, no MC scaling).
    """
    per_region = {label: [] for label in _REGION_CODES}
    for i, (dataset, files) in enumerate(files_by_dataset.items()):
        filepaths = list(files.keys()) if isinstance(files, dict) else list(files)
        if not filepaths:
            continue

        if ngen_xsec is None:
            flat_weight = 1.0
        else:
            if dataset not in ngen_xsec:
                print(f"    [WARN] No Ngen/Xsec entry for dataset '{dataset}'; skipping.")
                continue
            ngen, xsec = ngen_xsec[dataset]['Ngen'], ngen_xsec[dataset]['Xsec']
            if ngen <= 0:
                print(f"    [WARN] Ngen <= 0 for dataset '{dataset}'; skipping.")
                continue
            flat_weight = lumi * xsec / ngen

        hists = region_hists_for_dataset(filepaths, muon_prefix, abcd_prefix, flat_weight, is_mc, f"{dataset}_{i}", mc_sf_branches)
        if hists is None:
            print(f"    [WARN] Required branches not found for dataset '{dataset}'; skipping.")
            continue
        for label in _REGION_CODES:
            per_region[label].append(hists[label])
        print(f"    Added dataset '{dataset}' ({len(filepaths)} files, flat_weight={flat_weight:.6g})")
    return {label: sum_hists(hlist) for label, hlist in per_region.items()}


def floor_at_zero(hist, label, report):
    """Floors negative bin content at 0 in place; records how many bins/how
    much yield got floored (a negative data-driven QCD count is unphysical --
    usually a sign the background subtraction over-shot in a low-stat bin).
    """
    n_floored, total_negative = 0, 0.0
    for ix in range(1, hist.GetNbinsX() + 1):
        for iy in range(1, hist.GetNbinsY() + 1):
            v = hist.GetBinContent(ix, iy)
            if v < 0:
                n_floored += 1
                total_negative += v
                hist.SetBinContent(ix, iy, 0.0)
    if n_floored:
        report.setdefault("floored_bins", {})[label] = {
            "n_bins_floored": n_floored, "total_negative_yield": total_negative,
        }
        print(f"    [WARN] {label}: floored {n_floored} negative bin(s), "
              f"total negative yield {total_negative:.3g}")


def main():
    parser = argparse.ArgumentParser(
        description="Compute the (pt, |eta|)-binned data-driven ABCD transfer factor for QCD, "
                     "from fully SF-weighted selectionII skims.")
    parser.add_argument("--datasetJSON", required=True, help="Path to selectionII_{tag}_{era}_datasets.json.")
    parser.add_argument("--config", required=True, help="Path to config.yaml.")
    parser.add_argument("--era", required=True, help="Era whose DataLumiInfo/NgenandXsec/branchNames to use.")
    parser.add_argument("--outputDir", required=True, help="Directory to write the ROOT/JSON output.")
    parser.add_argument("--dataDataMC", default="Data_mu", help="DataMC key for Data (default: Data_mu).")
    parser.add_argument("--dataGroup", default="SingleMuon", help="Group under --dataDataMC to use (default: SingleMuon).")
    parser.add_argument("--mcDataMC", default="MC_mu", help="DataMC key for background MC (default: MC_mu).")
    parser.add_argument("--qcdGroup", default="QCD", help="Group under --mcDataMC to exclude from the background sum (default: QCD).")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)
    branch_names = config.get("Modules", {}).get("abcdScaleFactor", {}).get("branchNames")
    if not branch_names:
        print("ERROR: config.yaml has no Modules.abcdScaleFactor.branchNames.", file=sys.stderr)
        sys.exit(1)
    muon_prefix, abcd_prefix = branch_names["muon"], branch_names["abcdRegion"]

    mc_sf_branches = config.get("weightList", {}).get("MC")
    if not mc_sf_branches:
        print("ERROR: config.yaml has no weightList.MC.", file=sys.stderr)
        sys.exit(1)

    lumi = config["DataLumiInfo"][args.era]["Lumi"]
    ngen_xsec_era = config["NgenandXsec"].get(args.era, {}).get(args.mcDataMC, {})

    with open(args.datasetJSON) as f:
        dataset_data = json.load(f)

    if args.dataDataMC not in dataset_data or args.dataGroup not in dataset_data[args.dataDataMC]:
        print(f"ERROR: '{args.dataDataMC}/{args.dataGroup}' not found in {args.datasetJSON}.", file=sys.stderr)
        sys.exit(1)
    if args.mcDataMC not in dataset_data:
        print(f"ERROR: '{args.mcDataMC}' not found in {args.datasetJSON}.", file=sys.stderr)
        sys.exit(1)

    print(f"\n=== {args.era}: Data ({args.dataDataMC}/{args.dataGroup}) ===")
    data_hists = combine_group(
        dataset_data[args.dataDataMC][args.dataGroup], muon_prefix, abcd_prefix,
        ngen_xsec=None, lumi=lumi, is_mc=False, mc_sf_branches=mc_sf_branches)

    bkg_groups = [g for g in dataset_data[args.mcDataMC] if g != args.qcdGroup]
    print(f"\n=== {args.era}: Background MC ({args.mcDataMC}, groups={bkg_groups}) ===")
    bkg_per_group = {}
    for group in bkg_groups:
        print(f"  Group: {group}")
        ngen_xsec_group = ngen_xsec_era.get(group, {})
        bkg_per_group[group] = combine_group(
            dataset_data[args.mcDataMC][group], muon_prefix, abcd_prefix,
            ngen_xsec=ngen_xsec_group, lumi=lumi, is_mc=True, mc_sf_branches=mc_sf_branches)
    bkg_hists = {label: sum_hists([bkg_per_group[g][label] for g in bkg_groups])
                 for label in _REGION_CODES}

    report = {"era": args.era, "pt_edges": PT_EDGES, "eta_edges": ETA_EDGES}
    qcd_hists = {}
    for label in _REGION_CODES:
        h = data_hists[label].Clone(f"h_qcd_{label}")
        h.Add(bkg_hists[label], -1.0)
        floor_at_zero(h, label, report)
        qcd_hists[label] = h

    n_pt, n_eta = len(PT_EDGES) - 1, len(ETA_EDGES) - 1
    r_hist = qcd_hists["D"].Clone("h_transferFactor_R")
    r_hist.Reset()
    pred_hist = qcd_hists["D"].Clone("h_NA_predicted")
    pred_hist.Reset()

    print(f"\n=== {args.era}: per-bin results ===")
    bins_report = []
    for ix in range(1, n_pt + 1):
        for iy in range(1, n_eta + 1):
            n_b = qcd_hists["B"].GetBinContent(ix, iy)
            n_c = qcd_hists["C"].GetBinContent(ix, iy)
            n_d = qcd_hists["D"].GetBinContent(ix, iy)
            r = (n_c / n_d) if n_d > 0 else float("nan")
            n_a_pred = (n_b * n_c / n_d) if n_d > 0 else float("nan")
            r_hist.SetBinContent(ix, iy, r if n_d > 0 else 0.0)
            pred_hist.SetBinContent(ix, iy, n_a_pred if n_d > 0 else 0.0)
            bins_report.append({
                "pt_range": [PT_EDGES[ix - 1], PT_EDGES[ix]],
                "eta_range": [ETA_EDGES[iy - 1], ETA_EDGES[iy]],
                "N_data_B": data_hists["B"].GetBinContent(ix, iy),
                "N_data_C": data_hists["C"].GetBinContent(ix, iy),
                "N_data_D": data_hists["D"].GetBinContent(ix, iy),
                "N_bkg_B": bkg_hists["B"].GetBinContent(ix, iy),
                "N_bkg_C": bkg_hists["C"].GetBinContent(ix, iy),
                "N_bkg_D": bkg_hists["D"].GetBinContent(ix, iy),
                "N_qcd_B": n_b, "N_qcd_C": n_c, "N_qcd_D": n_d,
                "R": r, "N_A_predicted": n_a_pred,
            })
            print(f"  pt[{PT_EDGES[ix-1]:.0f},{PT_EDGES[ix]:.0f}) eta[{ETA_EDGES[iy-1]:.1f},{ETA_EDGES[iy]:.1f}): "
                  f"N_qcd(B,C,D)=({n_b:.4g},{n_c:.4g},{n_d:.4g})  R={r:.4g}  N_A_pred={n_a_pred:.4g}")

    report["bins"] = bins_report

    out_dir = Path(args.outputDir)
    out_dir.mkdir(parents=True, exist_ok=True)
    root_path = out_dir / f"abcdScaleFactor_{args.era}.root"
    root_file = ROOT.TFile(str(root_path), "RECREATE")
    for label in _REGION_CODES:
        data_hists[label].Write(f"ABCD_data_{label}")
        bkg_hists[label].Write(f"ABCD_bkg_{label}")
        qcd_hists[label].Write(f"ABCD_qcd_{label}")
    r_hist.Write("ABCD_transferFactor_R")
    pred_hist.Write("ABCD_NA_predicted")
    root_file.Close()

    report_path = out_dir / f"abcdScaleFactor_{args.era}_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nDone. ROOT file: {root_path}")
    print(f"JSON report: {report_path}")


if __name__ == "__main__":
    main()

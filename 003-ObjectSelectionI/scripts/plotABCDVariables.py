#!/usr/bin/env python3
"""
Plot ABCD-plane variable distributions (e.g. muon isolation, MET) straight
from 003-ObjectSelectionI skims, ahead of picking any region-boundary
values for an ABCD-method QCD estimate.

Overlays normalized (unit-area) shape distributions for a set of MC groups
(default: MC_mu/SemiLeptonic vs MC_mu/QCD) for each variable configured in
config.yaml's ABCDVariables block. This stage has no SF weights yet (those
are added in 003-ObjectSelectionII), so each dataset is only corrected for
sign(LHEWeight_originalXWGTUP) -- there's no genWeight branch in these
skims; this matches 003-ObjectSelectionII's LHEWeightSignProducer -- and
scaled by Lumi*Xsec/Ngen when combining datasets within a group, the same
per-dataset scalar-weight convention used when merging histograms across a
group's datasets in 003-ObjectSelectionIII's --aggregrateGroupHists
(Lumi*Xsec/Ngen per dataset, summed into a group total). This is a
shape-comparison tool only: it does not attempt an actual ABCD yield
estimate.

Input is a selectionI_{tag}_{era}_datasets.json (from --generateDatasetJSON),
keyed {DataMC: {group: {dataset: {filepath: "Events"}}}}.

Usage:
    python3 plotABCDVariables.py --datasetJSON <selectionI_{tag}_{era}_datasets.json> \\
        --config <config.yaml> --era <era> --outputDir <dir> \\
        [--dataMC MC_mu] [--groups SemiLeptonic QCD]
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

_COLORS = [ROOT.kBlue + 1, ROOT.kRed + 1, ROOT.kGreen + 2, ROOT.kMagenta + 1, ROOT.kOrange + 1]


def style_canvas():
    ROOT.gStyle.SetOptStat(0)
    ROOT.gStyle.SetOptTitle(0)
    ROOT.gStyle.SetPadTickX(1)
    ROOT.gStyle.SetPadTickY(1)


def build_group_histogram(files_by_dataset, variable, bins, lo, hi, ngen_xsec, lumi, hist_name):
    """Lumi*Xsec/Ngen-weighted, sign(LHEWeight_originalXWGTUP)-corrected sum
    histogram for one MC group, combining all datasets within it (e.g. QCD's
    several pT-binned samples). Mirrors 003-ObjectSelectionIII's
    --aggregrateGroupHists Lumi*Xsec/Ngen scalar-per-dataset convention, and
    003-ObjectSelectionII's LHEWeightSignProducer for the sign correction
    (there's no genWeight branch in these skims -- see that module's README
    entry -- and this stage runs before LHEWeightSignProducer's own
    lheWeightSign output branch exists, so the sign is computed inline here
    the same way: sign of LHEWeight_originalXWGTUP, defaulting to +1 if the
    branch is absent for a given dataset).
    """
    total = None
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

        rdf = ROOT.RDataFrame("Events", filepaths)
        available = {str(c) for c in rdf.GetColumnNames()}
        if variable not in available:
            print(f"    [WARN] Variable '{variable}' not found for dataset '{dataset}'; skipping.")
            continue

        if "LHEWeight_originalXWGTUP" in available:
            sign_expr = "(LHEWeight_originalXWGTUP > 0) ? 1.0 : ((LHEWeight_originalXWGTUP < 0) ? -1.0 : 0.0)"
        else:
            print(f"    [WARN] 'LHEWeight_originalXWGTUP' not found for dataset '{dataset}'; assuming sign +1.")
            sign_expr = "1.0"
        rdf = rdf.Define("_abcd_w", f"({sign_expr}) * {weight}")
        model = ROOT.RDF.TH1DModel(f"{hist_name}_{dataset}", "", bins, lo, hi)
        h = rdf.Histo1D(model, variable, "_abcd_w").GetValue()
        h.SetDirectory(0)

        if total is None:
            total = h
            total.SetName(hist_name)
        else:
            total.Add(h)
        print(f"    Added dataset '{dataset}' ({len(filepaths)} files, weight={weight:.6g})")
    return total


def main():
    parser = argparse.ArgumentParser(
        description="Plot ABCD-plane variable shape distributions from selectionI skims.")
    parser.add_argument("--datasetJSON", required=True, help="Path to selectionI_{tag}_{era}_datasets.json.")
    parser.add_argument("--config", required=True, help="Path to config.yaml.")
    parser.add_argument("--era", required=True, help="Era whose DataLumiInfo/NgenandXsec to use (e.g. UL2018).")
    parser.add_argument("--outputDir", required=True, help="Directory to write PNG/PDF/ROOT output.")
    parser.add_argument("--dataMC", default="MC_mu", help="DataMC key to plot from (default: MC_mu).")
    parser.add_argument("--groups", nargs="+", default=["SemiLeptonic", "QCD"],
                        help="Groups to overlay (default: SemiLeptonic QCD).")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    abcd_vars = config.get("ABCDVariables")
    if not abcd_vars:
        print("ERROR: config.yaml has no ABCDVariables section -- nothing to plot.", file=sys.stderr)
        sys.exit(1)

    lumi = config["DataLumiInfo"][args.era]["Lumi"]
    ngen_xsec_era = config["NgenandXsec"].get(args.era, {}).get(args.dataMC, {})

    with open(args.datasetJSON) as f:
        dataset_data = json.load(f)

    if args.dataMC not in dataset_data:
        print(f"ERROR: '{args.dataMC}' not found in {args.datasetJSON}.", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.outputDir)
    out_dir.mkdir(parents=True, exist_ok=True)
    root_file = ROOT.TFile(str(out_dir / f"abcdVariables_{args.era}.root"), "RECREATE")

    style_canvas()
    canvas = ROOT.TCanvas("c_abcd", "c_abcd", 800, 700)

    any_plotted = False
    for var_key, var_cfg in abcd_vars.items():
        variable = var_cfg["variable"]
        bins = var_cfg["bins"]
        lo, hi = var_cfg["range"]
        label = var_cfg.get("label", variable)
        print(f"\n=== {args.era} / {variable} ===")

        group_hists = {}
        for group in args.groups:
            if group not in dataset_data[args.dataMC]:
                print(f"  [WARN] Group '{group}' not found under {args.dataMC} in dataset JSON; skipping.")
                continue
            print(f"  Group: {group}")
            ngen_xsec_group = ngen_xsec_era.get(group, {})
            h = build_group_histogram(
                dataset_data[args.dataMC][group], variable, bins, lo, hi,
                ngen_xsec_group, lumi, hist_name=f"h_{var_key}_{group}",
            )
            if h is None or h.Integral() == 0:
                print(f"  [WARN] Empty histogram for group '{group}'; skipping.")
                continue
            group_hists[group] = h

        if len(group_hists) < 2:
            print(f"  [WARN] Fewer than 2 non-empty group histograms for '{variable}'; skipping overlay plot.")
            continue

        # Write the raw Lumi*Xsec/Ngen-weighted histograms for later reuse.
        root_file.cd()
        for group, h in group_hists.items():
            h.Write(f"h_{var_key}_{group}_weighted")

        # Overlay normalized (unit-area) shapes.
        canvas.Clear()
        canvas.cd()
        canvas.SetLogy(0)
        legend = ROOT.TLegend(0.62, 0.68, 0.90, 0.88)
        legend.SetBorderSize(0)
        legend.SetFillStyle(0)

        drawn = []
        y_max = 0.0
        for color, (group, h) in zip(_COLORS, group_hists.items()):
            h_norm = h.Clone(f"h_{var_key}_{group}_norm")
            if h_norm.Integral() != 0:
                h_norm.Scale(1.0 / h_norm.Integral())
            h_norm.SetDirectory(0)
            h_norm.SetLineColor(color)
            h_norm.SetLineWidth(2)
            h_norm.GetXaxis().SetTitle(label)
            h_norm.GetYaxis().SetTitle("Normalized to unit area")
            y_max = max(y_max, h_norm.GetMaximum())
            drawn.append((group, h_norm))
            root_file.cd()
            h_norm.Write(f"h_{var_key}_{group}_norm")

        for i, (group, h_norm) in enumerate(drawn):
            h_norm.SetMaximum(y_max * 1.4)
            h_norm.Draw("HIST" if i == 0 else "HIST SAME")
            legend.AddEntry(h_norm, group, "l")

        latex = ROOT.TLatex()
        latex.SetNDC()
        latex.SetTextSize(0.035)
        latex.DrawLatex(0.14, 0.92, f"{args.era}  ({args.dataMC})")
        legend.Draw()
        canvas.Update()

        canvas.SaveAs(str(out_dir / f"{var_key}_{args.era}.png"))
        canvas.SaveAs(str(out_dir / f"{var_key}_{args.era}.pdf"))
        print(f"  Saved {var_key}_{args.era}.png/.pdf to {out_dir}")
        any_plotted = True

    root_file.Close()
    print(f"\nDone. ROOT file with histograms: {out_dir / f'abcdVariables_{args.era}.root'}")

    if not any_plotted:
        print("ERROR: no overlay plots were produced.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

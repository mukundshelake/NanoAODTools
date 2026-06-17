#!/usr/bin/env python3
"""
Create CMS-style Data/MC stacked comparison plots from aggregated .coffea histograms.

Usage:
    python scripts/rootHists.py \\
        --tag earlyApril \\
        --hash 0e454e8b0284 \\
        --outputDir /path/to/plots \\
        [--configFile ../config.yaml] \\
        [--filter UL2018 [UL2017/MC_mu/SingleTop ...]]

Outputs per era (inside --outputDir/{era}/):
  - {histName}.png
  - {histName}.pdf
  - rootHists.root  (all TH1F objects)
"""

import argparse
import array
import math
import sys
from pathlib import Path

import yaml
import ROOT
from ROOT import (
    TFile, TCanvas, TH1F, THStack, TLegend, TLatex, TLine,
    gROOT, gStyle, TPad
)
from coffea.util import load


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config(path: Path) -> dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def matches_filter(filters, era, data_mc=None, group=None):
    """Mirror of the matches_filter logic in run_all.py."""
    if not filters:
        return True
    for f in filters:
        parts = f.split('/')
        if parts[0] not in ('*', era):
            continue
        if data_mc is not None and len(parts) >= 2 and parts[1] not in ('*', data_mc):
            continue
        if group is not None and len(parts) >= 3 and parts[2] not in ('*', group):
            continue
        return True
    return False


def bh_to_th1(bh_hist, name: str, title: str) -> TH1F:
    """Convert a boost_histogram object to a ROOT TH1F.

    Works for variable-width bins (uses TH1F with explicit bin edges).
    Transfers bin values and sqrt(variance) as errors.
    """
    edges = list(bh_hist.axes[0].edges)
    n_bins = len(edges) - 1
    arr_edges = array.array('d', edges)
    h = TH1F(name, title, n_bins, arr_edges)
    h.Sumw2()
    values = bh_hist.values()
    variances = bh_hist.variances()  # None if histogram has no variance storage
    for i in range(n_bins):
        h.SetBinContent(i + 1, float(values[i]))
        if variances is not None:
            err = math.sqrt(float(variances[i])) if variances[i] >= 0 else 0.0
        else:
            err = math.sqrt(float(values[i])) if values[i] >= 0 else 0.0
        h.SetBinError(i + 1, err)
    return h


def get_aggregated_coffea_path(output_dir: Path, tag: str, era: str,
                               data_mc: str, group: str) -> Path:
    return output_dir / era / data_mc / group / f"{tag}_{era}_{data_mc}_{group}_selectionHists.coffea"


def make_mc_total(mc_hists: list) -> TH1F:
    """Sum a list of TH1F objects into a single total-MC histogram."""
    total = mc_hists[0].Clone("mc_total")
    total.Sumw2()
    for h in mc_hists[1:]:
        total.Add(h)
    return total


def style_canvas():
    gStyle.SetOptStat(0)
    gStyle.SetOptTitle(0)
    gStyle.SetFrameLineWidth(1)
    gStyle.SetPadTickX(1)
    gStyle.SetPadTickY(1)


# ---------------------------------------------------------------------------
# Main per-histogram plotting function
# ---------------------------------------------------------------------------

def make_plot(canvas: TCanvas, h_data: TH1F, mc_stack: THStack,
              h_mc_total: TH1F, hist_cfg: dict, era: str, lumi: float,
              channel: str = "#mu + jets"):
    """Draw Data/MC comparison with ratio panel on an existing TCanvas."""
    canvas.Clear()
    canvas.cd()

    # Upper pad
    pad_top = TPad("pad_top", "", 0.0, 0.28, 1.0, 1.0)
    pad_top.SetBottomMargin(0.02)
    pad_top.SetTopMargin(0.08)
    pad_top.SetLeftMargin(0.12)
    pad_top.SetRightMargin(0.05)
    pad_top.SetLogy(1)
    pad_top.Draw()

    # Lower pad (ratio)
    pad_bot = TPad("pad_bot", "", 0.0, 0.0, 1.0, 0.28)
    pad_bot.SetTopMargin(0.03)
    pad_bot.SetBottomMargin(0.35)
    pad_bot.SetLeftMargin(0.12)
    pad_bot.SetRightMargin(0.05)
    pad_bot.Draw()

    # ---- Upper pad --------------------------------------------------------
    pad_top.cd()

    # Stack
    mc_stack.Draw("HIST")
    mc_stack.GetXaxis().SetLabelSize(0)
    mc_stack.GetYaxis().SetTitle("Events")
    mc_stack.GetYaxis().SetTitleSize(0.06)
    mc_stack.GetYaxis().SetTitleOffset(0.95)
    mc_stack.GetYaxis().SetLabelSize(0.05)

    # Auto y-range: max of data or stack, with generous headroom
    y_max = max(h_data.GetMaximum(), mc_stack.GetMaximum()) * 10
    y_min = 0.5
    mc_stack.SetMinimum(y_min)
    mc_stack.SetMaximum(y_max)

    # Data
    h_data.SetMarkerStyle(20)
    h_data.SetMarkerSize(0.8)
    h_data.SetMarkerColor(ROOT.kBlack)
    h_data.SetLineColor(ROOT.kBlack)
    h_data.Draw("E SAME")

    # Legend
    legend = TLegend(0.62, 0.45, 0.93, 0.90)
    legend.SetBorderSize(0)
    legend.SetFillStyle(0)
    legend.SetTextSize(0.042)
    legend.AddEntry(h_data, "Data", "lep")
    # Add MC entries in reverse stack order (top-most first in legend)
    mc_hists_for_legend = []
    stack_hists = mc_stack.GetHists()
    for obj in stack_hists:
        mc_hists_for_legend.append(obj)
    for h in reversed(mc_hists_for_legend):
        legend.AddEntry(h, h.GetTitle(), "f")
    legend.Draw()

    # CMS labels
    latex = TLatex()
    latex.SetNDC()
    latex.SetTextFont(62)
    latex.SetTextSize(0.065)
    latex.DrawLatex(0.14, 0.87, "CMS")
    latex.SetTextFont(52)
    latex.SetTextSize(0.050)
    latex.DrawLatex(0.27, 0.87, "Preliminary")
    latex.SetTextFont(42)
    latex.SetTextSize(0.048)
    latex.DrawLatex(0.14, 0.80, channel)
    # Luminosity (top-right)
    latex.SetTextAlign(31)
    lumi_str = f"{lumi / 1000.0:.1f} fb^{{-1}} (13 TeV, {era})"
    latex.DrawLatex(0.94, 0.935, lumi_str)
    latex.SetTextAlign(11)  # reset

    # ---- Lower pad (ratio) -----------------------------------------------
    pad_bot.cd()

    # Ratio = Data / MC total
    h_ratio = h_data.Clone("ratio")
    h_ratio.Divide(h_mc_total)
    h_ratio.SetMarkerStyle(20)
    h_ratio.SetMarkerSize(0.8)
    h_ratio.SetMarkerColor(ROOT.kBlack)
    h_ratio.SetLineColor(ROOT.kBlack)

    # MC stat uncertainty band (centered at 1)
    h_unc_band = h_mc_total.Clone("unc_band")
    for i in range(1, h_mc_total.GetNbinsX() + 1):
        mc_val = h_mc_total.GetBinContent(i)
        mc_err = h_mc_total.GetBinError(i)
        if mc_val > 0:
            h_unc_band.SetBinContent(i, 1.0)
            h_unc_band.SetBinError(i, mc_err / mc_val)
        else:
            h_unc_band.SetBinContent(i, 1.0)
            h_unc_band.SetBinError(i, 0.0)
    h_unc_band.SetFillColorAlpha(ROOT.kGray + 1, 0.40)
    h_unc_band.SetFillStyle(1001)
    h_unc_band.SetLineColor(ROOT.kGray + 1)
    h_unc_band.SetMarkerSize(0)

    x_label = hist_cfg.get('label', '')
    h_ratio.GetXaxis().SetTitle(x_label)
    h_ratio.GetXaxis().SetTitleSize(0.14)
    h_ratio.GetXaxis().SetTitleOffset(0.95)
    h_ratio.GetXaxis().SetLabelSize(0.12)
    h_ratio.GetYaxis().SetTitle("Data / MC")
    h_ratio.GetYaxis().SetTitleSize(0.13)
    h_ratio.GetYaxis().SetTitleOffset(0.40)
    h_ratio.GetYaxis().SetLabelSize(0.11)
    h_ratio.GetYaxis().SetNdivisions(505)
    h_ratio.SetMinimum(0.5)
    h_ratio.SetMaximum(1.5)

    h_ratio.Draw("EP")
    h_unc_band.Draw("E2 SAME")
    h_ratio.Draw("EP SAME")   # redraw on top of band

    # Reference line at 1
    x_lo = h_ratio.GetXaxis().GetXmin()
    x_hi = h_ratio.GetXaxis().GetXmax()
    ref_line = TLine(x_lo, 1.0, x_hi, 1.0)
    ref_line.SetLineColor(ROOT.kGray + 2)
    ref_line.SetLineStyle(2)
    ref_line.SetLineWidth(1)
    ref_line.Draw()

    canvas.Update()

    # Return objects that must stay alive until canvas is saved
    return pad_top, pad_bot, legend, latex, h_ratio, h_unc_band, ref_line


# ---------------------------------------------------------------------------
# Core loop
# ---------------------------------------------------------------------------

def process_era(era: str, config: dict, output_dir: Path, tag: str, args):
    """Process one era: build all histograms and save PNG/PDF/ROOT."""
    plot_cfg = config.get('plotSettings', {})
    mc_mu_plot = plot_cfg.get('MC_mu', {})
    hist_details = config['histDetails']
    lumi = config['DataLumiInfo'][era]['Lumi']

    era_out = output_dir / era
    era_out.mkdir(parents=True, exist_ok=True)

    root_file = TFile(str(era_out / "rootHists.root"), "RECREATE")

    canvas = TCanvas("c1", "c1", 800, 800)
    style_canvas()

    # Load data coffea
    data_key = f"{era}_Data_mu_SingleMuon"
    data_coffea_path = get_aggregated_coffea_path(args.input_base, tag, era, "Data_mu", "SingleMuon")
    if not data_coffea_path.exists():
        print(f"  [WARN] Data coffea not found for {era}: {data_coffea_path}. Skipping era.")
        root_file.Close()
        return
    data_coffea = load(data_coffea_path)

    # Load MC coffea files — only groups present in NgenandXsec[era]['MC_mu']
    mc_groups_config = config['NgenandXsec'].get(era, {}).get('MC_mu', {})
    mc_coffea = {}  # group → coffea dict
    for group in mc_groups_config:
        if not matches_filter(args.filter, era, 'MC_mu', group):
            continue
        mc_coffea_path = get_aggregated_coffea_path(args.input_base, tag, era, "MC_mu", group)
        if not mc_coffea_path.exists():
            print(f"  [WARN] MC coffea not found: {mc_coffea_path}. Skipping group {group}.")
            continue
        mc_coffea[group] = load(mc_coffea_path)

    if not mc_coffea:
        print(f"  [WARN] No MC coffea loaded for {era}. Skipping era.")
        root_file.Close()
        return

    # Sort MC groups by stackOrder (lowest first = bottom of stack)
    def stack_order(g):
        return mc_mu_plot.get(g, {}).get('stackOrder', 99)
    sorted_groups = sorted(mc_coffea.keys(), key=stack_order)

    # Loop over histograms
    for hist_name, hist_cfg in hist_details.items():
        print(f"  Plotting {era} / {hist_name} ...")

        # --- Data histogram ---
        bh_data = data_coffea[data_key].get(hist_name)
        if bh_data is None:
            print(f"    [WARN] '{hist_name}' missing in data coffea for {era}. Skipping.")
            continue
        h_data = bh_to_th1(bh_data, f"h_{hist_name}_Data", "Data")
        h_data.SetDirectory(0)

        # --- MC histograms ---
        mc_stack = THStack(f"hs_{hist_name}", "")
        mc_hists = []
        for group in sorted_groups:
            key = f"{era}_MC_mu_{group}"
            bh_mc = mc_coffea[group].get(key, {}).get(hist_name)
            if bh_mc is None:
                print(f"    [WARN] '{hist_name}' missing in MC coffea for {era}/{group}. Skipping group.")
                continue
            grp_label = mc_mu_plot.get(group, {}).get('label', group)
            grp_color = mc_mu_plot.get(group, {}).get('color', 1)
            h_mc = bh_to_th1(bh_mc, f"h_{hist_name}_{group}", grp_label)
            h_mc.SetFillColor(grp_color)
            h_mc.SetLineColor(ROOT.kBlack)
            h_mc.SetLineWidth(1)
            h_mc.SetDirectory(0)
            mc_stack.Add(h_mc)
            mc_hists.append(h_mc)

        if not mc_hists:
            print(f"    [WARN] No MC histograms built for {era}/{hist_name}. Skipping.")
            continue

        h_mc_total = make_mc_total(mc_hists)
        h_mc_total.SetDirectory(0)

        # Draw
        kept_refs = make_plot(canvas, h_data, mc_stack, h_mc_total,
                              hist_cfg, era, lumi)

        # Save images
        canvas.SaveAs(str(era_out / f"{hist_name}.png"))
        canvas.SaveAs(str(era_out / f"{hist_name}.pdf"))
        canvas.SaveAs(str(era_out / f"{hist_name}.C"))

        # Write TH1F objects to ROOT file
        root_file.cd()
        h_data.Write(f"h_{hist_name}_Data")
        for h_mc in mc_hists:
            h_mc.Write(h_mc.GetName())
        h_mc_total.Write(f"h_{hist_name}_MCTotal")

        del kept_refs  # allow ROOT to garbage-collect pad objects

    root_file.Close()
    print(f"  Saved plots and ROOT file to {era_out}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    gROOT.SetBatch(True)

    parser = argparse.ArgumentParser(
        description="Create CMS-style Data/MC ROOT histograms from aggregated coffea files.")
    parser.add_argument('--tag', type=str, required=True,
                        help='Output tag used in run_all.py (e.g., earlyApril)')
    parser.add_argument('--hash', type=str, required=True,
                        help='Config hash directory (e.g., 0e454e8b0284)')
    parser.add_argument('--configFile', type=str,
                        default=str(Path(__file__).parent.parent / 'config.yaml'),
                        help='Path to config.yaml (default: ../config.yaml relative to this script)')
    parser.add_argument('--outputDir', type=str, default=None,
                        help='Directory where per-era plot folders will be created. '
                             'Defaults to outputs/{tag}/{hash}/plots/ relative to the script.')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                        help='Filter by era[/DataMC[/group]]. '
                             'Multiple filters are OR-ed. E.g.: --filter UL2018 UL2017/MC_mu/SingleTop')
    args = parser.parse_args()

    config = load_config(Path(args.configFile))

    # Base directory: outputs/{tag}/{hash}/
    base_dir = Path(__file__).parent.parent
    args.input_base = base_dir / 'outputs' / args.tag / args.hash
    if not args.input_base.exists():
        print(f"Error: Input directory does not exist: {args.input_base}", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.outputDir) if args.outputDir else args.input_base / 'plots'
    output_dir.mkdir(parents=True, exist_ok=True)

    eras = list(config['NgenandXsec'].keys())
    for era in eras:
        if not matches_filter(args.filter, era):
            continue
        print(f"\nProcessing era: {era}")
        process_era(era, config, output_dir, args.tag, args)

    print("\nDone.")


if __name__ == '__main__':
    main()




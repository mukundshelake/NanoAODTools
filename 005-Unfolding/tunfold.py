"""
TUnfold-based unfolding for ttbar charge asymmetry.

Adapted from:
  https://github.com/Sanskar-hep/TOP-ANALYSIS-FRAMEWORK/blob/main/UNFOLDING/TUNFOLD_scripts/tunfold.py

Inputs:
  - unrolled_histograms.root : h_reco (pseudo-data) and h_gen (truth)
                               produced by make_histograms.py
  - response_matrix.root     : response matrix + systematic variations
                               produced by response_matrix.py

Usage:
    python tunfold.py \\
        --histograms unrolled_histograms.root \\
        --matrix     response_matrix.root \\
        --outdir     results/
"""

import argparse
import ctypes
import os

import numpy as np
import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(0)
ROOT.gStyle.SetOptFit(0)
ROOT.gSystem.Load("libUnfold")

RECO_MTT_EDGES = np.array(
    [300, 375, 450, 525, 600, 675, 750, 825, 900, 975, 1050, 1125, 1200], dtype=float
)
GEN_MTT_EDGES = np.array([300, 450, 600, 750, 900, 1050, 1200], dtype=float)

n_reco_mtt  = len(RECO_MTT_EDGES) - 1
n_gen_mtt   = len(GEN_MTT_EDGES)  - 1
n_reco_bins = 2 * n_reco_mtt
n_gen_bins  = 2 * n_gen_mtt

# Match names used in response_matrix.py SYST_PAIRS
SYSTEMATICS = ["pileup", "prefiring", "muonID", "btag"]


def main():
    parser = argparse.ArgumentParser(description="TUnfold unfolding for ttbar charge asymmetry")
    parser.add_argument("--histograms", required=True,
                        help="ROOT file from make_histograms.py")
    parser.add_argument("--matrix",     required=True,
                        help="ROOT file from response_matrix.py")
    parser.add_argument("--outdir",     default="results_unfolding",
                        help="Output directory for plots and ROOT file")
    parser.add_argument("--era",        default="UL2016preVFP",
                        help="Era label for plot titles")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    out_root = os.path.join(args.outdir, "unfolding_results.root")

    # --- Load inputs ---
    f_proc = ROOT.TFile.Open(args.histograms)
    f_resp = ROOT.TFile.Open(args.matrix)

    h_data   = f_proc.Get("ttbar_SemiLeptonic_reco_nominal")
    h_truth  = f_proc.Get("ttbar_SemiLeptonic_gen_nominal")
    h_matrix = f_resp.Get("response_matrix_nominal")

    for obj, name in [(h_data, "ttbar_SemiLeptonic_reco_nominal"),
                      (h_truth, "ttbar_SemiLeptonic_gen_nominal"),
                      (h_matrix, "response_matrix_nominal")]:
        if not obj:
            raise RuntimeError(f"Object '{name}' not found in input files")

    h_data.SetDirectory(0)
    h_truth.SetDirectory(0)
    h_matrix.SetDirectory(0)

    # --- TUnfoldDensity setup ---
    unfold = ROOT.TUnfoldDensity(
        h_matrix,
        ROOT.TUnfold.kHistMapOutputHoriz,
        ROOT.TUnfold.kRegModeCurvature,
        ROOT.TUnfold.kEConstraintArea,
        ROOT.TUnfoldDensity.kDensityModeBinWidth,
    )

    status = unfold.SetInput(h_data)
    print(f"SetInput status: {status}")
    if status >= 10000:
        raise RuntimeError(
            f"SetInput failed (status={status}) — "
            "check that h_data binning matches response matrix Y-axis"
        )

    # --- Add systematics ---
    print("\nAdding systematics:")
    for sys_name in SYSTEMATICS:
        h_up   = f_resp.Get(f"response_matrix_{sys_name}Up")
        h_down = f_resp.Get(f"response_matrix_{sys_name}Down")
        if not h_up or not h_down:
            print(f"  [SKIP] {sys_name} — histograms not found in matrix file")
            continue
        h_up.SetDirectory(0)
        h_down.SetDirectory(0)

        h_shift = h_up.Clone(f"h_sys_shift_{sys_name}")
        h_shift.SetDirectory(0)
        h_shift.Add(h_down, -1.0)
        h_shift.Scale(0.5)

        unfold.AddSysError(
            h_shift,
            sys_name,
            ROOT.TUnfold.kHistMapOutputHoriz,
            ROOT.TUnfoldSys.kSysErrModeShift,
        )
        print(f"  [OK] {sys_name}")

    # --- L-curve scan ---
    print("\nScanning L-curve...")
    l_curve   = ROOT.TGraph()
    log_tau_x = ROOT.TSpline3()
    log_tau_y = ROOT.TSpline3()

    i_best = unfold.ScanLcurve(100, 0.0, 0.0, l_curve, log_tau_x, log_tau_y)
    tau_best = unfold.GetTau()
    print(f"  Best scan point : {i_best}")
    print(f"  Optimal tau     : {tau_best:.8f}")
    print(f"  Chi2 (L)        : {unfold.GetChi2L():.4f}")
    print(f"  Chi2 (A)        : {unfold.GetChi2A():.4f}")
    print(f"  Rho avg         : {unfold.GetRhoAvg():.4f}")

    # --- Get unfolded result and covariance ---
    h_unfolded = unfold.GetOutput("h_unfolded")
    h_unfolded.SetDirectory(0)
    for i in range(1, n_gen_bins + 1):
        h_unfolded.GetXaxis().SetBinLabel(i, h_truth.GetXaxis().GetBinLabel(i))

    h_cov = unfold.GetEmatrixTotal("h_cov_total")
    h_cov.SetDirectory(0)

    # --- Print unfolded vs truth ---
    print(f"\n{'='*65}")
    print("Unfolded vs Truth per gen bin:")
    print(f"  {'Bin':<5} {'Label':<28} {'Unfolded':>10} {'Err':>10} {'Truth':>10} {'Pull':>8}")
    print(f"  {'-'*65}")
    for i in range(n_gen_bins):
        unf   = h_unfolded.GetBinContent(i + 1)
        err   = np.sqrt(max(0.0, h_cov.GetBinContent(i + 1, i + 1)))
        truth = h_truth.GetBinContent(i + 1)
        pull  = (unf - truth) / err if err > 0 else 0.0
        lbl   = h_unfolded.GetXaxis().GetBinLabel(i + 1)
        print(f"  {i+1:<5} {lbl:<28} {unf:>10.2f} {err:>10.2f} {truth:>10.2f} {pull:>8.3f}")

    # --- Plot: truth vs unfolded ---
    c_comp = ROOT.TCanvas("c_comp", "Truth vs Unfolded", 900, 700)
    pad1 = ROOT.TPad("pad1", "", 0.0, 0.0, 1.0, 1.0)
    pad1.SetLeftMargin(0.14)
    pad1.SetRightMargin(0.05)
    pad1.SetTopMargin(0.12)
    pad1.SetBottomMargin(0.25)
    pad1.Draw()
    pad1.cd()

    h_truth_draw    = h_truth.Clone("h_truth_draw")
    h_unfolded_draw = h_unfolded.Clone("h_unfolded_draw")
    ymax = max(h_truth_draw.GetMaximum(), h_unfolded_draw.GetMaximum()) * 1.35

    h_truth_draw.SetLineColor(ROOT.kBlue + 1)
    h_truth_draw.SetLineWidth(2)
    h_truth_draw.SetFillColorAlpha(ROOT.kBlue + 1, 0.20)
    h_truth_draw.GetYaxis().SetTitle("Events")
    h_truth_draw.GetYaxis().SetTitleSize(0.055)
    h_truth_draw.GetYaxis().SetTitleOffset(1.1)
    h_truth_draw.GetYaxis().SetLabelSize(0.045)
    h_truth_draw.GetXaxis().SetLabelSize(0.045)
    h_truth_draw.GetXaxis().LabelsOption("v")
    h_truth_draw.GetXaxis().SetLabelOffset(0.01)
    h_truth_draw.SetMaximum(ymax)
    h_truth_draw.SetMinimum(0)
    h_truth_draw.Draw("HIST")

    h_unfolded_draw.SetMarkerStyle(20)
    h_unfolded_draw.SetMarkerSize(1.3)
    h_unfolded_draw.SetMarkerColor(ROOT.kBlack)
    h_unfolded_draw.SetLineColor(ROOT.kBlack)
    h_unfolded_draw.SetLineWidth(2)
    h_unfolded_draw.Draw("E1 SAME")

    line = ROOT.TLine(n_gen_mtt, 0, n_gen_mtt, ymax)
    line.SetLineColor(ROOT.kRed)
    line.SetLineStyle(2)
    line.SetLineWidth(2)
    line.Draw()

    leg = ROOT.TLegend(0.62, 0.68, 0.92, 0.86)
    leg.SetBorderSize(0)
    leg.SetTextSize(0.045)
    leg.AddEntry(h_truth_draw,    "Gen-level truth", "lf")
    leg.AddEntry(h_unfolded_draw, "Unfolded",        "lep")
    leg.Draw()

    lat = ROOT.TLatex()
    lat.SetNDC(True)
    lat.SetTextSize(0.05)
    lat.SetTextFont(62)
    lat.DrawLatex(0.16, 0.91, f"Closure test  {args.era}")
    lat.SetTextFont(42)
    lat.SetTextSize(0.04)
    lat.DrawLatex(0.16, 0.84, f"#tau = {tau_best:.2e}")

    c_comp.cd()
    c_comp.SaveAs(os.path.join(args.outdir, "truth_vs_unfolded.pdf"))
    c_comp.SaveAs(os.path.join(args.outdir, "truth_vs_unfolded.png"))
    print(f"\nSaved: truth_vs_unfolded.pdf / .png")

    # --- Plot: L-curve ---
    c_lcurve = ROOT.TCanvas("c_lcurve", "L-curve", 700, 600)
    c_lcurve.SetLeftMargin(0.14)
    c_lcurve.SetRightMargin(0.05)
    c_lcurve.SetTopMargin(0.10)
    c_lcurve.SetBottomMargin(0.14)

    l_curve.SetTitle("L-curve;log_{10}(#chi^{2}_{L});log_{10}(Regularisation)")
    l_curve.SetMarkerStyle(20)
    l_curve.SetMarkerSize(0.5)
    l_curve.SetMarkerColor(ROOT.kBlue + 1)
    l_curve.SetLineColor(ROOT.kBlue + 1)
    l_curve.Draw("AL")

    x_best = ctypes.c_double(0.0)
    y_best = ctypes.c_double(0.0)
    l_curve.GetPoint(i_best, x_best, y_best)

    best_pt = ROOT.TGraph(1)
    best_pt.SetPoint(0, x_best.value, y_best.value)
    best_pt.SetMarkerStyle(29)
    best_pt.SetMarkerSize(2.5)
    best_pt.SetMarkerColor(ROOT.kRed)
    best_pt.Draw("P SAME")

    lat2 = ROOT.TLatex()
    lat2.SetNDC(True)
    lat2.SetTextSize(0.04)
    lat2.DrawLatex(0.16, 0.92, f"Optimal #tau = {tau_best:.2e}")

    c_lcurve.SaveAs(os.path.join(args.outdir, "lcurve.pdf"))
    c_lcurve.SaveAs(os.path.join(args.outdir, "lcurve.png"))
    print(f"Saved: lcurve.pdf / .png")

    # --- Save ROOT file ---
    fout = ROOT.TFile(out_root, "RECREATE")
    h_unfolded.Write("h_unfolded")
    h_truth.Write("h_gen_truth")
    h_data.Write("h_reco_measured")
    h_cov.Write("h_cov_total")
    l_curve.Write("lcurve")
    c_comp.Write("canvas_truth_vs_unfolded")
    c_lcurve.Write("canvas_lcurve")
    fout.Close()

    f_proc.Close()
    f_resp.Close()

    print(f"\n{'='*55}")
    print(f"Done. Results written to: {out_root}")


if __name__ == "__main__":
    main()

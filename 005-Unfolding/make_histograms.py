"""
Build unrolled reco and gen TH1D histograms from the signal parquet (closure test).

Usage:
    python make_histograms.py signal.parquet output.root \\
        --xsec 365.5 --ngen 110787582 --lumi 19520.0
"""

import argparse
import os

import numpy as np
import pandas as pd
import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(0)

Y_0 = 1.2

RECO_MTT_EDGES = np.array(
    [300, 375, 450, 525, 600, 675, 750, 825, 900, 975, 1050, 1125, 1200], dtype=float
)
GEN_MTT_EDGES = np.array([300, 450, 600, 750, 900, 1050, 1200], dtype=float)


def make_Nplus_Nminus(y_top, y_antitop):
    top_fwd  = np.abs(y_top)  > Y_0
    atop_fwd = np.abs(y_antitop) > Y_0
    return top_fwd & ~atop_fwd, atop_fwd & ~top_fwd


def get_unrolled_bin(mtt, is_Nplus, is_Nminus, edges):
    n = len(edges) - 1
    result = np.full(len(mtt), -1, dtype=int)
    for i in range(n):
        in_bin = (mtt >= edges[i]) & (mtt < edges[i + 1])
        result[in_bin & is_Nplus]  = i
        result[in_bin & is_Nminus] = n + i
    return result


def make_labels(edges):
    labels = []
    n = len(edges) - 1
    for sign in ["N_{+}", "N_{-}"]:
        for i in range(n):
            labels.append(f"{sign}({int(edges[i])}-{int(edges[i+1])})")
    return labels


def make_th1(name, title, n_bins, edges):
    h = ROOT.TH1D(name, title, n_bins, 0, n_bins)
    h.Sumw2()
    for i, label in enumerate(make_labels(edges)):
        h.GetXaxis().SetBinLabel(i + 1, label)
    h.GetXaxis().SetLabelSize(0.04)
    h.GetXaxis().LabelsOption("v")
    return h


def main():
    parser = argparse.ArgumentParser(description="Build unrolled reco/gen histograms from signal parquet")
    parser.add_argument("parquet", help="Signal parquet produced by getParquet.py --signal")
    parser.add_argument("output",  help="Output ROOT file")
    parser.add_argument("--xsec",  type=float, required=True, help="Cross section in pb")
    parser.add_argument("--ngen",  type=int,   required=True, help="Number of generated events")
    parser.add_argument("--lumi",  type=float, required=True, help="Luminosity in pb-1")
    args = parser.parse_args()

    df = pd.read_parquet(args.parquet)
    lumi_scale = args.xsec * args.lumi / args.ngen
    w = df["weight_nominal"].values * lumi_scale

    # --- Reco ---
    Nplus_reco, Nminus_reco = make_Nplus_Nminus(df["yt_lab"].values, df["ytbar_lab"].values)
    reco_bin = get_unrolled_bin(df["mtt_reco"].values, Nplus_reco, Nminus_reco, RECO_MTT_EDGES)

    n_reco_bins = 2 * (len(RECO_MTT_EDGES) - 1)
    h_reco = make_th1("ttbar_SemiLeptonic_reco_nominal",
                      "Reco unrolled (pseudo-data);Bin;Events",
                      n_reco_bins, RECO_MTT_EDGES)

    mask_reco = reco_bin >= 0
    reco_vals = (reco_bin[mask_reco] + 0.5).astype(np.float64)
    reco_w    = w[mask_reco].astype(np.float64)
    h_reco.FillN(len(reco_vals), reco_vals, reco_w)

    # --- Gen ---
    Nplus_gen, Nminus_gen = make_Nplus_Nminus(df["gen_yt"].values, df["gen_ytbar"].values)
    gen_bin = get_unrolled_bin(df["mtt_gen"].values, Nplus_gen, Nminus_gen, GEN_MTT_EDGES)

    n_gen_bins = 2 * (len(GEN_MTT_EDGES) - 1)
    h_gen = make_th1("ttbar_SemiLeptonic_gen_nominal",
                     "Gen unrolled (truth);Bin;Events",
                     n_gen_bins, GEN_MTT_EDGES)

    mask_gen = gen_bin >= 0
    gen_vals = (gen_bin[mask_gen] + 0.5).astype(np.float64)
    gen_w    = w[mask_gen].astype(np.float64)
    h_gen.FillN(len(gen_vals), gen_vals, gen_w)

    print(f"Reco histogram: {h_reco.GetEntries():.0f} raw entries, {h_reco.Integral():.1f} weighted")
    print(f"Gen  histogram: {h_gen.GetEntries():.0f} raw entries, {h_gen.Integral():.1f} weighted")

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    fout = ROOT.TFile(args.output, "RECREATE")
    h_reco.Write()
    h_gen.Write()
    fout.Close()
    print(f"Wrote: {args.output}")


if __name__ == "__main__":
    main()

"""
Build response matrix for ttbar charge asymmetry unfolding.

Reads from a parquet file produced by:
    python getParquet.py --signal <input_dir> <output.parquet>

Usage:
    python response_matrix.py signal.parquet output.root \\
        --xsec 365.5 --ngen 110787582 --lumi 19520.0
"""

import argparse
import os

import numpy as np
import pandas as pd
import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(0)

Y_0      = 1.2    # rapidity threshold for N+/N- definition
PGOF_CUT = 0.0    # no cut — keep all reconstructed events

RECO_MTT_EDGES = np.array(
    [300, 375, 450, 525, 600, 675, 750, 825, 900, 975, 1050, 1125, 1200], dtype=float
)
GEN_MTT_EDGES = np.array([300, 450, 600, 750, 900, 1050, 1200], dtype=float)

SYST_PAIRS = [
    ("weight_pileupUp",      "weight_pileupDown"),
    ("weight_prefiringUp",   "weight_prefiringDown"),
    ("weight_muonIDUp",      "weight_muonIDDown"),
    ("weight_btagUp",        "weight_btagDown"),
]


def make_Nplus_Nminus(y_top, y_antitop):
    top_fwd  = np.abs(y_top) > Y_0
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


def make_response_matrix(name, title, with_errors, n_gen, n_reco, gen_edges, reco_edges):
    h = ROOT.TH2D(name, title, n_gen, 0, n_gen, n_reco, 0, n_reco)
    if with_errors:
        h.Sumw2()
    for i, label in enumerate(make_labels(gen_edges)):
        h.GetXaxis().SetBinLabel(i + 1, label)
    for i, label in enumerate(make_labels(reco_edges)):
        h.GetYaxis().SetBinLabel(i + 1, label)
    h.SetMinimum(0.0)
    h.SetOption("COLZ0")
    return h


def fill_response_matrix(h, gen_bin, reco_bin, weights):
    # Valid bins → bin centre (+0.5); failed/out-of-range → underflow (-0.5)
    g_fill = np.where(gen_bin >= 0, gen_bin + 0.5, -0.5).astype(np.float64)
    r_fill = np.where(reco_bin >= 0, reco_bin + 0.5, -0.5).astype(np.float64)
    w_fill = weights.astype(np.float64)
    h.FillN(len(g_fill), g_fill, r_fill, w_fill, 1)


def zero_bin_errors(h):
    for gx in range(1, h.GetNbinsX() + 1):
        for ry in range(1, h.GetNbinsY() + 1):
            h.SetBinError(gx, ry, 0.0)


def main():
    parser = argparse.ArgumentParser(description="Build response matrix from signal parquet")
    parser.add_argument("parquet", help="Signal parquet produced by getParquet.py --signal")
    parser.add_argument("output",  help="Output ROOT file")
    parser.add_argument("--xsec",  type=float, required=True, help="Cross section in pb")
    parser.add_argument("--ngen",  type=int,   required=True, help="Number of generated events")
    parser.add_argument("--lumi",  type=float, required=True, help="Luminosity in pb-1")
    args = parser.parse_args()

    df = pd.read_parquet(args.parquet)
    lumi_scale = args.xsec * args.lumi / args.ngen

    # Quality-of-fit selection on reco
    pass_reco = df["Pgof"].values > PGOF_CUT

    # Reco binning using lab-frame rapidities
    Nplus_reco, Nminus_reco = make_Nplus_Nminus(df["yt_lab"].values, df["ytbar_lab"].values)
    reco_bin = get_unrolled_bin(df["mtt_reco"].values, Nplus_reco, Nminus_reco, RECO_MTT_EDGES)
    reco_bin[~pass_reco] = -1  # failed reco → miss

    # Gen binning
    Nplus_gen, Nminus_gen = make_Nplus_Nminus(df["gen_yt"].values, df["gen_ytbar"].values)
    gen_bin = get_unrolled_bin(df["mtt_gen"].values, Nplus_gen, Nminus_gen, GEN_MTT_EDGES)

    # Include every event that contributes to at least one axis
    in_matrix = (reco_bin >= 0) | (gen_bin >= 0)

    print(f"\nEra statistics:")
    print(f"  Total events:                    {len(df)}")
    print(f"  Entering matrix:                 {np.sum(in_matrix)}")
    print(f"  Hits   (gen & reco valid):       {np.sum((gen_bin >= 0) & (reco_bin >= 0))}")
    print(f"  Misses (gen ok, reco fail/out):  {np.sum((gen_bin >= 0) & (reco_bin < 0))}")
    print(f"  Fakes  (reco ok, gen out-of-range): {np.sum((gen_bin < 0) & (reco_bin >= 0))}")
    print()

    n_reco_bins = 2 * (len(RECO_MTT_EDGES) - 1)
    n_gen_bins  = 2 * (len(GEN_MTT_EDGES)  - 1)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    fout = ROOT.TFile(args.output, "RECREATE")

    # Nominal response matrix (with stat errors via Sumw2)
    h_nominal = make_response_matrix(
        "response_matrix_nominal", "Nominal response matrix;Gen bin;Reco bin",
        True, n_gen_bins, n_reco_bins, GEN_MTT_EDGES, RECO_MTT_EDGES,
    )
    w_nominal = df["weight_nominal"].values * lumi_scale
    fill_response_matrix(h_nominal, gen_bin[in_matrix], reco_bin[in_matrix], w_nominal[in_matrix])
    h_nominal.Write()

    # Systematic variations (no stat errors stored — zero them out)
    for up_col, dn_col in SYST_PAIRS:
        source = up_col.replace("weight_", "").replace("Up", "")
        h_up = make_response_matrix(
            f"response_matrix_{source}Up",   f"{source} Up;Gen bin;Reco bin",
            False, n_gen_bins, n_reco_bins, GEN_MTT_EDGES, RECO_MTT_EDGES,
        )
        h_dn = make_response_matrix(
            f"response_matrix_{source}Down", f"{source} Down;Gen bin;Reco bin",
            False, n_gen_bins, n_reco_bins, GEN_MTT_EDGES, RECO_MTT_EDGES,
        )
        w_up = df[up_col].values * lumi_scale
        w_dn = df[dn_col].values * lumi_scale
        fill_response_matrix(h_up, gen_bin[in_matrix], reco_bin[in_matrix], w_up[in_matrix])
        fill_response_matrix(h_dn, gen_bin[in_matrix], reco_bin[in_matrix], w_dn[in_matrix])
        zero_bin_errors(h_up)
        zero_bin_errors(h_dn)
        h_up.Write()
        h_dn.Write()

    fout.Close()
    print(f"Wrote: {args.output}")


if __name__ == "__main__":
    main()

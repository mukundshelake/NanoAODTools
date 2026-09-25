#!/usr/bin/env python3
"""Parquet -> unrolled histograms + response matrix. Replaces make_histograms.py
and response_matrix.py, which are merged here precisely because keeping them
apart let their selections drift (issue #32).

Everything below draws its binning from scripts/binning.py, its selection from
scripts/selection.py and its normalisation from config.yaml. The measured
histogram, the response matrix and every systematic variation are filled from
*the same* mask, computed once.

Usage:
    python scripts/build_inputs.py --era UL2016preVFP --tag midNov
    python scripts/build_inputs.py --era UL2016preVFP --tag midNov \
        --acceptance outputs/gen_acceptance/gen_acceptance_UL2016preVFP.npz

Scope note: this is a faithful port of what the two old scripts did, on the new
shared foundation. The response-matrix bookkeeping problems -- no inefficiency
column, all fakes piled into one gen underflow bin, no m_tt overflow handling,
zero_bin_errors() missing the underflow -- are issue #36 and are marked TODO
below rather than silently half-fixed here.
"""

import argparse
import json
import os
import sys

import numpy as np
import pandas as pd
import ROOT

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import binning  # noqa: E402
import config as cfgmod  # noqa: E402
import selection  # noqa: E402

ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(0)


def make_th1(name, title, edges):
    n = binning.n_unrolled_bins(edges)
    h = ROOT.TH1D(name, title, n, 0, n)
    h.Sumw2()
    for i, label in enumerate(binning.labels(edges)):
        h.GetXaxis().SetBinLabel(i + 1, label)
    h.GetXaxis().SetLabelSize(0.04)
    h.GetXaxis().LabelsOption("v")
    return h


def make_th2(name, title, gen_edges, reco_edges, with_errors):
    n_gen = binning.n_unrolled_bins(gen_edges)
    n_reco = binning.n_unrolled_bins(reco_edges)
    h = ROOT.TH2D(name, title, n_gen, 0, n_gen, n_reco, 0, n_reco)
    if with_errors:
        h.Sumw2()
    for i, label in enumerate(binning.labels(gen_edges)):
        h.GetXaxis().SetBinLabel(i + 1, label)
    for i, label in enumerate(binning.labels(reco_edges)):
        h.GetYaxis().SetBinLabel(i + 1, label)
    h.SetMinimum(0.0)
    h.SetOption("COLZ0")
    return h


def fill_th1(h, bins, weights, label):
    keep = bins >= 0
    values = (bins[keep] + 0.5).astype(np.float64)
    w = weights[keep].astype(np.float64)
    selection.assert_no_nan_reaches_histogram(values, w, label)
    if len(values):
        h.FillN(len(values), values, w)


def fill_th2(h, gen_bins, reco_bins, weights):
    # TODO(#36): unclassified events on either axis are all dumped into that
    # axis' underflow, so m_tt-out-of-range, both-forward and both-central
    # events become one indistinguishable pile that TUnfold then treats as a
    # single extra degree of freedom. Fakes belong in SubtractBackground, and
    # the gen inefficiency needs its own column.
    g = np.where(gen_bins >= 0, gen_bins + 0.5, -0.5).astype(np.float64)
    r = np.where(reco_bins >= 0, reco_bins + 0.5, -0.5).astype(np.float64)
    w = weights.astype(np.float64)
    selection.assert_no_nan_reaches_histogram(w, w, "response matrix")
    h.FillN(len(g), g, r, w, 1)


def zero_bin_errors(h):
    # TODO(#36): this loops 1..GetNbinsX() and so never reaches the underflow
    # bins, which is exactly where the fakes and misses live.
    for gx in range(1, h.GetNbinsX() + 1):
        for ry in range(1, h.GetNbinsY() + 1):
            h.SetBinError(gx, ry, 0.0)


def classify_events(df, cfg, mask):
    """Reco and gen unrolled bin index for every event (UNCLASSIFIED where not)."""
    gen_edges = cfgmod.gen_mtt_edges(cfg)
    reco_edges = cfgmod.reco_mtt_edges(cfg)
    scheme, y0 = cfgmod.scheme(cfg), cfgmod.y0(cfg)

    binning.check_edges_representable(gen_edges, scheme=scheme, y0=y0)
    binning.check_edges_representable(reco_edges, scheme=scheme, y0=y0)

    p_reco, m_reco = binning.classify(df["yt_lab"].values, df["ytbar_lab"].values,
                                      scheme=scheme, y0=y0)
    reco_bin = binning.unrolled_bin(df["mtt_reco"].values, p_reco, m_reco, reco_edges)
    # A reco bin is only meaningful for an event that passed the selection --
    # this is the single place that coupling is expressed (issue #32).
    reco_bin = np.where(mask, reco_bin, binning.UNCLASSIFIED)

    gen_bin = None
    if "gen_yt" in df:
        p_gen, m_gen = binning.classify(df["gen_yt"].values, df["gen_ytbar"].values,
                                        scheme=scheme, y0=y0)
        gen_bin = binning.unrolled_bin(df["mtt_gen"].values, p_gen, m_gen, gen_edges)
    return reco_bin, gen_bin


def report_categories(reco_bin, gen_bin, weights):
    hits = (gen_bin >= 0) & (reco_bin >= 0)
    misses = (gen_bin >= 0) & (reco_bin < 0)
    fakes = (gen_bin < 0) & (reco_bin >= 0)
    neither = (gen_bin < 0) & (reco_bin < 0)
    total = len(reco_bin)
    print("\n  response matrix categories (raw / weighted):")
    for name, m in [("hits", hits), ("misses", misses),
                    ("fakes", fakes), ("neither", neither)]:
        print(f"    {name:<10} {m.sum():>10,} ({m.sum() / total:6.2%})"
              f"   {weights[m].sum():>14,.1f}")
    return dict(hits=int(hits.sum()), misses=int(misses.sum()),
                fakes=int(fakes.sum()), neither=int(neither.sum()))


def load_acceptance(path, cfg):
    """Project a gen_acceptance.py scan onto the analysis gen binning (issue #31)."""
    with np.load(path, allow_pickle=False) as data:
        hist, hist_dy = data["hist"], data["hist_dy"]
        meta = json.loads(str(data["meta"]))
    unrolled, outside = binning.project_fine(
        hist, hist_dy, cfgmod.gen_mtt_edges(cfg),
        scheme=cfgmod.scheme(cfg), y0=cfgmod.y0(cfg),
    )
    return unrolled, outside, meta


def main():
    parser = argparse.ArgumentParser(description="Build unfolding inputs from parquet")
    parser.add_argument("--era", required=True)
    parser.add_argument("--tag", default="Dump")
    parser.add_argument("--config", default=None)
    parser.add_argument("--acceptance", default=None,
                        help="gen_acceptance_{era}.npz from scripts/gen_acceptance.py; "
                             "without it the gen spectrum is of SELECTED events only "
                             "and is not a parton-level A_C (issue #31)")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    cfg = cfgmod.load(args.config)
    outdir = cfgmod.output_dir(cfg, args.era, args.tag)
    signal_path = outdir / "signal.parquet"
    if not signal_path.exists():
        raise SystemExit(f"{signal_path} not found -- run scripts/extract.py --mode signal")

    out_path = args.output or (outdir / "unfolding_inputs.root")
    gen_edges = cfgmod.gen_mtt_edges(cfg)
    reco_edges = cfgmod.reco_mtt_edges(cfg)

    print(f"config hash {cfg['config_hash']}   scheme {cfgmod.scheme(cfg)}")
    print(f"reading {signal_path}")
    df = pd.read_parquet(signal_path)

    mask, report = selection.reco_mask(df, cfg)
    print("\n  reco selection:")
    print(report)

    reco_bin, gen_bin = classify_events(df, cfg, mask)
    scale = cfgmod.lumi_scale(cfg, args.era, cfg["Signal"])
    w_nominal = df["weight_nominal"].values * scale
    categories = report_categories(reco_bin, gen_bin, w_nominal)

    fout = ROOT.TFile(str(out_path), "RECREATE")

    # --- measured spectrum (closure: signal MC as pseudo-data) ---
    # TODO(#38): this is built from the same events as the response matrix, so
    # unfolding reproduces the truth by construction and the closure test
    # cannot fail. A half/half split is the minimum bar.
    h_reco = make_th1("h_reco_measured", "Reco unrolled (pseudo-data);Bin;Events", reco_edges)
    fill_th1(h_reco, reco_bin, w_nominal, "h_reco_measured")
    h_reco.Write()

    # --- gen truth ---
    title = ("Gen unrolled (truth, SELECTED events only);Bin;Events"
             if args.acceptance is None else "Gen unrolled (truth);Bin;Events")
    h_gen = make_th1("h_gen_truth", title, gen_edges)
    fill_th1(h_gen, gen_bin, w_nominal, "h_gen_truth")
    h_gen.Write()

    # --- response matrix, nominal + systematics ---
    in_matrix = (reco_bin >= 0) | (gen_bin >= 0)
    h_matrix = make_th2("response_matrix_nominal",
                        "Nominal response matrix;Gen bin;Reco bin",
                        gen_edges, reco_edges, with_errors=True)
    fill_th2(h_matrix, gen_bin[in_matrix], reco_bin[in_matrix], w_nominal[in_matrix])
    h_matrix.Write()

    for source in cfg["systematics"]:
        for direction in ("Up", "Down"):
            column = f"weight_{source}{direction}"
            if column not in df:
                print(f"  [skip] {column} not in the parquet")
                continue
            h = make_th2(f"response_matrix_{source}{direction}",
                         f"{source} {direction};Gen bin;Reco bin",
                         gen_edges, reco_edges, with_errors=False)
            w = df[column].values * scale
            fill_th2(h, gen_bin[in_matrix], reco_bin[in_matrix], w[in_matrix])
            zero_bin_errors(h)
            h.Write()

    # --- acceptance (issue #31) ---
    if args.acceptance:
        generated, outside, meta = load_acceptance(args.acceptance, cfg)
        h_all = make_th1("h_gen_generated",
                         "Gen unrolled, ALL generated events;Bin;Events", gen_edges)
        for i, value in enumerate(generated):
            h_all.SetBinContent(i + 1, value * scale)
        h_all.Write()
        selected = np.array([h_gen.GetBinContent(i + 1)
                             for i in range(binning.n_unrolled_bins(gen_edges))])
        print(f"\n  acceptance from {os.path.basename(args.acceptance)} "
              f"({meta.get('dataset', '?')}):")
        for label, sel, gen_all in zip(binning.labels(gen_edges), selected, generated * scale):
            eff = sel / gen_all if gen_all else float("nan")
            print(f"    {label:<24} eff = {eff:6.3%}")
        print(f"    outside the gen binning: {outside * scale:,.1f}")
    else:
        print("\n  NO ACCEPTANCE INPUT (issue #31): the gen spectrum written here is "
              "of\n  SELECTED events only. Unfolding it yields N_gen(i) x eff(i), "
              "not N_gen(i),\n  and therefore NOT a parton-level A_C.")

    # Read the summary numbers out BEFORE closing: the histograms are owned by
    # the TFile, so touching them after Close() is a use-after-free.
    stats = (h_reco.GetEntries(), h_reco.Integral(),
             h_gen.GetEntries(), h_gen.Integral())

    meta = ROOT.TNamed("provenance", json.dumps({
        **cfgmod.provenance(cfg, "build_inputs.py"),
        "era": args.era, "tag": args.tag,
        "selection": [list(c) for c in report.cuts],
        "categories": categories,
        "acceptance": args.acceptance or "none",
    }))
    meta.Write()
    fout.Close()

    print(f"\n  reco histogram : {stats[0]:,.0f} entries, {stats[1]:,.1f} weighted")
    print(f"  gen  histogram : {stats[2]:,.0f} entries, {stats[3]:,.1f} weighted")
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()

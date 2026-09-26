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


def fill_response(h, gen_bins, reco_bins, weights):
    """Fill the response matrix with hits and the inefficiency column only.

    TUnfold's conventions for a (gen on X, reco on Y) matrix:

      A[i][j]  i,j both valid   a hit: generated in gen bin i, reconstructed
                                in reco bin j.
      A[i][0]  reco underflow   generated in gen bin i but NOT reconstructed.
                                This is the INEFFICIENCY, and TUnfold uses the
                                column total to normalise P(reco j | gen i).
                                Getting it right is what makes the unfolded
                                result an estimate of N_generated rather than
                                of N_generated x efficiency.
      A[0][j]  gen underflow    reconstructed but generated outside the gen
                                range -- i.e. fakes. TUnfold *unfolds* the gen
                                underflow, so putting a heterogeneous pile
                                there hands it one extra free parameter to
                                absorb m_tt-out-of-range, both-forward and
                                both-central events at once (issue #36).

    So fakes are deliberately NOT filled here. They are collected separately by
    make_fakes() and removed with TUnfoldSys::SubtractBackground, which is both
    the correct treatment and the one that propagates their uncertainty.
    """
    gen_ok = gen_bins >= 0
    reco_ok = reco_bins >= 0
    keep = gen_ok          # fakes are handled by make_fakes(), not here

    g = (gen_bins[keep] + 0.5).astype(np.float64)
    r = np.where(reco_ok[keep], reco_bins[keep] + 0.5, -0.5).astype(np.float64)
    w = weights[keep].astype(np.float64)
    selection.assert_no_nan_reaches_histogram(w, w, "response matrix")
    if len(g):
        h.FillN(len(g), g, r, w, 1)

    return {
        "hits": int((gen_ok & reco_ok).sum()),
        "misses": int((gen_ok & ~reco_ok).sum()),
        "fakes": int((~gen_ok & reco_ok).sum()),
        "neither": int((~gen_ok & ~reco_ok).sum()),
    }


def make_fakes(name, gen_bins, reco_bins, weights, reco_edges):
    """Reco-level spectrum of events with no valid gen bin.

    Subtracted from the measured spectrum rather than given a gen bin, per the
    TUnfold documentation's treatment of background (issue #36).
    """
    h = make_th1(name, "Fakes (reco valid, gen out of range);Bin;Events", reco_edges)
    fakes = (gen_bins < 0) & (reco_bins >= 0)
    fill_th1(h, np.where(fakes, reco_bins, -1), weights, name)
    return h


def add_inefficiency(h, generated, scale, gen_edges, negative_tolerance=1e-6):
    """Top up the reco-underflow column so each gen column totals what was
    GENERATED in that bin -- including events that never entered the skim.

    The parquet holds only events that passed the 002 preselection, so the
    misses already in the matrix cover just the ~4.5% that were selected and
    then failed reconstruction. `generated` comes from the lxplus gen scan
    (issue #31) and supplies the rest.

    The added weight carries the gen scan's own statistical error. Those are
    sign-only weights, so per event Var(w) = 1 and the Poisson error on a bin
    is sqrt(N_raw); with a 0.405% negative-weight fraction, N_raw is the signed
    sum to better than a percent, which is far inside any use this is put to.
    """
    n_gen = binning.n_unrolled_bins(gen_edges)
    n_reco = h.GetNbinsY()
    report = []
    for i in range(n_gen):
        column = sum(h.GetBinContent(i + 1, r) for r in range(0, n_reco + 2))
        target = generated[i] * scale
        missing = target - column
        if missing < -abs(target) * negative_tolerance:
            raise ValueError(
                f"gen bin {i}: the selected yield ({column:,.1f}) exceeds the "
                f"generated yield ({target:,.1f}). The acceptance histogram and "
                "the parquet are inconsistent -- wrong era, wrong dataset, or a "
                "different weight convention."
            )
        missing = max(missing, 0.0)
        current = h.GetBinContent(i + 1, 0)
        current_error = h.GetBinError(i + 1, 0)
        # sign weights: Var = N_raw ~ signed sum, so error ~ sqrt(N) * scale
        added_error = np.sqrt(max(missing / scale, 0.0)) * scale
        h.SetBinContent(i + 1, 0, current + missing)
        h.SetBinError(i + 1, 0, np.sqrt(current_error ** 2 + added_error ** 2))
        report.append((i, column, target, missing, column / target if target else np.nan))
    return report


def zero_bin_errors(h):
    """Zero every bin error INCLUDING under/overflow.

    The old version looped 1..GetNbinsX(), so it never reached the underflow --
    which is exactly where the misses and the inefficiency live (issue #36).
    """
    for gx in range(0, h.GetNbinsX() + 2):
        for ry in range(0, h.GetNbinsY() + 2):
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


def report_categories(df, cfg, mask, reco_bin, gen_bin, weights):
    """Category breakdown, separating m_tt out-of-range from unclassified.

    The old report merged them, so the 0.78% of reco and 0.92% of gen events
    falling outside [300, 1200) were indistinguishable from events the
    rapidity classification could not place (issue #36).
    """
    scheme, y0 = cfgmod.scheme(cfg), cfgmod.y0(cfg)
    gen_edges, reco_edges = cfgmod.gen_mtt_edges(cfg), cfgmod.reco_mtt_edges(cfg)

    def split(y_top, y_antitop, mtt, edges, extra=None):
        plus, minus = binning.classify(y_top, y_antitop, scheme=scheme, y0=y0)
        classified = plus | minus
        in_range = np.isfinite(mtt) & (mtt >= edges[0]) & (mtt < edges[-1])
        base = extra if extra is not None else np.ones(len(mtt), dtype=bool)
        return (base & ~classified, base & classified & ~in_range, ~base)

    reco_unclass, reco_oor, reco_cut = split(
        df["yt_lab"].values, df["ytbar_lab"].values, df["mtt_reco"].values,
        reco_edges, extra=mask)
    gen_unclass, gen_oor, _ = split(
        df["gen_yt"].values, df["gen_ytbar"].values, df["mtt_gen"].values, gen_edges)

    total = len(reco_bin)
    hits = (gen_bin >= 0) & (reco_bin >= 0)
    misses = (gen_bin >= 0) & (reco_bin < 0)
    fakes = (gen_bin < 0) & (reco_bin >= 0)
    neither = (gen_bin < 0) & (reco_bin < 0)

    print("\n  response matrix categories (raw / weighted):")
    for name, m in [("hits", hits), ("misses", misses),
                    ("fakes", fakes), ("neither", neither)]:
        print(f"    {name:<10} {m.sum():>10,} ({m.sum() / total:6.2%})"
              f"   {weights[m].sum():>14,.1f}")

    print("\n  why events fail each axis:")
    for name, m in [("reco: failed selection", reco_cut),
                    ("reco: m_tt out of range", reco_oor),
                    ("reco: unclassified N+/N-", reco_unclass),
                    ("gen:  m_tt out of range", gen_oor),
                    ("gen:  unclassified N+/N-", gen_unclass)]:
        print(f"    {name:<26} {m.sum():>10,} ({m.sum() / total:6.2%})")

    return dict(hits=int(hits.sum()), misses=int(misses.sum()),
                fakes=int(fakes.sum()), neither=int(neither.sum()),
                reco_failed_selection=int(reco_cut.sum()),
                reco_mtt_out_of_range=int(reco_oor.sum()),
                reco_unclassified=int(reco_unclass.sum()),
                gen_mtt_out_of_range=int(gen_oor.sum()),
                gen_unclassified=int(gen_unclass.sum()))


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
    categories = report_categories(df, cfg, mask, reco_bin, gen_bin, w_nominal)

    generated, outside_binning, acc_meta = (None, 0.0, {})
    if args.acceptance:
        generated, outside_binning, acc_meta = load_acceptance(args.acceptance, cfg)

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
    h_matrix = make_th2("response_matrix_nominal",
                        "Nominal response matrix;Gen bin;Reco bin",
                        gen_edges, reco_edges, with_errors=True)
    fill_response(h_matrix, gen_bin, reco_bin, w_nominal)

    # Fakes are subtracted from the measured spectrum, not given a gen bin.
    h_fakes = make_fakes("h_fakes", gen_bin, reco_bin, w_nominal, reco_edges)
    h_fakes.Write()

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
            fill_response(h, gen_bin, reco_bin, w)
            if generated is not None:
                add_inefficiency(h, generated, scale, gen_edges)
            zero_bin_errors(h)
            h.Write()

    # --- inefficiency column from the gen scan (issues #31, #36) ---
    if generated is not None:
        rows = add_inefficiency(h_matrix, generated, scale, gen_edges)
        h_all = make_th1("h_gen_generated",
                         "Gen unrolled, ALL generated events;Bin;Events", gen_edges)
        for i, value in enumerate(generated):
            h_all.SetBinContent(i + 1, value * scale)
            h_all.SetBinError(i + 1, np.sqrt(max(value, 0.0)) * scale)
        h_all.Write()

        print(f"\n  inefficiency column from {os.path.basename(args.acceptance)}")
        print(f"  ({acc_meta.get('dataset', '?')})")
        print(f"    {'bin':<24} {'selected':>12} {'generated':>14} {'efficiency':>11}")
        for i, column, target, _, eff in rows:
            print(f"    {binning.labels(gen_edges)[i]:<24} {column:>12,.1f} "
                  f"{target:>14,.1f} {eff:>11.3%}")
        print(f"    {'outside the gen binning':<24} {'':>12} "
              f"{outside_binning * scale:>14,.1f}")
    else:
        print("\n  NO ACCEPTANCE INPUT (issue #31): the response matrix has no "
              "inefficiency\n  column, so unfolding yields N_gen(i) x eff(i), "
              "not N_gen(i), and the result\n  is NOT a parton-level A_C.")

    h_matrix.Write()

    # Read the summary numbers out BEFORE closing: the histograms are owned by
    # the TFile, so touching them after Close() is a use-after-free.
    stats = (h_reco.GetEntries(), h_reco.Integral(),
             h_gen.GetEntries(), h_gen.Integral(), h_fakes.Integral())

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
    print(f"  fakes          : {stats[4]:,.1f} weighted "
          f"({stats[4] / stats[1]:.2%} of the measured spectrum)")
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()

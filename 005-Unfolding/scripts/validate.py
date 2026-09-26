#!/usr/bin/env python3
"""Validation suite for the 005 unfolding (issue #38).

The closure test that existed before this built its pseudo-data from the *same
events* as the response matrix, so unfolding reproduced the truth by
construction, the pulls came out identically 0.000, and nothing beyond wiring
was tested. Five tests replace it:

  split-closure   response matrix from half the MC, pseudo-data from the other
                  half. The minimum bar for a closure test meaning anything.

  stress          inject a known asymmetry by reweighting the gen spectrum,
                  unfold with the NOMINAL response matrix, and check the
                  recovered value is linear in the injected one with unit
                  slope. For an asymmetry measurement this is *the* essential
                  test: it is what exposes regularisation pulling the result
                  back toward the MC prior.

  toys            Poisson pseudo-experiments at data-like statistics; pull
                  mean, width and coverage from the propagated covariance.

  foldback        A . x_unfolded against the measured spectrum, with a chi2.

  conditioning    condition number of the response matrix and the global
                  correlation coefficients rho_i, which say whether the chosen
                  binning is viable at all.

Everything routes through unfold.run_unfold, the same function the measurement
uses -- a suite that reimplemented the unfolding would be testing a copy.

Usage:
    python scripts/validate.py --era UL2016preVFP --tag midNov
    python scripts/validate.py --era UL2016preVFP --tag midNov --toys 1000
    python scripts/validate.py --era UL2016preVFP --tag midNov --only stress
"""

import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import asymmetry  # noqa: E402
import binning  # noqa: E402
import build_inputs as bi  # noqa: E402
import config as cfgmod  # noqa: E402
import selection  # noqa: E402
from tunfold_env import ROOT, load_tunfold  # noqa: E402
import unfold as unf  # noqa: E402

_counter = [0]


def _uid(stem):
    _counter[0] += 1
    return f"{stem}_{_counter[0]}"


def prepare(cfg, era, tag):
    """Load the signal parquet and compute bins and weights once."""
    path = cfgmod.output_dir(cfg, era, tag, create=False) / "signal.parquet"
    if not path.exists():
        raise SystemExit(f"{path} not found -- run scripts/extract.py --mode signal")
    df = pd.read_parquet(path)
    mask, report = selection.reco_mask(df, cfg)
    reco_bin, gen_bin = bi.classify_events(df, cfg, mask)
    weights = df["weight_nominal"].values * cfgmod.lumi_scale(cfg, era, cfg["Signal"])
    return df, reco_bin, gen_bin, weights, report


def build(gen_bin, reco_bin, weights, gen_edges, reco_edges, subset=None,
          data_like=False, generated=None, scale=1.0, fraction=1.0):
    """Measured histogram, truth histogram and response matrix for a subset.

    data_like replaces the MC statistical errors with Poisson errors on the
    expected yield, so a toy study measures coverage for a real dataset rather
    than for the MC's much larger effective sample.

    `generated` is the gen-scan spectrum (issue #31). When supplied, the matrix
    gets a real inefficiency column and the truth histogram becomes the FULL
    generated spectrum -- so the suite tests the configuration that actually
    ships rather than a matrix without acceptance. `fraction` scales it for a
    subset: a random half of the events corresponds to half the generated
    sample.
    """
    if subset is None:
        subset = np.ones(len(weights), dtype=bool)

    h_data = bi.make_th1(_uid("h_data"), ";Bin;Events", reco_edges)
    bi.fill_th1(h_data, reco_bin[subset], weights[subset], "validate h_data")
    h_data.SetDirectory(0)

    h_truth = bi.make_th1(_uid("h_truth"), ";Bin;Events", gen_edges)
    bi.fill_th1(h_truth, gen_bin[subset], weights[subset], "validate h_truth")
    h_truth.SetDirectory(0)

    h_matrix = bi.make_th2(_uid("h_matrix"), ";Gen bin;Reco bin",
                           gen_edges, reco_edges, with_errors=True)
    inside = np.where(subset, gen_bin, -1)
    bi.fill_response(h_matrix, inside, np.where(subset, reco_bin, -1), weights)
    h_matrix.SetDirectory(0)

    if generated is not None:
        bi.add_inefficiency(h_matrix, np.asarray(generated) * fraction,
                            scale, gen_edges)
        # truth is then the full generated spectrum, not the selected one
        h_truth = bi.make_th1(_uid("h_truth_gen"), ";Bin;Events", gen_edges)
        h_truth.SetDirectory(0)
        for i, value in enumerate(np.asarray(generated) * fraction):
            h_truth.SetBinContent(i + 1, value * scale)
            h_truth.SetBinError(i + 1, np.sqrt(max(value * fraction, 0.0)) * scale)

    # Fakes, so the suite unfolds the same way the measurement does. Without
    # this the measured spectrum carries ~0.5% of events the matrix knows
    # nothing about, which showed up as a consistent 0.6% excess in the
    # unfolded yield and a linearity slope of 0.994 rather than 1.
    h_fakes = bi.make_fakes(_uid("h_fakes"), np.where(subset, gen_bin, -1),
                            np.where(subset, reco_bin, -1), weights, reco_edges)
    h_fakes.SetDirectory(0)

    if data_like:
        for i in range(1, h_data.GetNbinsX() + 1):
            content = h_data.GetBinContent(i)
            h_data.SetBinError(i, np.sqrt(max(content, 0.0)))

    return h_data, h_truth, h_matrix, h_fakes


def _vector(hist, n):
    return np.array([hist.GetBinContent(i + 1) for i in range(n)])


def chi2_against(x, truth, cov):
    """(x-t)^T C^-1 (x-t), using a pseudo-inverse because the unfolded
    covariance is close to singular at rho_avg ~ 0.98."""
    delta = x - truth
    return float(delta @ np.linalg.pinv(cov, rcond=1e-10) @ delta)


# ---------------------------------------------------------------------------
# 1. split closure
# ---------------------------------------------------------------------------

def test_split_closure(cfg, era, data, gen_edges, reco_edges, tau=0.0,
                       acceptance=None, scale=1.0, seed=12345):
    _, reco_bin, gen_bin, weights, _ = data
    rng = np.random.default_rng(seed)
    half_a = rng.random(len(weights)) < 0.5
    half_b = ~half_a
    n_gen = binning.n_unrolled_bins(gen_edges)

    # Response from A, pseudo-data and truth from B. Disjoint by construction,
    # which is the whole point -- the old test used the same events for both.
    _, _, h_matrix, _ = build(gen_bin, reco_bin, weights, gen_edges, reco_edges,
                              half_a, generated=acceptance, scale=scale, fraction=0.5)
    h_data, h_truth, _, h_fakes = build(gen_bin, reco_bin, weights, gen_edges,
                                       reco_edges, half_b, generated=acceptance,
                                       scale=scale, fraction=0.5)

    result = unf.run_unfold(h_matrix, h_data, n_gen, tau=tau, quiet=True,
                            backgrounds=[("fakes", h_fakes, 1.0, 0.0)])
    x, cov = result["x"], result["cov_stat"]
    truth = _vector(h_truth, n_gen)

    pulls = np.where(np.sqrt(np.diag(cov)) > 0,
                     (x - truth) / np.sqrt(np.diag(cov)), np.nan)
    chi2 = chi2_against(x, truth, cov)

    ac_unf, ac_cov = asymmetry.propagate(x, cov, gen_edges)
    ac_truth, _ = asymmetry.jacobian(truth, gen_edges)
    sigma = asymmetry.errors(ac_cov)[-1]
    deviation = (ac_unf[-1] - ac_truth[-1]) / sigma if sigma > 0 else np.nan

    print(f"  events: half A {half_a.sum():,}  half B {half_b.sum():,}")
    print(f"  per-bin pulls (stat cov): "
          f"{np.array2string(pulls, precision=2, floatmode='fixed')}")
    print(f"  spectrum chi2 / ndf = {chi2:.2f} / {n_gen} = {chi2 / n_gen:.2f}")
    if acceptance is not None:
        print("    (the response matrix's own statistical error is not propagated "
              "here, so a\n     value somewhat above 1 is expected. A_C is the "
              "quantity judged below.)")
    print(f"  A_C  unfolded {ac_unf[-1]:+.5f} +- {sigma:.5f}   "
          f"truth(B) {ac_truth[-1]:+.5f}   deviation {deviation:+.2f} sigma")
    ok = abs(deviation) < 3.0
    print(f"  -> {'PASS' if ok else 'FAIL'}: A_C recovered within 3 sigma")
    return {"chi2": chi2, "ndf": n_gen, "pulls": pulls.tolist(),
            "A_C_unfolded": ac_unf[-1], "A_C_truth": ac_truth[-1],
            "deviation_sigma": deviation, "pass": bool(ok)}


# ---------------------------------------------------------------------------
# 2. stress / linearity
# ---------------------------------------------------------------------------

def test_stress(cfg, era, data, gen_edges, reco_edges, tau=0.0,
                acceptance=None, scale=1.0,
                injections=(-0.05, -0.02, 0.0, 0.02, 0.05)):
    _, reco_bin, gen_bin, weights, _ = data
    n_gen = binning.n_unrolled_bins(gen_edges)
    n_mtt = len(gen_edges) - 1

    # The nominal response matrix stays fixed: it is the MC prior, and the
    # question is whether unfolding data drawn from a *different* truth with it
    # returns that different truth or drags the answer back toward the prior.
    _, _, h_matrix, _ = build(gen_bin, reco_bin, weights, gen_edges, reco_edges,
                              generated=acceptance, scale=scale)

    # sign of delta|y| at gen level, from the unrolled bin index
    gen_sign = np.zeros(len(weights))
    valid = gen_bin >= 0
    gen_sign[valid] = np.where(gen_bin[valid] < n_mtt, +1.0, -1.0)

    # The injection is a change to the underlying TRUTH, so the generated
    # spectrum must be reweighted by the same factor as the events. Leaving the
    # acceptance vector fixed compares a reweighted measurement against an
    # unreweighted truth -- which is what made an earlier version of this test
    # report slope 0.497 while the unfolding was in fact tracking the injection
    # correctly.
    bin_sign = np.where(np.arange(n_gen) < n_mtt, +1.0, -1.0)

    rows = []
    print(f"  {'injected':>10} {'truth A_C':>11} {'unfolded':>11} {'+- stat':>10} "
          f"{'residual':>10}")
    for eps in injections:
        w = weights * (1.0 + eps * gen_sign)
        injected_gen = (None if acceptance is None
                        else np.asarray(acceptance) * (1.0 + eps * bin_sign))
        h_data, h_truth, _, h_fakes = build(gen_bin, reco_bin, w, gen_edges,
                                           reco_edges, generated=injected_gen,
                                           scale=scale)
        result = unf.run_unfold(h_matrix, h_data, n_gen, tau=tau, quiet=True,
                                backgrounds=[("fakes", h_fakes, 1.0, 0.0)])
        ac_unf, ac_cov = asymmetry.propagate(result["x"], result["cov_stat"], gen_edges)
        ac_truth, _ = asymmetry.jacobian(_vector(h_truth, n_gen), gen_edges)
        sigma = asymmetry.errors(ac_cov)[-1]
        rows.append((eps, ac_truth[-1], ac_unf[-1], sigma))
        print(f"  {eps:>+10.3f} {ac_truth[-1]:>+11.5f} {ac_unf[-1]:>+11.5f} "
              f"{sigma:>10.5f} {ac_unf[-1] - ac_truth[-1]:>+10.5f}")

    truth_values = np.array([r[1] for r in rows])
    unfolded_values = np.array([r[2] for r in rows])
    slope, intercept = np.polyfit(truth_values, unfolded_values, 1)
    residuals = unfolded_values - (slope * truth_values + intercept)
    print(f"  linearity fit: slope {slope:.5f}, intercept {intercept:+.6f}, "
          f"max residual {np.abs(residuals).max():.2e}")
    ok = abs(slope - 1.0) < 0.05
    print(f"  -> {'PASS' if ok else 'FAIL'}: slope within 5% of unity "
          f"({'no' if ok else 'SIGNIFICANT'} regularisation bias)")

    # At tau = 0 this test cannot fail: plain inversion has no prior to pull
    # toward. Probe with the OLD 'unrolled' curvature at a strong tau, which is
    # exactly the pathology #35 fixed -- it smooths across the N+/N- boundary,
    # i.e. across the difference A_C is made of. If this probe ever comes back
    # at ~1, the test has stopped being able to see regularisation bias.
    probe_tau = 1e-3
    probe_mode = "unrolled"
    probe = []
    for eps in (injections[0], injections[-1]):
        w = weights * (1.0 + eps * gen_sign)
        injected_gen = (None if acceptance is None
                        else np.asarray(acceptance) * (1.0 + eps * bin_sign))
        h_data, h_truth, _, h_fakes = build(gen_bin, reco_bin, w, gen_edges,
                                           reco_edges, generated=injected_gen,
                                           scale=scale)
        r = unf.run_unfold(h_matrix, h_data, n_gen, tau=probe_tau, quiet=True,
                           regularisation=probe_mode,
                           backgrounds=[("fakes", h_fakes, 1.0, 0.0)])
        ac_unf, _ = asymmetry.propagate(r["x"], r["cov_stat"], gen_edges)
        ac_truth, _ = asymmetry.jacobian(_vector(h_truth, n_gen), gen_edges)
        probe.append((ac_truth[-1], ac_unf[-1]))
    probe_slope = ((probe[1][1] - probe[0][1]) / (probe[1][0] - probe[0][0])
                   if probe[1][0] != probe[0][0] else float("nan"))
    verdict = ("test has teeth" if abs(probe_slope - 1) > 0.01
               else "STILL ~1 -- this test may not be sensitive to "
                    "regularisation at all")
    print(f"  sensitivity check, '{probe_mode}' regularisation at tau = "
          f"{probe_tau:g}: slope {probe_slope:.5f}\n      ({verdict})")

    return {"rows": [list(map(float, r)) for r in rows],
            "slope": float(slope), "intercept": float(intercept),
            "max_residual": float(np.abs(residuals).max()),
            "probe_tau": probe_tau, "probe_mode": probe_mode,
            "probe_slope": float(probe_slope),
            "pass": bool(ok)}


# ---------------------------------------------------------------------------
# 3. toy pseudo-experiments
# ---------------------------------------------------------------------------

def test_toys(cfg, era, data, gen_edges, reco_edges, tau=0.0,
              acceptance=None, scale=1.0, n_toys=300, seed=987):
    _, reco_bin, gen_bin, weights, _ = data
    n_gen = binning.n_unrolled_bins(gen_edges)
    rng = np.random.default_rng(seed)

    # data_like=True so the toys probe coverage at the statistics a real
    # dataset would have, not at the MC's ~19x larger effective sample.
    h_data, h_truth, h_matrix, h_fakes = build(gen_bin, reco_bin, weights,
                                               gen_edges, reco_edges,
                                               data_like=True,
                                               generated=acceptance, scale=scale)
    n_reco = h_data.GetNbinsX()
    expectation = np.array([h_data.GetBinContent(i + 1) for i in range(n_reco)])
    ac_truth, _ = asymmetry.jacobian(_vector(h_truth, n_gen), gen_edges)

    pulls, values, sigmas, failures = [], [], [], 0
    for _ in range(n_toys):
        thrown = rng.poisson(np.clip(expectation, 0, None)).astype(float)
        h_toy = bi.make_th1(_uid("h_toy"), ";Bin;Events", reco_edges)
        h_toy.SetDirectory(0)
        for i in range(n_reco):
            h_toy.SetBinContent(i + 1, thrown[i])
            h_toy.SetBinError(i + 1, np.sqrt(max(thrown[i], 1.0)))
        try:
            result = unf.run_unfold(h_matrix, h_toy, n_gen, tau=tau, quiet=True,
                                    backgrounds=[("fakes", h_fakes, 1.0, 0.0)])
        except RuntimeError:
            failures += 1
            continue
        ac, cov = asymmetry.propagate(result["x"], result["cov_stat"], gen_edges)
        sigma = asymmetry.errors(cov)[-1]
        if not np.isfinite(sigma) or sigma <= 0:
            failures += 1
            continue
        values.append(ac[-1])
        sigmas.append(sigma)
        pulls.append((ac[-1] - ac_truth[-1]) / sigma)

    pulls = np.array(pulls)
    values = np.array(values)
    within1 = np.mean(np.abs(pulls) < 1.0)
    within2 = np.mean(np.abs(pulls) < 2.0)
    print(f"  {len(pulls)} toys ({failures} failed)")
    print(f"  truth A_C        : {ac_truth[-1]:+.5f}")
    print(f"  toy mean A_C     : {values.mean():+.5f}  (bias "
          f"{values.mean() - ac_truth[-1]:+.5f})")
    print(f"  toy spread       : {values.std(ddof=1):.5f}")
    print(f"  mean quoted sigma: {np.mean(sigmas):.5f}")
    print(f"  pull mean {pulls.mean():+.3f}   pull width {pulls.std(ddof=1):.3f}")
    print(f"  coverage: {within1:.1%} within 1 sigma (expect 68.3%), "
          f"{within2:.1%} within 2 sigma (expect 95.4%)")
    ok = abs(pulls.mean()) < 0.2 and 0.8 < pulls.std(ddof=1) < 1.25
    print(f"  -> {'PASS' if ok else 'FAIL'}: pull consistent with N(0,1)")
    return {"n_toys": len(pulls), "failures": failures,
            "A_C_truth": ac_truth[-1], "toy_mean": float(values.mean()),
            "toy_spread": float(values.std(ddof=1)),
            "mean_sigma": float(np.mean(sigmas)),
            "pull_mean": float(pulls.mean()), "pull_width": float(pulls.std(ddof=1)),
            "coverage_1sigma": float(within1), "coverage_2sigma": float(within2),
            "pass": bool(ok)}


# ---------------------------------------------------------------------------
# 4. fold-back
# ---------------------------------------------------------------------------

def test_foldback(cfg, era, data, gen_edges, reco_edges, tau=0.0,
                  acceptance=None, scale=1.0, seed=12345):
    """A . x_unfolded against the measured spectrum.

    Uses DISJOINT halves, like the split closure. Folding back a result that
    was unfolded with a response matrix built from the very same events gives
    chi2 = 0.000 exactly -- the model is right by construction, so the test
    passes without testing anything. Measured: 0.000 with shared events,
    31.18 with disjoint halves.
    """
    _, reco_bin, gen_bin, weights, _ = data
    n_gen = binning.n_unrolled_bins(gen_edges)
    rng = np.random.default_rng(seed)
    half_a = rng.random(len(weights)) < 0.5

    _, _, h_matrix, _ = build(gen_bin, reco_bin, weights, gen_edges, reco_edges,
                              half_a, generated=acceptance, scale=scale, fraction=0.5)
    h_data, _, _, h_fakes = build(gen_bin, reco_bin, weights, gen_edges, reco_edges,
                                  ~half_a, generated=acceptance, scale=scale,
                                  fraction=0.5)
    result = unf.run_unfold(h_matrix, h_data, n_gen, tau=tau, quiet=True,
                            backgrounds=[("fakes", h_fakes, 1.0, 0.0)])

    folded = result["unfold"].GetFoldedOutput(_uid("folded"))
    folded.SetDirectory(0)

    # GetFoldedOutput returns A . x, which does not include the subtracted
    # background. Compare it against what was actually unfolded -- the measured
    # spectrum MINUS the fakes -- or the comparison is inconsistent by the size
    # of the subtraction (chi2/ndf 20.8 instead of 1.2).
    chi2, used = 0.0, 0
    for i in range(1, h_data.GetNbinsX() + 1):
        error = h_data.GetBinError(i)
        if error > 0:
            measured = h_data.GetBinContent(i) - h_fakes.GetBinContent(i)
            chi2 += ((measured - folded.GetBinContent(i)) / error) ** 2
            used += 1
    ndf = max(used - n_gen, 1)
    print(f"  reco bins used {used}, gen bins {n_gen}, ndf {ndf}")
    print(f"  chi2 / ndf = {chi2:.2f} / {ndf} = {chi2 / ndf:.3f}")
    print(f"  (compared against measured - fakes, which is what was unfolded;"
          f"\n   TUnfold's own chi2A = {result['chi2A']:.2f})")
    print(f"  note: the response matrix's own statistical uncertainty is not in "
          f"this chi2,\n        so a value somewhat above 1 is expected here.")
    ok = chi2 / ndf < 3.0
    print(f"  -> {'PASS' if ok else 'FAIL'}: folded prediction consistent with input")
    return {"chi2": float(chi2), "ndf": int(ndf), "chi2_per_ndf": float(chi2 / ndf),
            "pass": bool(ok)}


# ---------------------------------------------------------------------------
# 5. conditioning
# ---------------------------------------------------------------------------

def test_conditioning(cfg, era, data, gen_edges, reco_edges, tau=0.0,
                      acceptance=None, scale=1.0):
    _, reco_bin, gen_bin, weights, _ = data
    n_gen = binning.n_unrolled_bins(gen_edges)
    h_data, _, h_matrix, h_fakes = build(gen_bin, reco_bin, weights, gen_edges,
                                         reco_edges, generated=acceptance,
                                         scale=scale)
    result = unf.run_unfold(h_matrix, h_data, n_gen, tau=tau, quiet=True,
                            backgrounds=[("fakes", h_fakes, 1.0, 0.0)])

    n_reco = h_matrix.GetNbinsY()
    A = np.array([[h_matrix.GetBinContent(g + 1, r + 1) for g in range(n_gen)]
                  for r in range(n_reco)])
    column_sums = A.sum(axis=0)
    probabilities = np.divide(A, column_sums, out=np.zeros_like(A),
                              where=column_sums > 0)
    condition = np.linalg.cond(probabilities)

    rho = []
    h_rho = result["unfold"].GetRhoItotal(_uid("rho"))
    if h_rho:
        h_rho.SetDirectory(0)
        rho = [h_rho.GetBinContent(i + 1) for i in range(n_gen)]

    purity = np.array([probabilities[g, g] if g < n_reco else np.nan
                       for g in range(n_gen)])
    print(f"  response matrix {n_reco} reco x {n_gen} gen")
    print(f"  condition number (column-normalised) = {condition:.1f}")
    if rho:
        print(f"  global correlation rho_i: "
              f"{np.array2string(np.array(rho), precision=3, floatmode='fixed')}")
        print(f"  max rho_i = {max(rho):.4f}")
    ok = condition < 1e4
    print(f"  -> {'PASS' if ok else 'FAIL'}: matrix not pathologically "
          f"ill-conditioned")
    return {"condition_number": float(condition), "rho_i": rho,
            "diagonal_probability": purity.tolist(), "pass": bool(ok)}


TESTS = {
    "split-closure": test_split_closure,
    "stress": test_stress,
    "toys": test_toys,
    "foldback": test_foldback,
    "conditioning": test_conditioning,
}


def main():
    parser = argparse.ArgumentParser(description="Validation suite for the 005 unfolding")
    parser.add_argument("--era", required=True)
    parser.add_argument("--tag", default="Dump")
    parser.add_argument("--config", default=None)
    parser.add_argument("--toys", type=int, default=300)
    parser.add_argument("--acceptance", default=None,
                        help="gen_acceptance npz; when given, the suite tests "
                             "the matrix WITH its inefficiency column, i.e. the "
                             "configuration that actually ships")
    parser.add_argument("--tau", type=float, default=0.0,
                        help="fixed regularisation strength for every test. "
                             "Default 0 because that is what the L-curve picks "
                             "for the nominal fit -- with ~12 gen bins the "
                             "problem is barely ill-posed. Re-scanning the "
                             "L-curve inside each toy would cost 100 unfolds "
                             "per toy AND give each toy its own tau, which the "
                             "real measurement does not have.")
    parser.add_argument("--only", nargs="+", choices=sorted(TESTS),
                        help="run only these tests")
    args = parser.parse_args()

    load_tunfold()
    ROOT.gROOT.SetBatch(True)
    cfg = cfgmod.load(args.config)
    gen_edges = cfgmod.gen_mtt_edges(cfg)
    reco_edges = cfgmod.reco_mtt_edges(cfg)

    print(f"config hash {cfg['config_hash']}   scheme {cfgmod.scheme(cfg)}   "
          f"tau {args.tau}")
    data = prepare(cfg, args.era, args.tag)
    acceptance, _, _ = (bi.load_acceptance(args.acceptance, cfg)
                        if args.acceptance else (None, 0.0, {}))
    scale = cfgmod.lumi_scale(cfg, args.era, cfg["Signal"])
    acc_note = ("yes" if acceptance is not None
                else "NO -- matrix has no inefficiency column")
    print(f"loaded {len(data[3]):,} events   acceptance: {acc_note}\n")

    chosen = args.only or list(TESTS)
    results = {}
    for name in chosen:
        print(f"{'=' * 72}\n{name}\n{'-' * 72}")
        kwargs = {"tau": args.tau, "acceptance": acceptance, "scale": scale}
        if name == "toys":
            kwargs["n_toys"] = args.toys
        results[name] = TESTS[name](cfg, args.era, data, gen_edges,
                                    reco_edges, **kwargs)
        print()

    outdir = cfgmod.output_dir(cfg, args.era, args.tag)
    report = outdir / "validation.json"
    report.write_text(json.dumps(
        {"provenance": cfgmod.provenance(cfg, "validate.py"),
         "era": args.era, "tag": args.tag, "results": results}, indent=2))

    print(f"{'=' * 72}")
    failed = [n for n, r in results.items() if not r.get("pass", True)]
    for name, result in results.items():
        print(f"  {'PASS' if result.get('pass', True) else 'FAIL'}  {name}")
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    print(f"Wrote {report}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

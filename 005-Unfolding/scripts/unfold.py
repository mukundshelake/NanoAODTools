#!/usr/bin/env python3
"""TUnfold-based unfolding for the ttbar charge asymmetry. Replaces tunfold.py.

Usage:
    python scripts/unfold.py --era UL2016preVFP --tag midNov

Reads `unfolding_inputs.root` from build_inputs.py and writes
`unfolding_results.root` plus figures alongside it.

Scope note: this is a port of the old tunfold.py onto the shared config and the
vendored TUnfold, with the binning read from config.yaml instead of retyped.
Two known physics problems are deliberately left marked rather than
half-addressed here, because each is its own issue:

  TODO(#35) kRegModeCurvature runs over consecutive unrolled bin indices, so it
            penalises curvature between the highest-m_tt N+ bin and the
            lowest-m_tt N- bin -- two physically unrelated bins, either side of
            the boundary the whole measurement rests on. The fix is to build
            the binning with TUnfoldBinning, or to use kRegModeNone plus
            explicit per-block RegularizeCurvature calls. kDensityModeBinWidth
            is also a no-op on a unit-width unrolled axis, and kEConstraintArea
            fixes the normalisation, which is a choice that needs justifying
            against kEConstraintNone.

(#39 is done: A_C is extracted below, with its uncertainty propagated through
the full covariance by scripts/asymmetry.py.)
"""

import argparse
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import asymmetry  # noqa: E402
import binning  # noqa: E402
import config as cfgmod  # noqa: E402
import plots  # noqa: E402
from tunfold_env import ROOT, load_tunfold  # noqa: E402


def run_unfold(h_matrix, h_data, n_gen_bins, tau=None, f_resp=None,
               sources=(), quiet=False, backgrounds=()):
    """Unfold one (matrix, measured) pair and return everything downstream needs.

    Factored out of main() so validate.py exercises the *same* code path the
    measurement uses. A validation suite that reimplements the unfolding tests
    a copy of the code rather than the code (issue #38).

    Returns a dict with the unfolded vector, both covariances, the live TUnfold
    object (for GetRhoItotal / GetDeltaSysSource / GetFoldedOutput) and the
    scan diagnostics.
    """
    # TODO(#35): kRegModeCurvature smooths across the N+/N- boundary.
    unfold = ROOT.TUnfoldDensity(
        h_matrix,
        ROOT.TUnfold.kHistMapOutputHoriz,
        ROOT.TUnfold.kRegModeCurvature,
        ROOT.TUnfold.kEConstraintArea,
        ROOT.TUnfoldDensity.kDensityModeBinWidth,
    )

    status = unfold.SetInput(h_data)
    if status >= 10000:
        raise RuntimeError(
            f"SetInput failed (status={status}) -- the measured histogram binning "
            "does not match the response matrix Y axis"
        )

    # Fakes and physics backgrounds are removed from the measured spectrum
    # here rather than given a gen bin, so their normalisation uncertainty
    # propagates into the unfolded covariance (issue #36).
    for name, hist, scale, scale_error in backgrounds:
        unfold.SubtractBackground(hist, name, scale, scale_error)
        if not quiet:
            print(f"  [bkg]  {name}: {hist.Integral():,.1f} events "
                  f"(scale {scale:g} +- {scale_error:g})")

    added = []
    if f_resp is not None and sources:
        added = add_systematics(unfold, f_resp, list(sources), quiet=quiet)

    l_curve, best_index = ROOT.TGraph(), 0
    if tau is not None:
        unfold.DoUnfold(tau)
    else:
        best_index = unfold.ScanLcurve(100, 0.0, 0.0, l_curve,
                                       ROOT.TSpline3(), ROOT.TSpline3())
        tau = unfold.GetTau()

    out = unfold.GetOutput("h_unfolded")
    out.SetDirectory(0)
    cov_total_h = unfold.GetEmatrixTotal("h_cov_total")
    cov_total_h.SetDirectory(0)
    cov_stat_h = unfold.GetEmatrixInput("h_cov_stat")
    cov_stat_h.SetDirectory(0)

    return {
        "unfold": unfold,
        "h_unfolded": out,
        "h_cov_stat": cov_stat_h,
        "h_cov_total": cov_total_h,
        "x": np.array([out.GetBinContent(i + 1) for i in range(n_gen_bins)]),
        "cov_stat": th2_to_matrix(cov_stat_h, n_gen_bins),
        "cov_total": th2_to_matrix(cov_total_h, n_gen_bins),
        "tau": tau,
        "best_index": best_index,
        "l_curve": l_curve,
        "status": status,
        "systematics": added,
        "rho_avg": unfold.GetRhoAvg(),
        "chi2A": unfold.GetChi2A(),
        "chi2L": unfold.GetChi2L(),
    }


def th2_to_matrix(hist, n):
    """TUnfold covariance TH2 -> numpy matrix over the n physical bins."""
    out = np.empty((n, n), dtype=np.float64)
    for i in range(n):
        for j in range(n):
            out[i, j] = hist.GetBinContent(i + 1, j + 1)
    return out


def add_systematics(unfold, f_resp, sources, quiet=False):
    """Register each response-matrix variation as a TUnfoldSys shift."""
    added = []
    for source in sources:
        h_up = f_resp.Get(f"response_matrix_{source}Up")
        h_down = f_resp.Get(f"response_matrix_{source}Down")
        if not h_up or not h_down:
            if not quiet:
                print(f"  [skip] {source}: variation histograms not in the input file")
            continue
        h_up.SetDirectory(0)
        h_down.SetDirectory(0)

        shift = h_up.Clone(f"h_sys_shift_{source}")
        shift.SetDirectory(0)
        shift.Add(h_down, -1.0)
        shift.Scale(0.5)

        unfold.AddSysError(shift, source, ROOT.TUnfold.kHistMapOutputHoriz,
                           ROOT.TUnfoldSys.kSysErrModeShift)
        added.append(source)
        if not quiet:
            print(f"  [ok]   {source}")
    return added


def main():
    parser = argparse.ArgumentParser(description="TUnfold unfolding for the ttbar charge asymmetry")
    parser.add_argument("--era", required=True)
    parser.add_argument("--tag", default="Dump")
    parser.add_argument("--config", default=None)
    parser.add_argument("--inputs", default=None, help="override unfolding_inputs.root")
    parser.add_argument("--outdir", default=None)
    parser.add_argument("--fakes-error", type=float, default=0.0,
                        help="fractional uncertainty on the fake subtraction, "
                             "passed to SubtractBackground. 0 treats the MC "
                             "fake prediction as exact; raise it once the fake "
                             "composition is understood (issue #36)")
    parser.add_argument("--tau", type=float, default=None,
                        help="fixed regularisation strength; default is an L-curve scan. "
                             "With ~12 gen bins the problem is barely ill-posed, so "
                             "--tau 0 (plain inversion) is worth evaluating (issue #35)")
    args = parser.parse_args()

    load_tunfold()
    cfg = cfgmod.load(args.config)
    outdir = cfgmod.output_dir(cfg, args.era, args.tag)
    inputs = args.inputs or (outdir / "unfolding_inputs.root")
    plotdir = args.outdir or str(outdir / "plots")
    gen_edges = cfgmod.gen_mtt_edges(cfg)
    n_gen_bins = binning.n_unrolled_bins(gen_edges)
    n_gen_mtt = len(gen_edges) - 1

    print(f"config hash {cfg['config_hash']}")
    print(f"reading {inputs}")
    f_in = ROOT.TFile.Open(str(inputs))
    if not f_in or f_in.IsZombie():
        raise SystemExit(f"cannot open {inputs} -- run scripts/build_inputs.py first")

    h_data = f_in.Get("h_reco_measured")
    # With an inefficiency column in the matrix the unfolded vector estimates
    # the FULL generated spectrum, so comparing it against the spectrum of
    # selected events would be comparing two different quantities -- and would
    # show pulls of ~120 that mean nothing. Prefer h_gen_generated when the
    # acceptance was applied (issues #31, #36).
    h_truth = f_in.Get("h_gen_generated")
    truth_label = "all generated"
    if not h_truth:
        h_truth = f_in.Get("h_gen_truth")
        truth_label = "SELECTED events only -- no acceptance applied"
    h_matrix = f_in.Get("response_matrix_nominal")
    for obj, name in [(h_data, "h_reco_measured"), (h_truth, "truth"),
                      (h_matrix, "response_matrix_nominal")]:
        if not obj:
            raise SystemExit(f"'{name}' not found in {inputs}")
        obj.SetDirectory(0)

    backgrounds = []
    h_fakes = f_in.Get("h_fakes")
    if h_fakes:
        h_fakes.SetDirectory(0)
        backgrounds.append(("fakes", h_fakes, 1.0, args.fakes_error))
    else:
        print("\n  NOTE: no h_fakes in the input file; fakes are not being "
              "subtracted (issue #36).")

    print("\nBackgrounds and systematics:")
    result = run_unfold(h_matrix, h_data, n_gen_bins, tau=args.tau,
                        f_resp=f_in, sources=cfg["systematics"],
                        backgrounds=backgrounds)
    unfold = result["unfold"]
    added = result["systematics"]
    tau, best_index, l_curve = result["tau"], result["best_index"], result["l_curve"]
    print(f"SetInput status: {result['status']}")
    if args.tau is not None:
        print(f"Fixed tau = {tau:.8f}")
    else:
        print(f"  best scan point : {best_index}")
        print(f"  optimal tau     : {tau:.8f}")
    print(f"  chi2(A)         : {result['chi2A']:.4f}")
    print(f"  chi2(L)         : {result['chi2L']:.4f}")
    print(f"  rho avg         : {result['rho_avg']:.4f}")

    h_unfolded = result["h_unfolded"]
    for i in range(1, n_gen_bins + 1):
        h_unfolded.GetXaxis().SetBinLabel(i, h_truth.GetXaxis().GetBinLabel(i))
    if truth_label.startswith("SELECTED"):
        print("\n  NOTE: no acceptance was applied, so 'truth' below is the gen "
              "spectrum of\n  selected events and the result is NOT a "
              "parton-level A_C (issue #31).")

    h_cov_total, h_cov_stat = result["h_cov_total"], result["h_cov_stat"]

    # TODO(#38): for a closure test on the same MC the pull should use the
    # statistical covariance only; GetEmatrixTotal includes systematics and so
    # under-sizes the pull. Both are written so #38 can use the right one.
    print(f"\n{'=' * 72}")
    print(f"Unfolded vs truth per gen bin   [truth = {truth_label}]:")
    print(f"  {'Bin':<4} {'Label':<26} {'Unfolded':>12} {'Err':>10} {'Truth':>12} {'Pull':>7}")
    print(f"  {'-' * 72}")
    for i in range(n_gen_bins):
        value = h_unfolded.GetBinContent(i + 1)
        err = np.sqrt(max(0.0, h_cov_stat.GetBinContent(i + 1, i + 1)))
        truth = h_truth.GetBinContent(i + 1)
        pull = (value - truth) / err if err > 0 else 0.0
        label = h_unfolded.GetXaxis().GetBinLabel(i + 1)
        print(f"  {i + 1:<4} {label:<26} {value:>12,.1f} {err:>10,.1f} "
              f"{truth:>12,.1f} {pull:>7.3f}")

    # --- A_C from the unfolded vector (issue #39) ---
    x, cov_stat, cov_total = result["x"], result["cov_stat"], result["cov_total"]

    ac, ac_cov_stat = asymmetry.propagate(x, cov_stat, gen_edges)
    _, ac_cov_total = asymmetry.propagate(x, cov_total, gen_edges)

    # Per-source breakdown: TUnfold gives the shift of the unfolded spectrum
    # for each source, and the induced shift in A_C is J dx to first order.
    shifts = {}
    for source in added:
        h_delta = unfold.GetDeltaSysSource(source, f"delta_{source}")
        if not h_delta:
            continue
        h_delta.SetDirectory(0)
        dx = np.array([h_delta.GetBinContent(i + 1) for i in range(n_gen_bins)])
        shifts[source] = asymmetry.propagate_shift(x, dx, gen_edges)

    print(f"\n{'=' * 72}")
    print("Charge asymmetry from the unfolded spectrum:")
    print(asymmetry.format_table(ac, ac_cov_stat, ac_cov_total, gen_edges, shifts))

    # What the correlations are worth: the same numbers with the off-diagonal
    # terms discarded, which is what treating the bins as independent gives.
    _, naive = asymmetry.propagate(x, np.diag(np.diag(cov_total)), gen_edges)
    # Is the measured histogram data-like, or MC with far better statistics?
    # A weighted MC histogram scaled to a lumi yield carries the MC's
    # statistical error, not that of a real dataset of the same size, so its
    # stat uncertainty must not be read as a sensitivity projection.
    integral = h_data.Integral()
    sumw2 = sum(h_data.GetBinError(i + 1) ** 2 for i in range(h_data.GetNbinsX()))
    n_eff = integral ** 2 / sumw2 if sumw2 > 0 else float("nan")
    if np.isfinite(n_eff) and n_eff > 2 * integral:
        factor = np.sqrt(n_eff / integral)
        print(f"\n  WARNING: the measured histogram is MC, not data. Its yield is "
              f"{integral:,.0f}\n  but its errors correspond to {n_eff:,.0f} effective "
              f"entries, so the stat\n  uncertainty above is ~{factor:.1f}x smaller than a "
              f"real {integral:,.0f}-event\n  dataset would give. Scaled: stat(A_C) ~ "
              f"{asymmetry.errors(ac_cov_stat)[-1] * factor:.4f} inclusive. "
              f"Do not quote\n  the closure number as a sensitivity projection (issue #38).")

    print(f"\n  inclusive A_C = {ac[-1]:+.5f} +- {asymmetry.errors(ac_cov_total)[-1]:.5f}"
          f"   (rho_avg = {unfold.GetRhoAvg():.3f})")
    print(f"  discarding the off-diagonal covariance would give "
          f"+- {asymmetry.errors(naive)[-1]:.5f} instead")

    plots.truth_vs_unfolded(h_truth, h_unfolded, plotdir, args.era, tau, n_gen_mtt)
    plots.asymmetry_vs_mtt(ac, asymmetry.errors(ac_cov_stat),
                           asymmetry.errors(ac_cov_total),
                           gen_edges, plotdir, args.era)
    if args.tau is None:
        plots.lcurve(l_curve, best_index, tau, plotdir)
    plots.response_matrix(h_matrix, plotdir, args.era)
    plots.correlation_matrix(h_cov_total, plotdir, args.era)

    out_path = outdir / "unfolding_results.root"
    fout = ROOT.TFile(str(out_path), "RECREATE")
    h_unfolded.Write("h_unfolded")
    h_truth.Write("h_gen_truth")
    h_data.Write("h_reco_measured")
    h_cov_total.Write("h_cov_total")
    h_cov_stat.Write("h_cov_stat")
    if args.tau is None:
        l_curve.Write("lcurve")
    ROOT.TNamed("asymmetry", json.dumps({
        "labels": asymmetry.labels(gen_edges),
        "A_C": ac.tolist(),
        "stat": asymmetry.errors(ac_cov_stat).tolist(),
        "total": asymmetry.errors(ac_cov_total).tolist(),
        "cov_stat": ac_cov_stat.tolist(),
        "cov_total": ac_cov_total.tolist(),
        "per_source": {k: v.tolist() for k, v in shifts.items()},
    })).Write()
    ROOT.TNamed("provenance", json.dumps({
        **cfgmod.provenance(cfg, "unfold.py"),
        "era": args.era, "tag": args.tag, "tau": tau,
        "systematics": added,
        "A_C_inclusive": float(ac[-1]),
        "A_C_inclusive_total_err": float(asymmetry.errors(ac_cov_total)[-1]),
        "rho_avg": unfold.GetRhoAvg(),
        "chi2A": unfold.GetChi2A(),
        "tunfold_version": str(ROOT.TUnfold.GetTUnfoldVersion()),
    })).Write()
    fout.Close()
    f_in.Close()

    print(f"\nWrote {out_path}")
    print(f"Figures in {plotdir}")


if __name__ == "__main__":
    main()

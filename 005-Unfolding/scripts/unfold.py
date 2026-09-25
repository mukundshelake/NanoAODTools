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

  TODO(#39) A_C is never computed from the unfolded vector. It needs to be,
            with its uncertainty propagated through the full covariance rather
            than assuming independent bins.
"""

import argparse
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import binning  # noqa: E402
import config as cfgmod  # noqa: E402
import plots  # noqa: E402
from tunfold_env import ROOT, load_tunfold  # noqa: E402


def add_systematics(unfold, f_resp, sources):
    """Register each response-matrix variation as a TUnfoldSys shift."""
    added = []
    for source in sources:
        h_up = f_resp.Get(f"response_matrix_{source}Up")
        h_down = f_resp.Get(f"response_matrix_{source}Down")
        if not h_up or not h_down:
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
        print(f"  [ok]   {source}")
    return added


def main():
    parser = argparse.ArgumentParser(description="TUnfold unfolding for the ttbar charge asymmetry")
    parser.add_argument("--era", required=True)
    parser.add_argument("--tag", default="Dump")
    parser.add_argument("--config", default=None)
    parser.add_argument("--inputs", default=None, help="override unfolding_inputs.root")
    parser.add_argument("--outdir", default=None)
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
    h_truth = f_in.Get("h_gen_truth")
    h_matrix = f_in.Get("response_matrix_nominal")
    for obj, name in [(h_data, "h_reco_measured"), (h_truth, "h_gen_truth"),
                      (h_matrix, "response_matrix_nominal")]:
        if not obj:
            raise SystemExit(f"'{name}' not found in {inputs}")
        obj.SetDirectory(0)

    # TODO(#35): see the module docstring.
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
        raise SystemExit(
            f"SetInput failed (status={status}) -- the measured histogram binning "
            "does not match the response matrix Y axis"
        )

    print("\nSystematics:")
    added = add_systematics(unfold, f_in, list(cfg["systematics"]))

    if args.tau is not None:
        unfold.DoUnfold(args.tau)
        tau, best_index, l_curve = args.tau, 0, ROOT.TGraph()
        print(f"\nFixed tau = {tau:.8f}")
    else:
        print("\nScanning L-curve...")
        l_curve = ROOT.TGraph()
        best_index = unfold.ScanLcurve(100, 0.0, 0.0, l_curve,
                                       ROOT.TSpline3(), ROOT.TSpline3())
        tau = unfold.GetTau()
        print(f"  best scan point : {best_index}")
        print(f"  optimal tau     : {tau:.8f}")
    print(f"  chi2(A)         : {unfold.GetChi2A():.4f}")
    print(f"  chi2(L)         : {unfold.GetChi2L():.4f}")
    print(f"  rho avg         : {unfold.GetRhoAvg():.4f}")

    h_unfolded = unfold.GetOutput("h_unfolded")
    h_unfolded.SetDirectory(0)
    for i in range(1, n_gen_bins + 1):
        h_unfolded.GetXaxis().SetBinLabel(i, h_truth.GetXaxis().GetBinLabel(i))

    h_cov_total = unfold.GetEmatrixTotal("h_cov_total")
    h_cov_total.SetDirectory(0)
    h_cov_stat = unfold.GetEmatrixInput("h_cov_stat")
    h_cov_stat.SetDirectory(0)

    # TODO(#38): for a closure test on the same MC the pull should use the
    # statistical covariance only; GetEmatrixTotal includes systematics and so
    # under-sizes the pull. Both are written so #38 can use the right one.
    print(f"\n{'=' * 72}")
    print("Unfolded vs truth per gen bin:")
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

    print("\n  TODO(#39): A_C is not computed from this vector yet. It must be, "
          "with the\n  uncertainty propagated through the covariance -- the "
          "unfolded bins are\n  strongly correlated, so treating them as "
          f"independent would misstate it (rho avg = {unfold.GetRhoAvg():.3f}).")

    plots.truth_vs_unfolded(h_truth, h_unfolded, plotdir, args.era, tau, n_gen_mtt)
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
    ROOT.TNamed("provenance", json.dumps({
        **cfgmod.provenance(cfg, "unfold.py"),
        "era": args.era, "tag": args.tag, "tau": tau,
        "systematics": added,
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

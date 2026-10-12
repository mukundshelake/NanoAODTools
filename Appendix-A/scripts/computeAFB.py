#!/usr/bin/env python3
"""
Compute the gen-level forward-backward asymmetry in bins of Mtt from the
Appendix-A output skims (branches written by modules/GenAsymmetryModule.py).

    A_FB = (N+ - N-) / (N+ + N-),   N+/- = events with cos(theta*) >< 0
    sigma(A_FB) = sqrt((1 - A_FB^2) / N)   (binomial, unweighted events)

Writes AFB.json (per category, per Mtt bin + inclusive) and AFB_vs_Mtt.png.
"""

import argparse
import json
import math
import os

import ROOT
ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PROD_TYPE = {"all": None, "qqbar": 1, "qg": 2, "gg": 0}


def afb(n_pos, n_neg):
    n = n_pos + n_neg
    if n == 0:
        return None, None
    a = (n_pos - n_neg) / n
    return a, math.sqrt(max(0.0, 1.0 - a * a) / n)


def load_files(dataset_json):
    with open(dataset_json) as f:
        tree = json.load(f)
    files = []
    for DataMC in tree.values():
        for group in DataMC.values():
            for dataset in group.values():
                files.extend(dataset.keys())
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--json", required=True, help="Appendix-A dataset JSON (from --generateDatasetJSON)")
    parser.add_argument("--outDir", required=True)
    parser.add_argument("--mttBins", type=float, nargs="+", required=True)
    parser.add_argument("--categories", nargs="+", default=list(PROD_TYPE))
    parser.add_argument("--label", default="", help="Text for the plot title (e.g. era)")
    args = parser.parse_args()

    files = load_files(args.json)
    if not files:
        raise SystemExit(f"No files listed in {args.json}")
    os.makedirs(args.outDir, exist_ok=True)

    edges = args.mttBins
    bins = [(edges[i], edges[i + 1]) for i in range(len(edges) - 1)]

    df = ROOT.RDataFrame("Events", files)
    df = df.Filter("Gen_prodType >= 0", "tops and partons identified")

    # Book every count up front so RDataFrame does one pass over the files.
    counts = {}
    for variable in ("Gen_cosThetaStar", "Gen_cosThetaStar_boost"):
        for cat in args.categories:
            code = PROD_TYPE[cat]
            d = df if code is None else df.Filter(f"Gen_prodType == {code}")
            for label, (lo, hi) in [("inclusive", (None, None))] + [(f"{lo:g}-{hi:g}", (lo, hi)) for lo, hi in bins]:
                sel = d if lo is None else d.Filter(f"Gen_Mtt >= {lo} && Gen_Mtt < {hi}")
                counts[(variable, cat, label)] = (
                    sel.Filter(f"{variable} > 0").Count(),
                    sel.Filter(f"{variable} < 0").Count(),
                )
    n_total = df.Count()

    results = {"files": len(files), "events": None, "mttBins": edges, "cosThetaStar": {}, "cosThetaStar_boost": {}}
    for (variable, cat, label), (pos, neg) in counts.items():
        n_pos, n_neg = int(pos.GetValue()), int(neg.GetValue())
        a, err = afb(n_pos, n_neg)
        key = "cosThetaStar" if variable == "Gen_cosThetaStar" else "cosThetaStar_boost"
        results[key].setdefault(cat, {})[label] = {"Npos": n_pos, "Nneg": n_neg, "AFB": a, "AFB_err": err}
    results["events"] = int(n_total.GetValue())

    out_json = os.path.join(args.outDir, "AFB.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2)

    # Printed summary
    print(f"Events with prodType >= 0: {results['events']}  ({len(files)} files)")
    for cat in args.categories:
        print(f"\n[{cat}]  reference: true incoming quark (boost proxy in brackets)")
        for label, r in results["cosThetaStar"][cat].items():
            rb = results["cosThetaStar_boost"][cat][label]
            if r["AFB"] is None:
                print(f"  {label:>12}: no events")
                continue
            print(f"  {label:>12}: N={r['Npos'] + r['Nneg']:>9}  A_FB = {r['AFB']:+.4f} +- {r['AFB_err']:.4f}"
                  f"   [{rb['AFB']:+.4f} +- {rb['AFB_err']:.4f}]")

    # Plot A_FB vs Mtt, one series per category
    centers = [0.5 * (lo + hi) for lo, hi in bins]
    halfw = [0.5 * (hi - lo) for lo, hi in bins]
    fig, ax = plt.subplots(figsize=(7, 5))
    n_cat = len(args.categories)
    for i, cat in enumerate(args.categories):
        rows = [results["cosThetaStar"][cat][f"{lo:g}-{hi:g}"] for lo, hi in bins]
        xs, ys, es, ws = [], [], [], []
        for c, w, r in zip(centers, halfw, rows):
            if r["AFB"] is None:
                continue
            offset = (i - (n_cat - 1) / 2) * 0.06 * w
            xs.append(c + offset); ys.append(r["AFB"]); es.append(r["AFB_err"]); ws.append(w)
        ax.errorbar(xs, ys, yerr=es, xerr=ws, fmt="o", capsize=3, label=cat)
    ax.axhline(0, color="grey", lw=0.8)
    ax.set_xlabel(r"gen $m_{t\bar{t}}$ [GeV]")
    ax.set_ylabel(r"gen $A_{FB}$ ($\cos\theta^*$)")
    ax.set_title(f"Gen-level $A_{{FB}}$ vs $m_{{t\\bar{{t}}}}$ {args.label}".strip())
    ax.legend()
    fig.tight_layout()
    out_png = os.path.join(args.outDir, "AFB_vs_Mtt.png")
    fig.savefig(out_png, dpi=150)
    print(f"\nWrote {out_json}\nWrote {out_png}")


if __name__ == "__main__":
    main()

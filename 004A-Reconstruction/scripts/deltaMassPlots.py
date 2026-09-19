#!/usr/bin/env python3
"""Plot reconstructed-minus-generator top-mass distributions and fit quality.

The input JSON is the dataset map produced by ``generateDatasetJSON.py``.
Only files below datasets whose name contains ``ttbar_SemiLeptonic`` are
processed -- the generator top mass is needed, so this is MC-only.

Five PNG files are written:

* ``deltaMass_hadronic.png`` / ``deltaMass_leptonic.png`` -- the residuals
  m_t^reco - m_t^gen for each top candidate.
* ``pgof.png`` -- the fit goodness-of-fit probability Pgof, split into
  correctly and incorrectly reconstructed events.
* ``pgof_purity.png`` -- differential purity s/(s+b) per Pgof bin.
* ``pgof_efficiency.png`` -- cumulative signal efficiency and purity for a
  ``Pgof > cut`` selection, i.e. what a Pgof cut actually buys you.

"Signal" here means *correctly reconstructed*: both top candidates within
``--matchWindow`` GeV of their generator counterpart. Everything else is
combinatorial background -- a fit that converged onto the wrong jet
assignment. This is a truth-matching definition, available only because these
are MC files with the generator tops in them.
"""

import argparse
import json
import logging
import os
from pathlib import Path

import numpy as np
import ROOT


LOG = logging.getLogger("deltaMassPlots")
LAST_COPY_BIT = 1 << 13  # NanoAOD GenPart_statusFlags: isLastCopy


def _root_files(node, tree_name="Events"):
    """Yield ``(file, tree)`` pairs from any nested JSON structure."""
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(key, str) and key.endswith(".root"):
                yield key, value if isinstance(value, str) else tree_name
            else:
                yield from _root_files(value, tree_name)
    elif isinstance(node, list):
        for value in node:
            yield from _root_files(value, tree_name)


def find_ttbar_files(dataset_json):
    """Return unique ROOT files belonging to semi-leptonic ttbar datasets."""
    with open(dataset_json, encoding="utf-8") as handle:
        payload = json.load(handle)

    found = {}

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if "ttbar_SemiLeptonic" in str(key):
                    for filename, tree in _root_files(value):
                        found[filename] = tree
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(payload)
    return list(found.items())


def _last_copy_mass(pdg_ids, status_flags, masses, pdg_id):
    """Find the mass of the last-copy top/antitop, or return ``None``."""
    for index, (particle_id, flags) in enumerate(zip(pdg_ids, status_flags)):
        if abs(int(particle_id)) != 6 or int(particle_id) != pdg_id:
            continue
        if int(flags) & LAST_COPY_BIT:
            return float(masses[index])
    return None


def collect_delta_masses(files):
    """Read input files and return ``(hadronic_deltas, leptonic_deltas, pgof)``.

    The three lists are parallel: entry *i* of each describes the same event.
    """
    hadronic, leptonic, pgof = [], [], []
    counters = {"files": 0, "events": 0, "used": 0}

    for filename, tree_name in files:
        root_file = ROOT.TFile.Open(filename, "READ")
        if not root_file or root_file.IsZombie():
            LOG.warning("Cannot open ROOT file: %s", filename)
            continue
        tree = root_file.Get(tree_name)
        if not tree:
            LOG.warning("Tree %r not found in %s", tree_name, filename)
            root_file.Close()
            continue

        counters["files"] += 1
        required = (
            "GenPart_pdgId", "GenPart_statusFlags", "GenPart_mass",
            "Top_had_mass", "Top_lep_mass", "Pgof",
        )
        branches = {branch.GetName() for branch in tree.GetListOfBranches()}
        charge_branch = next(
            (name for name in ("SelMuon_charge", "selMuon_charge") if name in branches),
            None,
        )
        missing = [name for name in required if name not in branches]
        if charge_branch is None:
            missing.append("SelMuon_charge (or selMuon_charge)")
        if missing:
            LOG.warning("Skipping %s; missing branches: %s", filename, ", ".join(missing))
            root_file.Close()
            continue

        for event in tree:
            counters["events"] += 1
            top_mass = _last_copy_mass(
                event.GenPart_pdgId, event.GenPart_statusFlags,
                event.GenPart_mass, 6
            )
            antitop_mass = _last_copy_mass(
                event.GenPart_pdgId, event.GenPart_statusFlags,
                event.GenPart_mass, -6
            )
            if top_mass is None or antitop_mass is None:
                continue

            charge = float(getattr(event, charge_branch))
            reco_had = float(event.Top_had_mass)
            reco_lep = float(event.Top_lep_mass)
            if charge > 0:
                # Positive muon comes from t -> b W+ -> b mu+ nu.
                had_gen, lep_gen = antitop_mass, top_mass
            elif charge < 0:
                had_gen, lep_gen = top_mass, antitop_mass
            else:
                continue

            # -1 is the sentinel written by RecoModule when no fit exists.
            if reco_had >= 0 and reco_lep >= 0:
                hadronic.append(reco_had - had_gen)
                leptonic.append(reco_lep - lep_gen)
                pgof.append(float(event.Pgof))
                counters["used"] += 1

        root_file.Close()

    LOG.info("Read %d files and %d events; filled %d events", counters["files"],
             counters["events"], counters["used"])
    return hadronic, leptonic, pgof


def make_plot(values, output_path, title, color, bins, xmin, xmax):
    """Create and save one ROOT histogram."""
    histogram = ROOT.TH1D("deltaMass", title, bins, xmin, xmax)
    histogram.SetDirectory(0)
    histogram.SetLineColor(color)
    histogram.SetLineWidth(2)
    histogram.SetFillColorAlpha(color, 0.25)
    histogram.GetXaxis().SetTitle("m_{t}^{reco} - m_{t}^{gen} [GeV]")
    histogram.GetYaxis().SetTitle("Events")
    for value in values:
        histogram.Fill(value)

    canvas = ROOT.TCanvas("canvas", title, 800, 650)
    canvas.SetGrid()
    histogram.Draw("HIST")
    canvas.SaveAs(str(output_path))
    canvas.Close()


def classify_signal(hadronic, leptonic, match_window):
    """Tag each event as correctly reconstructed (True) or combinatorial.

    Both top candidates must land within ``match_window`` GeV of their
    generator counterpart. Requiring both, rather than just the hadronic one,
    matters because the equal-top-mass term in the fit ties the two candidates
    together -- a wrong jet assignment typically pulls both, and an event where
    only one matches is not a clean reconstruction.
    """
    had = np.asarray(hadronic, dtype=float)
    lep = np.asarray(leptonic, dtype=float)
    return (np.abs(had) < match_window) & (np.abs(lep) < match_window)


def make_pgof_plot(pgof, is_signal, output_path, bins, match_window):
    """Pgof distribution, split into correctly/incorrectly reconstructed.

    Plotted linearly on [0, 1]: Pgof = exp(-Chi2/2) is a goodness-of-fit
    probability, so correct fits populate the whole range while wrong jet
    assignments pile up against zero. (Note Pgof underflows the float32 branch
    to exactly 0 for Chi2 above ~175, which lands in the first bin -- harmless
    here, but it is why this is not plotted in log(Pgof).)
    """
    values = np.asarray(pgof, dtype=float)
    title = f"Fit goodness-of-fit probability (match window {match_window:g} GeV)"

    total = ROOT.TH1D("pgof_total", title, bins, 0.0, 1.0)
    signal = ROOT.TH1D("pgof_signal", title, bins, 0.0, 1.0)
    background = ROOT.TH1D("pgof_background", title, bins, 0.0, 1.0)
    for histogram, color in (
        (total, ROOT.kBlack),
        (signal, ROOT.kAzure + 1),
        (background, ROOT.kRed + 1),
    ):
        histogram.SetDirectory(0)
        histogram.SetLineColor(color)
        histogram.SetLineWidth(2)
        histogram.GetXaxis().SetTitle("P_{gof}")
        histogram.GetYaxis().SetTitle("Events")
    signal.SetFillColorAlpha(ROOT.kAzure + 1, 0.25)
    background.SetFillColorAlpha(ROOT.kRed + 1, 0.25)

    for value, tag in zip(values, is_signal):
        total.Fill(value)
        (signal if tag else background).Fill(value)

    canvas = ROOT.TCanvas("canvas_pgof", title, 800, 650)
    canvas.SetGrid()
    canvas.SetLogy()
    total.Draw("HIST")
    signal.Draw("HIST SAME")
    background.Draw("HIST SAME")

    # Kept clear of the top-right stats box that gStyle.SetOptStat leaves
    # there; the log-y U shape leaves the middle of the frame empty.
    legend = ROOT.TLegend(0.34, 0.60, 0.80, 0.76)
    legend.AddEntry(total, "All reconstructed events", "l")
    legend.AddEntry(signal, "Correctly reconstructed (s)", "lf")
    legend.AddEntry(background, "Combinatorial (b)", "lf")
    legend.Draw()

    canvas.SaveAs(str(output_path))
    canvas.Close()


def make_purity_plot(pgof, is_signal, output_path, bins, match_window):
    """Differential purity s/(s+b) in each Pgof bin."""
    values = np.asarray(pgof, dtype=float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    sig, _ = np.histogram(values[is_signal], bins=edges)
    bkg, _ = np.histogram(values[~is_signal], bins=edges)
    total = sig + bkg

    centres = 0.5 * (edges[:-1] + edges[1:])
    graph = ROOT.TGraphErrors(len(centres))
    for index, (centre, s, n) in enumerate(zip(centres, sig, total)):
        if n == 0:
            # No events in this bin -- purity is undefined, not zero. Park the
            # point off-scale so it is dropped rather than drawn at 0.
            graph.SetPoint(index, centre, -1.0)
            graph.SetPointError(index, 0.0, 0.0)
            continue
        purity = s / n
        # Binomial error on the ratio.
        graph.SetPoint(index, centre, purity)
        graph.SetPointError(index, 0.0, np.sqrt(purity * (1.0 - purity) / n))

    title = (f"Purity per P_{{gof}} bin (match window {match_window:g} GeV);"
             "P_{gof};s / (s+b)")
    graph.SetTitle(title)
    graph.SetMarkerStyle(20)
    graph.SetMarkerSize(0.8)
    graph.SetMarkerColor(ROOT.kAzure + 1)
    graph.SetLineColor(ROOT.kAzure + 1)

    canvas = ROOT.TCanvas("canvas_purity", "purity", 800, 650)
    canvas.SetGrid()
    graph.Draw("AP")
    graph.GetXaxis().SetLimits(0.0, 1.0)
    graph.GetHistogram().SetMinimum(0.0)
    graph.GetHistogram().SetMaximum(1.0)

    inclusive = sig.sum() / total.sum() if total.sum() else 0.0
    line = ROOT.TLine(0.0, inclusive, 1.0, inclusive)
    line.SetLineColor(ROOT.kGray + 2)
    line.SetLineStyle(2)
    line.Draw()

    legend = ROOT.TLegend(0.40, 0.18, 0.88, 0.30)
    legend.AddEntry(graph, "Purity in bin", "lp")
    legend.AddEntry(line, f"Inclusive purity = {inclusive:.3f}", "l")
    legend.Draw()

    canvas.SaveAs(str(output_path))
    canvas.Close()
    return inclusive


def make_efficiency_plot(pgof, is_signal, output_path, bins, match_window):
    """Signal efficiency and purity for a ``Pgof > cut`` selection.

    This is the plot to read when choosing a cut: at each threshold it shows
    what fraction of correctly reconstructed events survives, and how pure the
    surviving sample is.
    """
    values = np.asarray(pgof, dtype=float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    sig, _ = np.histogram(values[is_signal], bins=edges)
    bkg, _ = np.histogram(values[~is_signal], bins=edges)

    # Events passing "Pgof > edge" = everything in this bin and above, so a
    # reverse cumulative sum evaluated at the lower edge of each bin.
    sig_pass = np.concatenate([sig[::-1].cumsum()[::-1], [0.0]])
    bkg_pass = np.concatenate([bkg[::-1].cumsum()[::-1], [0.0]])
    sig_total = sig.sum()

    # Stop at the last bin's lower edge: a "Pgof > 1.0" cut keeps nothing, so
    # that point would drag both curves to zero -- efficiency legitimately, but
    # purity only because 0/0 was floored to 0, which reads as the selection
    # going pure background rather than empty.
    cuts = edges[:-1]
    eff_graph = ROOT.TGraph(len(cuts))
    pur_graph = ROOT.TGraph(len(cuts))
    for index, edge in enumerate(cuts):
        passing = sig_pass[index] + bkg_pass[index]
        eff_graph.SetPoint(index, edge,
                           sig_pass[index] / sig_total if sig_total else 0.0)
        pur_graph.SetPoint(index, edge,
                           sig_pass[index] / passing if passing else 0.0)

    title = (f"P_{{gof}} cut performance (match window {match_window:g} GeV);"
             "P_{gof} cut;Fraction")
    eff_graph.SetTitle(title)
    eff_graph.SetLineColor(ROOT.kAzure + 1)
    eff_graph.SetLineWidth(3)
    pur_graph.SetLineColor(ROOT.kRed + 1)
    pur_graph.SetLineWidth(3)

    canvas = ROOT.TCanvas("canvas_eff", "efficiency", 800, 650)
    canvas.SetGrid()
    eff_graph.Draw("AL")
    eff_graph.GetXaxis().SetLimits(0.0, 1.0)
    eff_graph.GetHistogram().SetMinimum(0.0)
    eff_graph.GetHistogram().SetMaximum(1.05)
    pur_graph.Draw("L SAME")

    legend = ROOT.TLegend(0.40, 0.18, 0.88, 0.32)
    legend.AddEntry(eff_graph, "Signal efficiency  s(>cut) / s(total)", "l")
    legend.AddEntry(pur_graph, "Purity  s / (s+b)  above cut", "l")
    legend.Draw()

    canvas.SaveAs(str(output_path))
    canvas.Close()

    # A few reference working points for the log, so the numbers are recorded
    # even when nobody opens the PNG.
    for cut in (0.0, 0.01, 0.05, 0.1, 0.2, 0.5):
        index = int(np.searchsorted(edges, cut, side="left"))
        index = min(index, len(edges) - 1)
        passing = sig_pass[index] + bkg_pass[index]
        LOG.info(
            "Pgof > %.2f : eff = %.4f, purity = %.4f, events kept = %d",
            cut,
            sig_pass[index] / sig_total if sig_total else 0.0,
            sig_pass[index] / passing if passing else 0.0,
            int(passing),
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-j", "--json", required=True, help="Dataset JSON file")
    parser.add_argument("-o", "--outDir", required=True, help="Directory for PNG plots")
    parser.add_argument("--bins", type=int, default=100)
    parser.add_argument("--xmin", type=float, default=-200.0)
    parser.add_argument("--xmax", type=float, default=200.0)
    parser.add_argument("--pgofBins", type=int, default=50,
                        help="Bins across Pgof in [0,1] (default: 50)")
    parser.add_argument("--matchWindow", type=float, default=30.0,
                        help="An event counts as correctly reconstructed when both "
                             "|m_t^reco - m_t^gen| are below this, in GeV (default: 30)")
    args = parser.parse_args()

    if args.bins <= 0 or args.xmin >= args.xmax:
        parser.error("require --bins > 0 and --xmin < --xmax")
    if args.pgofBins <= 0:
        parser.error("require --pgofBins > 0")
    if args.matchWindow <= 0:
        parser.error("require --matchWindow > 0")
    if not os.path.isfile(args.json):
        parser.error(f"JSON file not found: {args.json}")

    ROOT.gROOT.SetBatch(True)
    ROOT.gStyle.SetOptStat(1110)
    ROOT.TH1.SetDefaultSumw2(True)
    os.makedirs(args.outDir, exist_ok=True)

    files = find_ttbar_files(args.json)
    if not files:
        raise RuntimeError("No ttbar_SemiLeptonic ROOT files found in the JSON")
    LOG.info("Found %d ttbar_SemiLeptonic ROOT files", len(files))
    hadronic, leptonic, pgof = collect_delta_masses(files)
    if not hadronic:
        raise RuntimeError("No events survived; nothing to plot")

    out_dir = Path(args.outDir)
    make_plot(hadronic, out_dir / "deltaMass_hadronic.png",
              "Hadronic top mass residual", ROOT.kAzure + 1,
              args.bins, args.xmin, args.xmax)
    make_plot(leptonic, out_dir / "deltaMass_leptonic.png",
              "Leptonic top mass residual", ROOT.kOrange + 7,
              args.bins, args.xmin, args.xmax)

    is_signal = classify_signal(hadronic, leptonic, args.matchWindow)
    LOG.info("Correctly reconstructed: %d of %d events (%.2f%%) within %g GeV",
             int(is_signal.sum()), len(is_signal),
             100.0 * is_signal.mean(), args.matchWindow)

    make_pgof_plot(pgof, is_signal, out_dir / "pgof.png",
                   args.pgofBins, args.matchWindow)
    make_purity_plot(pgof, is_signal, out_dir / "pgof_purity.png",
                     args.pgofBins, args.matchWindow)
    make_efficiency_plot(pgof, is_signal, out_dir / "pgof_efficiency.png",
                         args.pgofBins, args.matchWindow)
    LOG.info("Saved plots in %s", out_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()

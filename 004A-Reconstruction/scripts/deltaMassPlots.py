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

"Signal" here means *the fit picked the right jets*, established at parton
level in two steps.

First, is the event **reconstructible** at all? Both b partons and both
hadronic-W quarks must be matched, one-to-one and within ``--partonDR``, by
the four jets RecoModule actually fits: ``leading/subleadingbJet`` in the b
positions and ``leading/subleadingJet`` on the hadronic W. Events failing
this are lost to acceptance, jet merging or out-of-cone radiation before the
fit runs, and no amount of re-ranking recovers them. Their fraction is the
*physical ceiling* for this permutation set.

Second, among those, did the fit make the one choice it has -- which b-jet
goes hadronic? The light jets are always both assigned to the hadronic W, so
that binary choice is the whole assignment. It is read back by comparing the
fitted ``Top_had`` direction against the two hypotheses ``b + lj + slj``; the
fit rescales jet momenta but keeps their directions, so the hypothesis it
actually used is far closer (median DeltaR 0.09, against 1.7 for the other).

Deliberately *not* used: a window on ``|m_t^reco - m_t^gen|``. The fit pins
m_t to the same 172.5 GeV the generator used, so a mass window partly selects
on the very quantity it is meant to validate -- a wrong assignment that the
constraint has pulled back onto the nominal mass still passes it. Defining
signal this way instead makes it a strict subset of the reconstructible
events, so efficiency against the ceiling cannot exceed 1.
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
# The four jets RecoModule fits, in the slots it fits them into.
JET_NAMES = ("leadingbJet", "subleadingbJet", "leadingJet", "subleadingJet")


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


def _last_copy_index(pdg_ids, status_flags, pdg_id):
    """Index of the last-copy particle with exactly ``pdg_id``, else ``None``."""
    for index, (particle_id, flags) in enumerate(zip(pdg_ids, status_flags)):
        if int(particle_id) != pdg_id:
            continue
        if int(flags) & LAST_COPY_BIT:
            return index
    return None


def _delta_r(eta_one, phi_one, eta_two, phi_two):
    """DeltaR between two directions, with phi wrapped into [-pi, pi]."""
    delta_eta = float(eta_one) - float(eta_two)
    delta_phi = abs(float(phi_one) - float(phi_two))
    if delta_phi > np.pi:
        delta_phi = 2.0 * np.pi - delta_phi
    return float(np.hypot(delta_eta, delta_phi))


def _four_vector(event, name):
    """``(px, py, pz, E)`` for one of the stored jet branches."""
    pt = float(getattr(event, f"{name}_pt"))
    eta = float(getattr(event, f"{name}_eta"))
    phi = float(getattr(event, f"{name}_phi"))
    mass = float(getattr(event, f"{name}_mass"))
    px, py, pz = pt * np.cos(phi), pt * np.sin(phi), pt * np.sinh(eta)
    energy = np.sqrt(max(px * px + py * py + pz * pz + mass * mass, 0.0))
    return np.array([px, py, pz, energy])


def _direction(vector):
    """``(eta, phi)`` of a ``(px, py, pz, E)`` four-vector."""
    transverse = np.hypot(vector[0], vector[1])
    return (float(np.arcsinh(vector[2] / max(transverse, 1e-9))),
            float(np.arctan2(vector[1], vector[0])))


def _has_top_ancestor(index, pdg_ids, mothers, max_steps=80):
    """Walk up the mother chain looking for a top quark."""
    mother = int(mothers[index])
    for _ in range(max_steps):
        if mother < 0 or mother >= len(pdg_ids):
            return False
        if abs(int(pdg_ids[mother])) == 6:
            return True
        mother = int(mothers[mother])
    return False


def gen_parton_indices(event):
    """Return ``(b_had, b_lep, q_one, q_two)`` GenPart indices, or ``None``.

    The hadronic side is identified from the W daughters rather than by
    charge: in a semi-leptonic event exactly one W decays to quarks, so the
    two quarks whose direct mother is a W fix which W -- and therefore which
    top -- is the hadronic one. The b from that same top carries the same
    pdgId sign as the W (t -> b W+, tbar -> bbar W-).

    The b candidates are required to descend from a top. Without that, a b
    from gluon splitting earlier in the record can be picked up instead,
    which quietly matches the wrong jet.
    """
    pdg_ids = event.GenPart_pdgId
    mothers = event.GenPart_genPartIdxMother
    status_flags = event.GenPart_statusFlags
    count = len(pdg_ids)

    w_quarks = []
    for index in range(count):
        if not 1 <= abs(int(pdg_ids[index])) <= 5:
            continue
        mother = int(mothers[index])
        if 0 <= mother < count and abs(int(pdg_ids[mother])) == 24:
            w_quarks.append((index, int(pdg_ids[mother])))

    # Not a clean semi-leptonic event: no hadronic W, or both Ws hadronic.
    if len(w_quarks) != 2 or w_quarks[0][1] != w_quarks[1][1]:
        return None

    (q_one, w_pdg), (q_two, _) = w_quarks
    sign = 1 if w_pdg > 0 else -1

    bottoms = {}
    for index in range(count):
        pdg_id = int(pdg_ids[index])
        if abs(pdg_id) != 5 or not int(status_flags[index]) & LAST_COPY_BIT:
            continue
        if pdg_id in bottoms or not _has_top_ancestor(index, pdg_ids, mothers):
            continue
        bottoms[pdg_id] = index

    b_had, b_lep = bottoms.get(5 * sign), bottoms.get(-5 * sign)
    if b_had is None or b_lep is None:
        return None
    return b_had, b_lep, q_one, q_two


def _pair_matches(partons, jets, parton_dr):
    """Match two partons to two jets one-to-one; return the winning order.

    Returns ``"straight"``, ``"crossed"`` or ``None``. Which of the two jets
    is "leading" carries no truth information, so either order counts -- but
    *which* order won is what identifies the b-jet belonging to the hadronic
    top, so it is returned rather than collapsed to a boolean. When both
    orders are within the cone the closer pairing wins.
    """
    (first_eta, first_phi), (second_eta, second_phi) = partons
    (one_eta, one_phi), (two_eta, two_phi) = jets
    first_one = _delta_r(first_eta, first_phi, one_eta, one_phi)
    first_two = _delta_r(first_eta, first_phi, two_eta, two_phi)
    second_one = _delta_r(second_eta, second_phi, one_eta, one_phi)
    second_two = _delta_r(second_eta, second_phi, two_eta, two_phi)

    straight = first_one < parton_dr and second_two < parton_dr
    crossed = first_two < parton_dr and second_one < parton_dr
    if straight and crossed:
        return "straight" if first_one + second_two <= first_two + second_one \
            else "crossed"
    if straight:
        return "straight"
    return "crossed" if crossed else None


def match_partons_to_jets(event, partons, parton_dr):
    """Return ``(reconstructible, hadronic_b_jet_name)``.

    RecoModule only ever fits ``leading/subleadingbJet`` into the two b slots
    and ``leading/subleadingJet`` into the hadronic W, so the four partons
    must be covered by those four jets. Requiring the b partons to match the
    b-jets and the W quarks to match the light jets -- rather than any four
    jets -- is what makes this the ceiling for *this* permutation set.
    """
    b_had, b_lep, q_one, q_two = partons
    eta, phi = event.GenPart_eta, event.GenPart_phi

    jets = {}
    for name in JET_NAMES:
        if float(getattr(event, f"{name}_pt")) < 0:
            return False, None  # -1 sentinel: the jet is not there at all.
        jets[name] = (float(getattr(event, f"{name}_eta")),
                      float(getattr(event, f"{name}_phi")))

    b_order = _pair_matches(
        ((eta[b_had], phi[b_had]), (eta[b_lep], phi[b_lep])),
        (jets["leadingbJet"], jets["subleadingbJet"]),
        parton_dr,
    )
    q_order = _pair_matches(
        ((eta[q_one], phi[q_one]), (eta[q_two], phi[q_two])),
        (jets["leadingJet"], jets["subleadingJet"]),
        parton_dr,
    )
    if b_order is None or q_order is None:
        return False, None
    # "straight" put b_had on the leading b-jet.
    return True, "leadingbJet" if b_order == "straight" else "subleadingbJet"


def fit_chose_hadronic_b(event, true_b_jet):
    """Did the fit put ``true_b_jet`` on the hadronic top?

    The two hypotheses differ only in the b-jet, so comparing the fitted
    Top_had direction against ``b + lj + slj`` for each identifies the one
    the fit used. The fit rescales jet momenta within their resolutions but
    does not change their directions, so the two are well separated.
    """
    other = ("subleadingbJet" if true_b_jet == "leadingbJet"
             else "leadingbJet")
    light = _four_vector(event, "leadingJet") + _four_vector(event, "subleadingJet")
    true_eta, true_phi = _direction(_four_vector(event, true_b_jet) + light)
    other_eta, other_phi = _direction(_four_vector(event, other) + light)

    top_eta, top_phi = float(event.Top_had_eta), float(event.Top_had_phi)
    return (_delta_r(top_eta, top_phi, true_eta, true_phi)
            < _delta_r(top_eta, top_phi, other_eta, other_phi))


def collect_delta_masses(files, parton_dr):
    """Read input files and return one record per usable event.

    Returns a dict of parallel arrays -- ``hadronic``/``leptonic`` mass
    residuals, ``pgof``, ``reconstructible`` (the physical ceiling) and
    ``correct`` (reconstructible *and* the fit picked the right hadronic b).
    """
    hadronic, leptonic, pgof = [], [], []
    reconstructible, correct = [], []
    counters = {"files": 0, "events": 0, "used": 0, "no_partons": 0}

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
            "GenPart_eta", "GenPart_phi", "GenPart_genPartIdxMother",
            "Top_had_mass", "Top_had_eta", "Top_had_phi",
            "Top_lep_mass", "Pgof",
        ) + tuple(f"{name}_{field}"
                  for name in JET_NAMES
                  for field in ("pt", "eta", "phi", "mass"))
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
            top_index = _last_copy_index(
                event.GenPart_pdgId, event.GenPart_statusFlags, 6)
            antitop_index = _last_copy_index(
                event.GenPart_pdgId, event.GenPart_statusFlags, -6)
            if top_index is None or antitop_index is None:
                continue

            charge = float(getattr(event, charge_branch))
            reco_had = float(event.Top_had_mass)
            reco_lep = float(event.Top_lep_mass)
            if charge > 0:
                # Positive muon comes from t -> b W+ -> b mu+ nu.
                had_index, lep_index = antitop_index, top_index
            elif charge < 0:
                had_index, lep_index = top_index, antitop_index
            else:
                continue

            # -1 is the sentinel written by RecoModule when no fit exists.
            if reco_had < 0 or reco_lep < 0:
                continue

            gen_mass = event.GenPart_mass
            hadronic.append(reco_had - float(gen_mass[had_index]))
            leptonic.append(reco_lep - float(gen_mass[lep_index]))
            pgof.append(float(event.Pgof))

            partons = gen_parton_indices(event)
            if partons is None:
                # Not a clean semi-leptonic decay chain, so the ceiling is
                # undefined for it; counted separately rather than being
                # silently folded in as "not reconstructible".
                counters["no_partons"] += 1
                can_reconstruct, true_b_jet = False, None
            else:
                can_reconstruct, true_b_jet = match_partons_to_jets(
                    event, partons, parton_dr)
            reconstructible.append(can_reconstruct)
            correct.append(can_reconstruct
                           and fit_chose_hadronic_b(event, true_b_jet))
            counters["used"] += 1

        root_file.Close()

    LOG.info("Read %d files and %d events; filled %d events", counters["files"],
             counters["events"], counters["used"])
    if counters["no_partons"]:
        LOG.warning("%d of %d events had no clean semi-leptonic parton chain; "
                    "they count against the ceiling",
                    counters["no_partons"], counters["used"])
    return {
        "hadronic": hadronic,
        "leptonic": leptonic,
        "pgof": pgof,
        "reconstructible": np.asarray(reconstructible, dtype=bool),
        "correct": np.asarray(correct, dtype=bool),
    }


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


def make_pgof_plot(pgof, is_signal, output_path, bins, parton_dr):
    """Pgof distribution, split into correctly/incorrectly reconstructed.

    Plotted linearly on [0, 1]: Pgof = exp(-Chi2/2) is a goodness-of-fit
    probability, so correct fits populate the whole range while wrong jet
    assignments pile up against zero. (Note Pgof underflows the float32 branch
    to exactly 0 for Chi2 above ~175, which lands in the first bin -- harmless
    here, but it is why this is not plotted in log(Pgof).)
    """
    values = np.asarray(pgof, dtype=float)
    title = f"Fit goodness-of-fit probability (parton #DeltaR < {parton_dr:g})"

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


def make_purity_plot(pgof, is_signal, output_path, bins, parton_dr):
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

    title = (f"Purity per P_{{gof}} bin (parton #DeltaR < {parton_dr:g});"
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


def make_efficiency_plot(pgof, is_signal, output_path, bins, parton_dr,
                         n_reconstructible=0):
    """Signal efficiency and purity for a ``Pgof > cut`` selection.

    This is the plot to read when choosing a cut: at each threshold it shows
    what fraction of correctly reconstructed events survives, and how pure the
    surviving sample is.

    When ``n_reconstructible`` is given, a second efficiency curve is drawn
    against it. That is the honest denominator for judging the fit: dividing
    by the events it actually reconstructed correctly hides how many it never
    had a chance at, so a fit can look efficient while recovering only a
    fraction of what the jet collection allows.
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
    ceiling_graph = ROOT.TGraph(len(cuts)) if n_reconstructible else None
    for index, edge in enumerate(cuts):
        passing = sig_pass[index] + bkg_pass[index]
        eff_graph.SetPoint(index, edge,
                           sig_pass[index] / sig_total if sig_total else 0.0)
        pur_graph.SetPoint(index, edge,
                           sig_pass[index] / passing if passing else 0.0)
        if ceiling_graph is not None:
            ceiling_graph.SetPoint(index, edge,
                                   sig_pass[index] / n_reconstructible)

    title = (f"P_{{gof}} cut performance (parton #DeltaR < {parton_dr:g});"
             "P_{gof} cut;Fraction")
    eff_graph.SetTitle(title)
    eff_graph.SetLineColor(ROOT.kAzure + 1)
    eff_graph.SetLineWidth(3)
    pur_graph.SetLineColor(ROOT.kRed + 1)
    pur_graph.SetLineWidth(3)
    if ceiling_graph is not None:
        ceiling_graph.SetLineColor(ROOT.kGreen + 2)
        ceiling_graph.SetLineWidth(3)
        ceiling_graph.SetLineStyle(2)

    canvas = ROOT.TCanvas("canvas_eff", "efficiency", 800, 650)
    canvas.SetGrid()
    eff_graph.Draw("AL")
    eff_graph.GetXaxis().SetLimits(0.0, 1.0)
    eff_graph.GetHistogram().SetMinimum(0.0)
    eff_graph.GetHistogram().SetMaximum(1.05)
    pur_graph.Draw("L SAME")
    if ceiling_graph is not None:
        ceiling_graph.Draw("L SAME")

    legend = ROOT.TLegend(0.40, 0.18, 0.88, 0.36)
    legend.AddEntry(eff_graph, "Signal efficiency  s(>cut) / s(total)", "l")
    legend.AddEntry(pur_graph, "Purity  s / (s+b)  above cut", "l")
    if ceiling_graph is not None:
        legend.AddEntry(ceiling_graph,
                        "Efficiency vs reconstructible events", "l")
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
            "Pgof > %.2f : eff = %.4f, eff/ceiling = %.4f, purity = %.4f, "
            "events kept = %d",
            cut,
            sig_pass[index] / sig_total if sig_total else 0.0,
            sig_pass[index] / n_reconstructible if n_reconstructible else 0.0,
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
    parser.add_argument("--partonDR", type=float, default=0.4,
                        help="DeltaR for matching the b and W partons to the four "
                             "jets the fit uses. Sets both the physical ceiling "
                             "and which events count as correctly reconstructed "
                             "(default: 0.4, the AK4 cone)")
    args = parser.parse_args()

    if args.bins <= 0 or args.xmin >= args.xmax:
        parser.error("require --bins > 0 and --xmin < --xmax")
    if args.pgofBins <= 0:
        parser.error("require --pgofBins > 0")
    if args.partonDR <= 0:
        parser.error("require --partonDR > 0")
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
    events = collect_delta_masses(files, args.partonDR)
    hadronic, leptonic, pgof = (
        events["hadronic"], events["leptonic"], events["pgof"])
    if not hadronic:
        raise RuntimeError("No events survived; nothing to plot")

    out_dir = Path(args.outDir)
    make_plot(hadronic, out_dir / "deltaMass_hadronic.png",
              "Hadronic top mass residual", ROOT.kAzure + 1,
              args.bins, args.xmin, args.xmax)
    make_plot(leptonic, out_dir / "deltaMass_leptonic.png",
              "Leptonic top mass residual", ROOT.kOrange + 7,
              args.bins, args.xmin, args.xmax)

    is_signal = events["correct"]
    reconstructible = events["reconstructible"]
    n_reconstructible = int(reconstructible.sum())

    LOG.info("Physical ceiling: %d of %d events (%.2f%%) have all four partons "
             "matched within DeltaR < %g by the jets the fit is given",
             n_reconstructible, len(reconstructible),
             100.0 * reconstructible.mean(), args.partonDR)
    LOG.info("Correctly reconstructed: %d of %d events (%.2f%%)",
             int(is_signal.sum()), len(is_signal), 100.0 * is_signal.mean())
    if n_reconstructible:
        LOG.info("Fit recovers %.2f%% of the reconstructible events; the "
                 "remaining %.2f%% of all events were never reconstructible",
                 100.0 * is_signal.sum() / n_reconstructible,
                 100.0 * (1.0 - reconstructible.mean()))

    make_pgof_plot(pgof, is_signal, out_dir / "pgof.png",
                   args.pgofBins, args.partonDR)
    make_purity_plot(pgof, is_signal, out_dir / "pgof_purity.png",
                     args.pgofBins, args.partonDR)
    make_efficiency_plot(pgof, is_signal, out_dir / "pgof_efficiency.png",
                         args.pgofBins, args.partonDR, n_reconstructible)
    LOG.info("Saved plots in %s", out_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()

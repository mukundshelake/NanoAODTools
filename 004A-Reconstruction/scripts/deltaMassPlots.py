#!/usr/bin/env python3
"""Plot reconstructed-minus-generator top-mass distributions.

The input JSON is the dataset map produced by ``generateDatasetJSON.py``.
Only files below datasets whose name contains ``ttbar_SemiLeptonic`` are
processed.  Two PNG files are written: one for the hadronic top and one for
the leptonic top.
"""

import argparse
import json
import logging
import os
from pathlib import Path

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
    """Read input files and return ``(hadronic_deltas, leptonic_deltas)``."""
    hadronic, leptonic = [], []
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
            "Top_had_mass", "Top_lep_mass",
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
                counters["used"] += 1

        root_file.Close()

    LOG.info("Read %d files and %d events; filled %d events", counters["files"],
             counters["events"], counters["used"])
    return hadronic, leptonic


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-j", "--json", required=True, help="Dataset JSON file")
    parser.add_argument("-o", "--outDir", required=True, help="Directory for PNG plots")
    parser.add_argument("--bins", type=int, default=100)
    parser.add_argument("--xmin", type=float, default=-200.0)
    parser.add_argument("--xmax", type=float, default=200.0)
    args = parser.parse_args()

    if args.bins <= 0 or args.xmin >= args.xmax:
        parser.error("require --bins > 0 and --xmin < --xmax")
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
    hadronic, leptonic = collect_delta_masses(files)

    out_dir = Path(args.outDir)
    make_plot(hadronic, out_dir / "deltaMass_hadronic.png",
              "Hadronic top mass residual", ROOT.kAzure + 1,
              args.bins, args.xmin, args.xmax)
    make_plot(leptonic, out_dir / "deltaMass_leptonic.png",
              "Leptonic top mass residual", ROOT.kOrange + 7,
              args.bins, args.xmin, args.xmax)
    LOG.info("Saved plots in %s", out_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()

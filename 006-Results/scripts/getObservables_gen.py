#!/usr/bin/env python3
"""
getObservables_gen.py

Compute gen-level observables for MC datasets using GenPart information.
Requires both a top (pdgId==6) and anti-top (pdgId==-6) with statusFlags bit 13
(isLastCopy) set. Events missing either are skipped.
Datasets in groups not starting with 'MC' (e.g. Data_mu) are ignored completely.

Usage:
    python getObservables_gen.py \\
        --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \\
        --output_folder Outputs \\
        --output_name gen_UL2016preVFP

Arguments:
    --json_file     Path to the dataFiles JSON {group: {dataset: {filepath: treename}}}.
    --output_folder Output folder (created if absent).
    --output_name   Base name for the output coffea file (no extension).

Output:
    <output_folder>/<output_name>.coffea

    The coffea file stores:
        results[group][dataset] = {
            "gen"    : hist.Hist  – 5D histogram (yt, ytbar, costheta, deltaAbsY, ttbar_mass)
            "nEvents": int        – events passing gen-level top/anti-top selection
            "nTotal" : int        – total events in the dataset before any cuts
        }
"""

import argparse
import json
import os

import ROOT
import numpy as np
import awkward as ak
import uproot
import hist
from coffea.util import save

# statusFlags bit 13 = isLastCopy
IS_LAST_COPY_BIT = 1 << 13

BRANCHES = [
    "GenPart_pdgId",
    "GenPart_statusFlags",
    "GenPart_pt",
    "GenPart_eta",
    "GenPart_phi",
    "GenPart_mass",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_dataset(file_dict: dict) -> tuple:
    """
    Open all ROOT files for one dataset, return (total_events, concatenated ak.Array).
    Returns (0, None) if no file can be opened.
    """
    arrays_list  = []
    total_events = 0
    for fpath, treename in file_dict.items():
        try:
            with uproot.open(f"{fpath}:{treename}") as tree:
                arr = tree.arrays(BRANCHES, library="ak")
                total_events += len(arr)
                arrays_list.append(arr)
        except Exception as e:
            print(f"    [WARNING] Could not open {fpath}: {e}")
    if not arrays_list:
        return 0, None
    return total_events, ak.concatenate(arrays_list)


def _tlv(pt: float, eta: float, phi: float, mass: float) -> ROOT.TLorentzVector:
    """Create a ROOT.TLorentzVector from (pt, eta, phi, mass)."""
    v = ROOT.TLorentzVector()
    v.SetPtEtaPhiM(float(pt), float(eta), float(phi), float(mass))
    return v


def _compute_observables(
    top_pt, top_eta, top_phi, top_mass,
    atop_pt, atop_eta, atop_phi, atop_mass,
) -> tuple:
    """
    Compute (yt, ytbar, costheta, ttbar_mass) for a single event.
    costheta = sign(y_ttbar) * pz_top / |p_top| in the ttbar CM frame.
    """
    top  = _tlv(top_pt,  top_eta,  top_phi,  top_mass)
    atop = _tlv(atop_pt, atop_eta, atop_phi, atop_mass)

    yt    = top.Rapidity()
    ytbar = atop.Rapidity()

    ttbar = top + atop
    boost = ttbar.BoostVector()
    top_cm = ROOT.TLorentzVector(top)
    top_cm.Boost(-boost.X(), -boost.Y(), -boost.Z())

    costheta   = float(np.sign(ttbar.Rapidity())) * top_cm.Pz() / top_cm.P()
    ttbar_mass = ttbar.M()
    return yt, ytbar, costheta, ttbar_mass


_vcompute_observables = np.vectorize(
    _compute_observables,
    otypes=[float, float, float, float],
)


def _new_histogram() -> hist.Hist:
    """Return a fresh, empty 5D histogram with the standard axis definitions."""
    return hist.Hist(
        hist.axis.Regular(4, -2.5,  2.5,  name="yt",
                          label="Rapidity of top quark",
                          underflow=True, overflow=True),
        hist.axis.Regular(4, -2.5,  2.5,  name="ytbar",
                          label="Rapidity of anti-top quark",
                          underflow=True, overflow=True),
        hist.axis.Regular(2, -1,    1,    name="costheta",
                          label="cos(theta*) in ttbar CM frame",
                          underflow=True, overflow=True),
        hist.axis.Regular(2, -10,   10,   name="deltaAbsY",
                          label="|yt| - |ytbar|",
                          underflow=True, overflow=True),
        hist.axis.Regular(9,  300,  1200, name="ttbar_mass",
                          label="Invariant mass of ttbar system [GeV]",
                          underflow=True, overflow=True),
    )


def process_dataset(data: ak.Array) -> tuple:
    """
    Select gen-level last-copy top / anti-top, compute observables, fill histogram.

    Selection:
        - GenPart_statusFlags bit 13 (isLastCopy) must be set.
        - Require exactly at least one top (pdgId==6) and one anti-top (pdgId==-6).
        - Events missing either are dropped.
        - When multiple candidates exist per event, the first (highest-index) is used.

    Returns (h, n_events).
    """
    # ---- build masks for last-copy top and anti-top ----
    is_last_copy = (data["GenPart_statusFlags"] & IS_LAST_COPY_BIT) != 0
    top_mask     = is_last_copy & (data["GenPart_pdgId"] ==  6)
    atop_mask    = is_last_copy & (data["GenPart_pdgId"] == -6)

    # ---- require at least one of each per event ----
    has_top  = ak.num(data["GenPart_pdgId"][top_mask])  >= 1
    has_atop = ak.num(data["GenPart_pdgId"][atop_mask]) >= 1
    valid    = has_top & has_atop

    data      = data[valid]
    top_mask  = top_mask[valid]
    atop_mask = atop_mask[valid]

    # ---- take first candidate per event ----
    def _first(branch, mask):
        return ak.to_numpy(
            ak.fill_none(ak.firsts(data[branch][mask]), 0.0)
        )

    top_pt   = _first("GenPart_pt",   top_mask)
    top_eta  = _first("GenPart_eta",  top_mask)
    top_phi  = _first("GenPart_phi",  top_mask)
    top_mass = _first("GenPart_mass", top_mask)

    atop_pt   = _first("GenPart_pt",   atop_mask)
    atop_eta  = _first("GenPart_eta",  atop_mask)
    atop_phi  = _first("GenPart_phi",  atop_mask)
    atop_mass = _first("GenPart_mass", atop_mask)

    # ---- observables ----
    yt, ytbar, costheta, ttbar_mass = _vcompute_observables(
        top_pt, top_eta, top_phi, top_mass,
        atop_pt, atop_eta, atop_phi, atop_mass,
    )
    deltaAbsY = np.abs(yt) - np.abs(ytbar)

    # ---- fill histogram ----
    h = _new_histogram()
    h.fill(yt=yt, ytbar=ytbar, costheta=costheta,
           deltaAbsY=deltaAbsY, ttbar_mass=ttbar_mass)

    n_events = int(np.sum(h.values(flow=True)))
    return h, n_events


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Gen-level observables for MC datasets using GenPart top quarks."
    )
    parser.add_argument("--json_file",     type=str, required=True,
                        help="Path to the dataFiles JSON")
    parser.add_argument("--output_folder", type=str, required=True,
                        help="Output folder (created if absent)")
    parser.add_argument("--output_name",   type=str, required=True,
                        help="Output coffea file name (no extension)")
    args = parser.parse_args()

    with open(args.json_file) as f:
        datasets = json.load(f)

    os.makedirs(args.output_folder, exist_ok=True)
    results = {}

    for group, group_datasets in datasets.items():
        if not group.startswith("MC"):
            print(f"\n[Skipping group '{group}']")
            continue

        results[group] = {}
        for dataset_name, file_dict in group_datasets.items():
            print(f"\n[{group}] {dataset_name}  ({len(file_dict)} files)")

            n_total, data = load_dataset(file_dict)

            if data is None or len(data) == 0:
                print("  -> Skipping: no events loaded.")
                results[group][dataset_name] = {"gen": None, "nEvents": 0, "nTotal": 0}
                continue

            h, n_events = process_dataset(data)

            results[group][dataset_name] = {
                "gen":     h,
                "nEvents": n_events,
                "nTotal":  n_total,
            }
            print(f"  -> nTotal={n_total},  nEvents={n_events}")

    out_path = os.path.join(args.output_folder, f"{args.output_name}.coffea")
    save(results, out_path)
    print(f"\nSaved to: {out_path}")


if __name__ == "__main__":
    main()
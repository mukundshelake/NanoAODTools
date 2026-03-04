#!/usr/bin/env python3
"""
getObservables_reco.py

Compute reco-level observables for MC events with a BDT score cut.
Datasets in groups not starting with 'MC' (e.g. Data_mu) are ignored completely.

Usage:
    python getObservables_reco.py \\
        --era UL2016preVFP \\
        --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \\
        --output_folder Outputs \\
        --output_name reco_bdt_UL2016preVFP \\
        --bdt_cut 0.55

Arguments:
    --era           Era string. One of: UL2016preVFP, UL2016postVFP, UL2017, UL2018.
    --json_file     Path to the dataFiles JSON {group: {dataset: {filepath: treename}}}.
    --output_folder Output folder (created if absent).
    --output_name   Base name for the output coffea file (no extension).
    --bdt_cut       Keep only events with BDTScore > this value.

Output:
    <output_folder>/<output_name>.coffea

    The coffea file stores:
        results[group][dataset] = {
            "reco"   : hist.Hist  – 5D histogram (yt, ytbar, costheta, deltaAbsY, ttbar_mass)
            "nEvents": int        – events passing BDT cut + muon selection
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

# ---------------------------------------------------------------------------
# Per-era muon pT thresholds
# ---------------------------------------------------------------------------
MUON_PT_THRESHOLD = {
    "UL2016preVFP":  26,
    "UL2016postVFP": 26,
    "UL2017":        29,
    "UL2018":        27,
}

BRANCHES = [
    "Top_lep_pt",  "Top_lep_eta",  "Top_lep_phi",  "Top_lep_mass",
    "Top_had_pt",  "Top_had_eta",  "Top_had_phi",  "Top_had_mass",
    "Muon_pt",     "Muon_eta",     "Muon_charge",
    "Muon_pfRelIso04_all", "Muon_tightId",
    "BDTScore",   # BDT discriminant score
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


def process_dataset(
    data: ak.Array,
    muon_pt_thresh: float,
    bdt_cut: float,
) -> tuple:
    """
    Apply BDT cut + muon selection, compute reco observables, fill histogram.

    Returns (h, n_events) where n_events is the number of events that passed
    all cuts and were filled into the histogram.
    """
    # ---- BDT cut ----
    data = data[data["BDTScore"] > bdt_cut]

    # ---- muon selection ----
    muon_mask = (
        (data["Muon_pt"] > muon_pt_thresh)
        & (np.abs(data["Muon_eta"]) < 2.4)
        & (data["Muon_tightId"] == True)
        & (data["Muon_pfRelIso04_all"] <= 0.06)
    )
    masked_pt = ak.where(muon_mask, data["Muon_pt"], -999.0)
    best_idx  = ak.argmax(masked_pt, axis=1, keepdims=True)
    has_muon  = ak.any(muon_mask, axis=1)
    data      = data[has_muon]
    best_idx  = best_idx[has_muon]

    best_charge = ak.flatten(data["Muon_charge"][best_idx])
    is_pos      = best_charge > 0  # mu+ => lep=anti-top, had=top

    # ---- top / anti-top assignment ----
    top_pt   = ak.where(is_pos, data["Top_had_pt"],   data["Top_lep_pt"])
    top_eta  = ak.where(is_pos, data["Top_had_eta"],  data["Top_lep_eta"])
    top_phi  = ak.where(is_pos, data["Top_had_phi"],  data["Top_lep_phi"])
    top_mass = ak.where(is_pos, data["Top_had_mass"], data["Top_lep_mass"])

    atop_pt   = ak.where(is_pos, data["Top_lep_pt"],   data["Top_had_pt"])
    atop_eta  = ak.where(is_pos, data["Top_lep_eta"],  data["Top_had_eta"])
    atop_phi  = ak.where(is_pos, data["Top_lep_phi"],  data["Top_had_phi"])
    atop_mass = ak.where(is_pos, data["Top_lep_mass"], data["Top_had_mass"])

    # ---- observables ----
    yt, ytbar, costheta, ttbar_mass = _vcompute_observables(
        ak.to_numpy(top_pt),   ak.to_numpy(top_eta),
        ak.to_numpy(top_phi),  ak.to_numpy(top_mass),
        ak.to_numpy(atop_pt),  ak.to_numpy(atop_eta),
        ak.to_numpy(atop_phi), ak.to_numpy(atop_mass),
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
        description="Reco-level observables for MC events with a BDT score cut."
    )
    parser.add_argument("--era",           type=str,   required=True,
                        help="Era string, e.g. UL2016preVFP")
    parser.add_argument("--json_file",     type=str,   required=True,
                        help="Path to the dataFiles JSON")
    parser.add_argument("--output_folder", type=str,   required=True,
                        help="Output folder (created if absent)")
    parser.add_argument("--output_name",   type=str,   required=True,
                        help="Output coffea file name (no extension)")
    parser.add_argument("--bdt_cut",       type=float, required=True,
                        metavar="SCORE",
                        help="Keep only events with BDTScore > SCORE")
    args = parser.parse_args()

    if args.era not in MUON_PT_THRESHOLD:
        raise ValueError(
            f"Unknown era '{args.era}'. Valid: {list(MUON_PT_THRESHOLD.keys())}"
        )

    muon_pt_thresh = MUON_PT_THRESHOLD[args.era]
    print(f"Era: {args.era}  |  Muon pT threshold: {muon_pt_thresh} GeV")
    print(f"BDT cut: BDTScore > {args.bdt_cut}")

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
                results[group][dataset_name] = {"reco": None, "nEvents": 0, "nTotal": 0}
                continue

            h, n_events = process_dataset(data, muon_pt_thresh, args.bdt_cut)

            results[group][dataset_name] = {
                "reco":    h,
                "nEvents": n_events,
                "nTotal":  n_total,
            }
            print(f"  -> nTotal={n_total},  nEvents={n_events}")

    out_path = os.path.join(args.output_folder, f"{args.output_name}.coffea")
    save(results, out_path)
    print(f"\nSaved to: {out_path}")


if __name__ == "__main__":
    main()
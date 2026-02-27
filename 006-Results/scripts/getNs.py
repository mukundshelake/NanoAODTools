#!/usr/bin/env python3
"""
getNs.py - Compute N histograms from ROOT ttbar ntuples for a given era.
         Uses ROOT.TLorentzVector for four-momentum operations.

Usage:
    python getNs.py <era> <json_file> <output_folder> <output_name>

Example:
    python getNs.py UL2016preVFP \\
        Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \\
        Outputs ns_UL2016preVFP

Arguments:
    era           : Era string. One of UL2016preVFP, UL2016postVFP, UL2017, UL2018.
    json_file     : Path to the JSON file that maps dataset names to {filepath: treename}.
    output_folder : Relative path to the folder where the coffea file will be saved.
    output_name   : Name of the output coffea file (without extension).

Output:
    <output_folder>/<output_name>.coffea

    The coffea file stores a nested dict:
        results[group][dataset] = {
            "nevents": int,            # total events in the dataset before any selection
            "hist":    hist.Hist,      # 4D histogram (yt, ytbar, costheta, deltaAbsY)
        }

    Histogram axes (all with underflow and overflow bins):
        yt        : rapidity of top quark             – 4 bins in [-2.5, 2.5]
        ytbar     : rapidity of anti-top quark        – 4 bins in [-2.5, 2.5]
        costheta  : cos(angle top, beam) in CM frame  – 2 bins in [-1, 1]
        deltaAbsY : |yt| - |ytbar|                    – 2 bins in [-10, 10]
        ttbar_mass : invariant mass of ttbar system    – 9 bins in [300, 1200] GeV
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
    "y",           # parton type: 1=qqbar, 2=gg, 3=qg, 4=qqprime, 5=qq, 0=undefined/data
    "BDTScore",    # BDT discriminant score
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_dataset(file_dict: dict) -> tuple:
    """
    Open all ROOT files for a dataset and concatenate the Events tree.

    Parameters
    ----------
    file_dict : dict
        Mapping of {filepath: treename} as stored in the JSON.

    Returns
    -------
    (total_events, concatenated_awkward_array)  or  (0, None) on failure.
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
    Compute (yt, ytbar, costheta, ttbar_mass) for a single event using ROOT.TLorentzVector.

    costheta  = sign(y_ttbar) * pz_top / |p_top|  in the ttbar CM rest frame.
                The sign of the ttbar rapidity approximates the direction of the incoming quark.
    ttbar_mass = invariant mass of the ttbar system in GeV.
    """
    top  = _tlv(top_pt,  top_eta,  top_phi,  top_mass)
    atop = _tlv(atop_pt, atop_eta, atop_phi, atop_mass)

    yt    = top.Rapidity()
    ytbar = atop.Rapidity()

    ttbar = top + atop
    boost = ttbar.BoostVector()

    top_cm = ROOT.TLorentzVector(top)          # copy before boosting
    top_cm.Boost(-boost.X(), -boost.Y(), -boost.Z())

    costheta   = float(np.sign(ttbar.Rapidity())) * top_cm.Pz() / top_cm.P()
    ttbar_mass = ttbar.M()

    return yt, ytbar, costheta, ttbar_mass


# Vectorised version: applies _compute_observables element-wise over numpy arrays.
_vcompute_observables = np.vectorize(
    _compute_observables,
    otypes=[float, float, float, float],
)


def fill_histogram(
    data: ak.Array,
    muon_pt_thresh: float,
    qqbar_only: bool = False,
    bdt_cut: float | None = None,
) -> hist.Hist:
    """
    Apply event-level cuts, muon selection, assign top/anti-top, compute observables,
    fill histogram.

    Event-level pre-selection (applied before muon selection):
        BDTScore > bdt_cut          (if bdt_cut is not None; applied to all events)
        y == 1                      (if qqbar_only is True; applied to MC groups only)

    Muon selection (era-dependent pt threshold, common cuts):
        Muon_pt > muon_pt_thresh
        |Muon_eta| < 2.4
        Muon_tightId == True
        Muon_pfRelIso04_all <= 0.06

    Top/anti-top assignment via leading-muon charge:
        mu+ (charge > 0) -> leptonic decay of anti-top (W+)
            => Top_lep = anti-top,  Top_had = top
        mu- (charge < 0) -> leptonic decay of top (W-)
            => Top_lep = top,       Top_had = anti-top

    Observables:
        yt        = rapidity of top quark
        ytbar     = rapidity of anti-top quark
        costheta   = sign(y_ttbar) * pz_top / |p_top|  in the ttbar CM frame
                     (quark direction approximated by the direction of the ttbar boost)
        deltaAbsY  = |yt| - |ytbar|
        ttbar_mass = invariant mass of the ttbar system in GeV
    """

    # ------------------------------------------------------------------
    # Event-level pre-selection cuts
    # ------------------------------------------------------------------
    if bdt_cut is not None:
        data = data[data["BDTScore"] > bdt_cut]

    if qqbar_only:
        data = data[data["y"] == 1]

    # ------------------------------------------------------------------
    # Muon selection: pick the highest-pT muon passing all cuts
    # ------------------------------------------------------------------
    muon_mask = (
        (data["Muon_pt"] > muon_pt_thresh)
        & (np.abs(data["Muon_eta"]) < 2.4)
        & (data["Muon_tightId"] == True)
        & (data["Muon_pfRelIso04_all"] <= 0.06)
    )

    # Replace pT with -999 for failing muons, then argmax picks the best passing one
    masked_pt = ak.where(muon_mask, data["Muon_pt"], -999.0)
    best_idx  = ak.argmax(masked_pt, axis=1, keepdims=True)

    # Require at least one muon passing selection
    has_muon = ak.any(muon_mask, axis=1)
    data     = data[has_muon]
    best_idx = best_idx[has_muon]

    best_charge = ak.flatten(data["Muon_charge"][best_idx])  # shape: (nevents,)

    # ------------------------------------------------------------------
    # Assign top / anti-top
    # ------------------------------------------------------------------
    is_pos = best_charge > 0  # True => lep=anti-top, had=top

    top_pt   = ak.where(is_pos, data["Top_had_pt"],   data["Top_lep_pt"])
    top_eta  = ak.where(is_pos, data["Top_had_eta"],  data["Top_lep_eta"])
    top_phi  = ak.where(is_pos, data["Top_had_phi"],  data["Top_lep_phi"])
    top_mass = ak.where(is_pos, data["Top_had_mass"], data["Top_lep_mass"])

    atop_pt   = ak.where(is_pos, data["Top_lep_pt"],   data["Top_had_pt"])
    atop_eta  = ak.where(is_pos, data["Top_lep_eta"],  data["Top_had_eta"])
    atop_phi  = ak.where(is_pos, data["Top_lep_phi"],  data["Top_had_phi"])
    atop_mass = ak.where(is_pos, data["Top_lep_mass"], data["Top_had_mass"])

    # ------------------------------------------------------------------
    # Observables via ROOT.TLorentzVector (event-by-event)
    # ------------------------------------------------------------------
    yt, ytbar, costheta, ttbar_mass = _vcompute_observables(
        ak.to_numpy(top_pt),   ak.to_numpy(top_eta),
        ak.to_numpy(top_phi),  ak.to_numpy(top_mass),
        ak.to_numpy(atop_pt),  ak.to_numpy(atop_eta),
        ak.to_numpy(atop_phi), ak.to_numpy(atop_mass),
    )

    deltaAbsY = np.abs(yt) - np.abs(ytbar)

    # ------------------------------------------------------------------
    # Build and fill histogram
    # ------------------------------------------------------------------
    h = hist.Hist(
        hist.axis.Regular(
            4, -2.5, 2.5,
            name="yt", label="Rapidity of top quark",
            underflow=True, overflow=True,
        ),
        hist.axis.Regular(
            4, -2.5, 2.5,
            name="ytbar", label="Rapidity of anti-top quark",
            underflow=True, overflow=True,
        ),
        hist.axis.Regular(
            2, -1, 1,
            name="costheta",
            label="Cosine of the angle between the top quark and the incoming quark in the center of mass frame",
            underflow=True, overflow=True,
        ),
        hist.axis.Regular(
            2, -10, 10,
            name="deltaAbsY",
            label="Delta between abs(rapidity of top quark) and abs(rapidity of anti-top quark)",
            underflow=True, overflow=True,
        ),
        hist.axis.Regular(
            9, 300, 1200,
            name="ttbar_mass",
            label="Invariant mass of the ttbar system [GeV]",
            underflow=True, overflow=True,
        ),
    )
    h.fill(yt=yt, ytbar=ytbar, costheta=costheta, deltaAbsY=deltaAbsY, ttbar_mass=ttbar_mass)

    return h


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Compute rapidity/angle histograms from ttbar ROOT ntuples."
    )
    parser.add_argument("--era",           type=str, help="Era string, e.g. UL2016preVFP")
    parser.add_argument("--json_file",     type=str, help="Path to the dataset JSON file")
    parser.add_argument("--output_folder", type=str, help="Output folder (created if absent)")
    parser.add_argument("--output_name",   type=str, help="Output coffea file name (no extension)")
    parser.add_argument(
        "--qqbar_only", action="store_true", default=False,
        help="Only process qqbar events (y==1). Applied to MC groups only; Data is unaffected.",
    )
    parser.add_argument(
        "--bdt_cut", type=float, default=None, metavar="SCORE",
        help="Keep only events with BDTScore > SCORE (applied to both Data and MC).",
    )
    args = parser.parse_args()

    if args.era not in MUON_PT_THRESHOLD:
        raise ValueError(
            f"Unknown era '{args.era}'. Valid options: {list(MUON_PT_THRESHOLD.keys())}"
        )

    muon_pt_thresh = MUON_PT_THRESHOLD[args.era]
    print(f"Era: {args.era}  |  Muon pT threshold: {muon_pt_thresh} GeV")
    if args.bdt_cut is not None:
        print(f"BDT cut: BDTScore > {args.bdt_cut}")
    if args.qqbar_only:
        print("qqbar-only mode: MC groups will be filtered to y==1 events.")

    with open(args.json_file) as f:
        datasets = json.load(f)

    os.makedirs(args.output_folder, exist_ok=True)

    results = {}

    for group, group_datasets in datasets.items():
        results[group] = {}
        for dataset_name, file_dict in group_datasets.items():
            print(f"\n[{group}] {dataset_name}  ({len(file_dict)} files)")

            nevents, data = load_dataset(file_dict)

            if data is None or len(data) == 0:
                print("  -> Skipping: no events loaded.")
                results[group][dataset_name] = {"nevents": 0, "hist": None}
                continue

            # Apply qqbar filter only for MC groups (Data has y=0 always)
            apply_qqbar = args.qqbar_only and group.startswith("MC")
            h = fill_histogram(data, muon_pt_thresh,
                               qqbar_only=apply_qqbar,
                               bdt_cut=args.bdt_cut)
            n_filled = int(np.sum(h.values(flow=True)))

            results[group][dataset_name] = {
                "nevents": nevents,
                "hist":    h,
            }
            print(f"  -> Total events: {nevents},  histogram entries (incl. flow): {n_filled}")

    out_path = os.path.join(args.output_folder, f"{args.output_name}.coffea")
    save(results, out_path)
    print(f"\nSaved results to: {out_path}")


if __name__ == "__main__":
    main()

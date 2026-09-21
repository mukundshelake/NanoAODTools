"""
Extract reco kinematics, gen-level top/antitop rapidities and mtt_gen (signal mode),
and event weights from BDTScore ROOT files into a single parquet file.

Usage:
  python getParquet.py input_dir output.parquet           # background (reco + weights only)
  python getParquet.py --signal input_dir output.parquet  # signal (adds gen columns)
"""

import argparse
import glob
import os
import sys

import awkward as ak
import numpy as np
import uproot

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
import kinematics  # noqa: E402


RECO_BRANCHES = [
    "yt", "ytbar", "ttbar_mass", "Pgof",
    "Top_lep_pt", "Top_lep_eta", "Top_lep_mass",
    "Top_had_pt", "Top_had_eta", "Top_had_mass",
    "Muon_charge",
]

WEIGHT_BRANCHES = [
    "LHEWeightSign",
    "puWeight", "puWeightUp", "puWeightDown",
    "bTaggingWeight", "bTaggingWeightUp", "bTaggingWeightDown",
    "MuonHLTWeight",
    "MuonIDWeight", "MuonIDWeightUp", "MuonIDWeightDown",
    "L1PreFiringWeight_Nom", "L1PreFiringWeight_Up", "L1PreFiringWeight_Dn",
]

GEN_BRANCHES = [
    "GenPart_pdgId", "GenPart_statusFlags",
    "GenPart_pt", "GenPart_eta", "GenPart_phi", "GenPart_mass",
]


def compute_lab_yt_ytbar(a):
    """Lab-frame top/antitop rapidity, assigned from the muon charge.

    The convention lives in kinematics.assign_top_antitop -- read its docstring
    before touching this. Briefly: t -> W+ b -> mu+ nu b, so mu+ means the
    LEPTONIC side is the TOP. This file previously had that inverted, which
    swapped y_t and y_tbar in every event (issue #29).
    """
    y_lep = kinematics.rapidity(a["Top_lep_pt"], a["Top_lep_eta"], a["Top_lep_mass"])
    y_had = kinematics.rapidity(a["Top_had_pt"], a["Top_had_eta"], a["Top_had_mass"])

    # FIXME(#33): this is the first muon of the *unfiltered* collection, not the
    # muon the analysis selected. 45% of events have more than one muon and the
    # charge disagrees with the properly selected muon in ~1.4% of events.
    # kinematics.select_muon_index() does it correctly but needs the era, which
    # this script has no way to know -- it arrives with the config in #41.
    charge = ak.to_numpy(ak.fill_none(ak.firsts(a["Muon_charge"]), 0))

    return kinematics.assign_top_antitop(y_lep, y_had, charge)


def build_weights(a):
    """Compute nominal weight and per-source systematic variations."""
    sign  = a["LHEWeightSign"]
    pu    = a["puWeight"];    pu_up  = a["puWeightUp"];    pu_dn  = a["puWeightDown"]
    btag  = a["bTaggingWeight"]; btag_up = a["bTaggingWeightUp"]; btag_dn = a["bTaggingWeightDown"]
    hlt   = a["MuonHLTWeight"]
    mid   = a["MuonIDWeight"];   mid_up  = a["MuonIDWeightUp"];   mid_dn  = a["MuonIDWeightDown"]
    pref  = a["L1PreFiringWeight_Nom"]; pref_up = a["L1PreFiringWeight_Up"]; pref_dn = a["L1PreFiringWeight_Dn"]

    base = sign * hlt
    return {
        "weight_nominal":       base * pu    * btag    * mid    * pref,
        "weight_pileupUp":      base * pu_up * btag    * mid    * pref,
        "weight_pileupDown":    base * pu_dn * btag    * mid    * pref,
        "weight_btagUp":        base * pu    * btag_up * mid    * pref,
        "weight_btagDown":      base * pu    * btag_dn * mid    * pref,
        "weight_muonIDUp":      base * pu    * btag    * mid_up * pref,
        "weight_muonIDDown":    base * pu    * btag    * mid_dn * pref,
        "weight_prefiringUp":   base * pu    * btag    * mid    * pref_up,
        "weight_prefiringDown": base * pu    * btag    * mid    * pref_dn,
    }


def process_file(path, is_signal):
    tree = uproot.open(path)["Events"]
    if tree.num_entries == 0:
        return None

    branches = RECO_BRANCHES + WEIGHT_BRANCHES + (GEN_BRANCHES if is_signal else [])
    
    # Filter to only branches that exist in the file
    available_branches = [b for b in branches if b in tree.keys()]
    arrays = tree.arrays(available_branches)
    
    # If LHEWeightSign is missing, create it as ones
    if "LHEWeightSign" not in available_branches:
        arrays["LHEWeightSign"] = np.ones(tree.num_entries)

    result = {
        "yt":       arrays["yt"],
        "ytbar":    arrays["ytbar"],
        "mtt_reco": arrays["ttbar_mass"],
        "Pgof":     arrays["Pgof"],
        **build_weights(arrays),
    }

    yt_lab, ytbar_lab = compute_lab_yt_ytbar(arrays)
    result["yt_lab"]    = yt_lab
    result["ytbar_lab"] = ytbar_lab

    if is_signal:
        pdgId = arrays["GenPart_pdgId"]
        sf    = arrays["GenPart_statusFlags"]
        pt    = arrays["GenPart_pt"]
        eta   = arrays["GenPart_eta"]
        phi   = arrays["GenPart_phi"]
        mass  = arrays["GenPart_mass"]

        isLastCopy = (sf >> 13) & 1
        top_mask  = (pdgId ==  6) & (isLastCopy == 1)
        atop_mask = (pdgId == -6) & (isLastCopy == 1)

        top_pt   = ak.firsts(pt[top_mask]);   top_eta  = ak.firsts(eta[top_mask])
        top_phi  = ak.firsts(phi[top_mask]);  top_mass = ak.firsts(mass[top_mask])
        atop_pt  = ak.firsts(pt[atop_mask]);  atop_eta = ak.firsts(eta[atop_mask])
        atop_phi = ak.firsts(phi[atop_mask]); atop_mass = ak.firsts(mass[atop_mask])

        gen_yt    = kinematics.rapidity(top_pt,  top_eta,  top_mass)
        gen_ytbar = kinematics.rapidity(atop_pt, atop_eta, atop_mass)
        mtt_gen   = kinematics.invariant_mass(
            (top_pt, top_eta, top_phi, top_mass),
            (atop_pt, atop_eta, atop_phi, atop_mass),
        )

        result["gen_yt"]    = gen_yt
        result["gen_ytbar"] = gen_ytbar
        result["mtt_gen"]   = mtt_gen

        combined = ak.Array(result)
        # Drop events where gen top/antitop selection failed (shouldn't happen in ttbar MC)
        valid = ~(ak.is_none(gen_yt) | ak.is_none(gen_ytbar))
        return combined[valid]

    return ak.Array(result)


def main():
    parser = argparse.ArgumentParser(description="Extract kinematics and weights to parquet")
    parser.add_argument("input_dir", help="Directory containing BDTScore ROOT files")
    parser.add_argument("output", help="Output parquet file path")
    parser.add_argument("--signal", action="store_true",
                        help="Signal mode: also extract gen top/antitop columns")
    args = parser.parse_args()

    files = sorted(glob.glob(os.path.join(args.input_dir, "*.root")))
    if not files:
        raise RuntimeError(f"No ROOT files found in {args.input_dir}")

    chunks = []
    for path in files:
        chunk = process_file(path, args.signal)
        n = len(chunk) if chunk is not None else 0
        if chunk is not None and n > 0:
            chunks.append(chunk)
        print(f"  {os.path.basename(path)}: {n} events")

    if not chunks:
        raise RuntimeError("No events found across all files")

    combined = ak.concatenate(chunks)
    print(f"\nTotal events: {len(combined)}")

    ak.to_parquet(combined, args.output)
    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()

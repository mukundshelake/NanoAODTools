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

import awkward as ak
import numpy as np
import uproot


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


def compute_rapidity(pt, eta, mass):
    pz = pt * np.sinh(eta)
    E = np.sqrt((pt * np.cosh(eta)) ** 2 + mass**2)
    return 0.5 * np.log((E + pz) / (E - pz))


def compute_lab_yt_ytbar(a):
    """Lab-frame top/antitop rapidity, assigning via muon charge.

    mu- (charge=-1) comes from t->W-: leptonic side is top.
    mu+ (charge=+1) comes from tbar->W+: leptonic side is antitop.
    """
    y_lep = compute_rapidity(a["Top_lep_pt"], a["Top_lep_eta"], a["Top_lep_mass"])
    y_had = compute_rapidity(a["Top_had_pt"], a["Top_had_eta"], a["Top_had_mass"])
    # selected muon charge: first muon in each event
    charge = ak.firsts(a["Muon_charge"])
    mu_minus = charge < 0  # True -> leptonic = top, False -> leptonic = antitop
    yt_lab    = ak.where(mu_minus, y_lep, y_had)
    ytbar_lab = ak.where(mu_minus, y_had, y_lep)
    return yt_lab, ytbar_lab


def compute_mtt(pt1, eta1, phi1, mass1, pt2, eta2, phi2, mass2):
    px1 = pt1 * np.cos(phi1);  px2 = pt2 * np.cos(phi2)
    py1 = pt1 * np.sin(phi1);  py2 = pt2 * np.sin(phi2)
    pz1 = pt1 * np.sinh(eta1); pz2 = pt2 * np.sinh(eta2)
    E1 = np.sqrt((pt1 * np.cosh(eta1)) ** 2 + mass1**2)
    E2 = np.sqrt((pt2 * np.cosh(eta2)) ** 2 + mass2**2)
    return np.sqrt(np.maximum(
        (E1 + E2)**2 - (px1 + px2)**2 - (py1 + py2)**2 - (pz1 + pz2)**2, 0.0
    ))


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
    arrays = tree.arrays(branches)

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

        gen_yt    = compute_rapidity(top_pt,  top_eta,  top_mass)
        gen_ytbar = compute_rapidity(atop_pt, atop_eta, atop_mass)
        mtt_gen   = compute_mtt(top_pt, top_eta, top_phi, top_mass,
                                atop_pt, atop_eta, atop_phi, atop_mass)

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

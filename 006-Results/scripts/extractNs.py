#!/usr/bin/env python3
"""
extractNs.py - Extract event counts from per-dataset coffea files produced by getObservables_*.py.

Usage:
    python extractNs.py \\
        --input_folder Outputs/reco_bdt \\
        --era UL2016preVFP \\
        --json_file Inputs/midNov_BDTScore_UL2016preVFP_dataFiles.json \\
        --output_folder Outputs \\
        --output_name counts_UL2016preVFP

Arguments:
    --input_folder  : Folder containing <era>_<dataset>.coffea files.
    --era           : Era string, e.g. UL2016preVFP.
    --json_file     : Path to the dataFiles JSON (used to reconstruct group->dataset mapping).
    --output_folder : Output folder path (created if absent).
    --output_name   : Output JSON file name (without extension).

Output:
    <output_folder>/<output_name>.json

    The JSON stores a nested dict:
        output[group][dataset] = {
            "N_out_pos" : [v0, v1, ..., v8],   # |yt|   in [1.25, 2.5],  per ttbar_mass bin
            "N_out_neg" : [...],                # |ytbar| in [1.25, 2.5],  per ttbar_mass bin
            "N_in_pos"  : [...],                # |yt|   < 1.25,           per ttbar_mass bin
            "N_in_neg"  : [...],                # |ytbar| < 1.25,          per ttbar_mass bin
            "N_FB_pos"  : [...],                # cos(theta*) > 0,         per ttbar_mass bin
            "N_FB_neg"  : [...],                # cos(theta*) < 0,         per ttbar_mass bin
            "N_c_pos"   : [...],                # |yt| - |ytbar| > 0,      per ttbar_mass bin
            "N_c_neg"   : [...],                # |yt| - |ytbar| < 0,      per ttbar_mass bin
            "nEvents"   : int,
            "nTotal"    : int,

            # Only present when the coffea file contains syst variations:
            "syst": {
                "muonIDUp":   { "N_out_pos": [...], ... },
                "muonIDDown": { ... },
                ...  # one entry per variation found in the histogram's systematic axis
            }
        }

    The 9 values in each list correspond to the ttbar_mass bins:
        [300,400], [400,500], ..., [1100,1200]  (100 GeV steps)

Histogram axis layout (after slicing the systematic axis):
    Axis 0  yt        : 4 bins, edges [-2.5, -1.25, 0, 1.25, 2.5]
              bin 0 [-2.5,-1.25] -> |yt| in [1.25,2.5]  (out)
              bin 1 [-1.25, 0  ] -> |yt| in [0,  1.25]  (in)
              bin 2 [0,    1.25] -> |yt| in [0,  1.25]  (in)
              bin 3 [1.25, 2.5 ] -> |yt| in [1.25,2.5]  (out)
    Axis 1  ytbar     : same structure as yt
    Axis 2  costheta  : 2 bins, edges [-1, 0, 1]
              bin 0 [-1, 0] -> costheta < 0  (neg)
              bin 1 [ 0, 1] -> costheta > 0  (pos)
    Axis 3  deltaAbsY : 2 bins, edges [-10, 0, 10]
              bin 0 [-10, 0] -> deltaAbsY < 0  (neg)
              bin 1 [  0,10] -> deltaAbsY > 0  (pos)
    Axis 4  ttbar_mass: 9 bins, edges [300, 400, ..., 1200]
"""

import argparse
import json
import os

import numpy as np
from coffea.util import load

# yt / ytbar axis:  bins 0 and 3 have |y| in [1.25, 2.5] ("out"),
#                   bins 1 and 2 have |y| < 1.25           ("in")
OUT_BINS = [0, 3]
IN_BINS  = [1, 2]


def extract_counts(h) -> dict:
    """
    Extract per-ttbar_mass-bin counts from a 5D hist.Hist object.

    The histogram axes must be ordered as:
        (yt, ytbar, costheta, deltaAbsY, ttbar_mass)
    (i.e. call with the systematic axis already sliced off)

    Returns
    -------
    dict with keys N_out_pos, N_out_neg, N_in_pos, N_in_neg,
                   N_FB_pos, N_FB_neg, N_c_pos, N_c_neg.
    Each value is a list of 9 floats (one per ttbar_mass bin).
    """
    # values shape: (4, 4, 2, 2, 9)  — no flow bins
    v = h.values(flow=False)

    def _sum_over_leading(arr_5d, axis, bins):
        idx        = [slice(None)] * arr_5d.ndim
        idx[axis]  = bins
        selected   = arr_5d[tuple(idx)]
        return selected.sum(axis=tuple(range(arr_5d.ndim - 1))).tolist()

    return {
        "N_out_pos": _sum_over_leading(v, axis=0, bins=OUT_BINS),
        "N_out_neg": _sum_over_leading(v, axis=1, bins=OUT_BINS),
        "N_in_pos":  _sum_over_leading(v, axis=0, bins=IN_BINS),
        "N_in_neg":  _sum_over_leading(v, axis=1, bins=IN_BINS),
        "N_FB_pos":  _sum_over_leading(v, axis=2, bins=[1]),
        "N_FB_neg":  _sum_over_leading(v, axis=2, bins=[0]),
        "N_c_pos":   _sum_over_leading(v, axis=3, bins=[1]),
        "N_c_neg":   _sum_over_leading(v, axis=3, bins=[0]),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Extract event counts from per-dataset coffea files."
    )
    parser.add_argument("--input_folder",  type=str, required=True,
                        help="Folder containing <era>_<dataset>.coffea files")
    parser.add_argument("--era",           type=str, required=True,
                        help="Era string, e.g. UL2016preVFP")
    parser.add_argument("--json_file",     type=str, required=True,
                        help="Path to the dataFiles JSON (for group->dataset mapping)")
    parser.add_argument("--output_folder", type=str, required=True,
                        help="Output folder path (created if absent)")
    parser.add_argument("--output_name",   type=str, required=True,
                        help="Output JSON file name (without extension)")
    args = parser.parse_args()

    with open(args.json_file) as f:
        datasets_json = json.load(f)

    output = {}

    for group, group_datasets in datasets_json.items():
        output[group] = {}
        for dataset_name in group_datasets:
            coffea_file = os.path.join(
                args.input_folder, f"{args.era}_{dataset_name}.coffea"
            )
            if not os.path.exists(coffea_file):
                print(f"  [{group}] {dataset_name}: file not found, skipping.")
                continue

            print(f"\n  [{group}] {dataset_name}: loading {coffea_file}")
            data = load(coffea_file)

            # Find the histogram (key may be "reco" or "gen")
            h_full = None
            for key in ("reco", "gen", "hist"):
                h_full = data.get(key)
                if h_full is not None:
                    break

            if h_full is None:
                print(f"  [{group}] {dataset_name}: no histogram found, skipping.")
                continue

            # Determine if the histogram has a systematic axis
            axis_names = [ax.name for ax in h_full.axes]
            has_syst   = "systematic" in axis_names

            # Slice out the nominal 5D histogram
            h_nom = h_full[{"systematic": "nominal"}] if has_syst else h_full

            counts = extract_counts(h_nom)
            counts["nEvents"] = int(data.get("nEvents", 0))
            counts["nTotal"]  = int(data.get("nTotal",  0))

            # Extract syst variations from the same file
            if has_syst:
                syst_labels = [c for c in h_full.axes["systematic"]
                               if c != "nominal"]
                if syst_labels:
                    syst_counts = {}
                    for var in syst_labels:
                        h_var = h_full[{"systematic": var}]
                        syst_counts[var] = extract_counts(h_var)
                    counts["syst"] = syst_counts
                    print(f"  [{group}] {dataset_name}: {len(syst_counts)} syst variations extracted.")

            output[group][dataset_name] = counts

            total = sum(counts["N_out_pos"]) + sum(counts["N_in_pos"])
            print(f"  [{group}] {dataset_name}: {total:.0f} selected events")

    os.makedirs(args.output_folder, exist_ok=True)
    out_path = os.path.join(args.output_folder, f"{args.output_name}.json")

    with open(out_path, "w") as f:
        json.dump(output, f, indent=4)

    print(f"\nSaved to: {out_path}")


if __name__ == "__main__":
    main()

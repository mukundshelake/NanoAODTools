#!/usr/bin/env python3
"""
extractNs.py - Extract event counts from a coffea file produced by getNs.py.

Usage:
    python extractNs.py --coffea_file <path> --output_folder <folder> --output_name <name>

Arguments:
    --coffea_file   : Path to the input coffea file (output of getNs.py).
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
        }

    The 9 values in each list correspond to the ttbar_mass bins:
        [300,400], [400,500], ..., [1100,1200]  (100 GeV steps)

Histogram axis layout (from getNs.py):
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

    Returns
    -------
    dict with keys N_out_pos, N_out_neg, N_in_pos, N_in_neg,
                   N_FB_pos, N_FB_neg, N_c_pos, N_c_neg.
    Each value is a list of 9 floats (one per ttbar_mass bin).
    """
    # values shape: (4, 4, 2, 2, 9)  — no flow bins
    v = h.values(flow=False)

    def _sum_over_leading(arr_5d, axis, bins):
        """Sum arr_5d along `axis`, keeping only `bins`, then flatten remaining axes."""
        idx        = [slice(None)] * arr_5d.ndim
        idx[axis]  = bins
        selected   = arr_5d[tuple(idx)]                   # reduced along `axis`
        # sum all axes except the last one (ttbar_mass)
        return selected.sum(axis=tuple(range(arr_5d.ndim - 1))).tolist()

    return {
        # |yt| in [1.25, 2.5]  -> yt bins 0, 3
        "N_out_pos": _sum_over_leading(v, axis=0, bins=OUT_BINS),
        # |ytbar| in [1.25, 2.5]  -> ytbar bins 0, 3
        "N_out_neg": _sum_over_leading(v, axis=1, bins=OUT_BINS),
        # |yt| < 1.25  -> yt bins 1, 2
        "N_in_pos":  _sum_over_leading(v, axis=0, bins=IN_BINS),
        # |ytbar| < 1.25  -> ytbar bins 1, 2
        "N_in_neg":  _sum_over_leading(v, axis=1, bins=IN_BINS),
        # costheta > 0  -> costheta bin 1
        "N_FB_pos":  _sum_over_leading(v, axis=2, bins=[1]),
        # costheta < 0  -> costheta bin 0
        "N_FB_neg":  _sum_over_leading(v, axis=2, bins=[0]),
        # deltaAbsY > 0  -> deltaAbsY bin 1
        "N_c_pos":   _sum_over_leading(v, axis=3, bins=[1]),
        # deltaAbsY < 0  -> deltaAbsY bin 0
        "N_c_neg":   _sum_over_leading(v, axis=3, bins=[0]),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Extract event counts from a getNs.py coffea file."
    )
    parser.add_argument("--coffea_file",   type=str, required=True,
                        help="Path to the input coffea file")
    parser.add_argument("--output_folder", type=str, required=True,
                        help="Output folder path (created if absent)")
    parser.add_argument("--output_name",   type=str, required=True,
                        help="Output JSON file name (without extension)")
    args = parser.parse_args()

    print(f"Loading: {args.coffea_file}")
    data = load(args.coffea_file)

    output = {}

    for group, group_datasets in data.items():
        output[group] = {}
        for dataset_name, dataset_data in group_datasets.items():
            h = dataset_data.get("hist")

            if h is None:
                print(f"  [{group}] {dataset_name}: no histogram, skipping.")
                output[group][dataset_name] = None
                continue

            counts = extract_counts(h)
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

#!/usr/bin/env python3
"""
getAs.py - Compute charge asymmetries A_out, A_in, A_FB, A_c from extractNs.py output.

Usage:
    python getAs.py --counts_file <path> --lumiXinfo_file <path>
                    --output_folder <folder> --output_name <name>

Arguments:
    --counts_file    : Path to the counts JSON produced by extractNs.py.
    --lumiXinfo_file : Path to the lumiXinfo JSON with luminosity, cross-sections,
                       and number of generated events.
    --output_folder  : Output folder path (created if absent).
    --output_name    : Output JSON file name (without extension).

Output:
    <output_folder>/<output_name>.json

    The JSON stores:
        {    
            "Data": {
                "A_out": [v0, ..., v8],   # one value per ttbar_mass bin
                "A_in":  [...],
                "A_FB":  [...],
                "A_c":   [...]
            },
            "MC": {
                "A_out": [...],
                "A_in":  [...],
                "A_FB":  [...],
                "A_c":   [...]
            }
        }

    Asymmetries are defined as:
        A_X = (N_X_pos_total - N_X_neg_total) / (N_X_pos_total + N_X_neg_total)

    Totals:
        Data : simple sum over all datasets in any group whose name starts with "Data"
        MC   : luminosity-weighted sum over all datasets in any group whose name
               starts with "MC",  weight = lumi * xsec / n_gen

    lumiXinfo key format: "{era}_{dataset_name}"
    (e.g. "UL2016preVFP_ttbar_SemiLeptonic")
"""

import argparse
import json
import os

import numpy as np

COUNT_KEYS = ["N_out_pos", "N_out_neg", "N_in_pos", "N_in_neg",
              "N_FB_pos",  "N_FB_neg",  "N_c_pos",  "N_c_neg"]

ASYMMETRY_MAP = {
    "A_out": ("N_out_pos", "N_out_neg"),
    "A_in":  ("N_in_pos",  "N_in_neg"),
    "A_FB":  ("N_FB_pos",  "N_FB_neg"),
    "A_c":   ("N_c_pos",   "N_c_neg"),
}


def asymmetry(pos: np.ndarray, neg: np.ndarray) -> list:
    """
    Compute A = (pos - neg) / (pos + neg) bin-by-bin.
    Returns 0.0 for bins where pos + neg == 0.
    """
    denom = pos + neg
    result = np.where(denom > 0, (pos - neg) / denom, 0.0)
    return result.tolist()


def main():
    parser = argparse.ArgumentParser(
        description="Compute charge asymmetries from extractNs.py counts."
    )
    parser.add_argument("--counts_file",    type=str, required=True,
                        help="Path to the counts JSON from extractNs.py")
    parser.add_argument("--lumiXinfo_file", type=str, required=True,
                        help="Path to the lumiXinfo JSON")
    parser.add_argument("--output_folder",  type=str, required=True,
                        help="Output folder path (created if absent)")
    parser.add_argument("--output_name",    type=str, required=True,
                        help="Output JSON file name (without extension)")
    args = parser.parse_args()

    with open(args.counts_file) as f:
        counts = json.load(f)

    with open(args.lumiXinfo_file) as f:
        lumiXinfo = json.load(f)

    lumi       = lumiXinfo["Luminosity"]         # pb^-1
    era        = lumiXinfo["era"]
    xsecs      = lumiXinfo["cross_sections"]     # {era_dataset: xsec [pb]}
    ngens      = lumiXinfo["generated_events"]   # {era_dataset: n_gen}

    # Determine number of mass bins from first valid entry
    n_bins = None
    for group in counts.values():
        for dataset_counts in group.values():
            if dataset_counts is not None:
                n_bins = len(dataset_counts["N_out_pos"])
                break
        if n_bins is not None:
            break

    if n_bins is None:
        raise RuntimeError("No valid dataset found in counts file.")

    # Accumulators: Data (unweighted sum) and MC (lumi-weighted sum)
    totals = {
        "Data": {k: np.zeros(n_bins) for k in COUNT_KEYS},
        "MC":   {k: np.zeros(n_bins) for k in COUNT_KEYS},
    }

    for group_name, group_datasets in counts.items():
        if group_name.startswith("Data"):
            tag = "Data"
        elif group_name.startswith("MC"):
            tag = "MC"
        else:
            print(f"[WARNING] Unknown group '{group_name}', skipping.")
            continue

        for dataset_name, dataset_counts in group_datasets.items():
            if dataset_counts is None:
                print(f"  [{group_name}] {dataset_name}: no counts, skipping.")
                continue

            if tag == "Data":
                weight = 1.0
            else:
                key = f"{era}_{dataset_name}"
                if key not in xsecs:
                    print(f"  [WARNING] No xsec for '{key}', skipping.")
                    continue
                if key not in ngens:
                    print(f"  [WARNING] No n_gen for '{key}', skipping.")
                    continue
                xsec  = xsecs[key]
                n_gen = ngens[key]
                weight = lumi * xsec / n_gen

            for k in COUNT_KEYS:
                totals[tag][k] += weight * np.array(dataset_counts[k])

            print(f"  [{group_name}] {dataset_name}: weight={weight:.6g}")

    # Compute asymmetries
    output = {}
    for tag in ("Data", "MC"):
        output[tag] = {}
        for asym_name, (pos_key, neg_key) in ASYMMETRY_MAP.items():
            output[tag][asym_name] = asymmetry(
                totals[tag][pos_key],
                totals[tag][neg_key],
            )
        # Also store the totals for reference
        output[tag]["totals"] = {k: totals[tag][k].tolist() for k in COUNT_KEYS}

    os.makedirs(args.output_folder, exist_ok=True)
    out_path = os.path.join(args.output_folder, f"{args.output_name}.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=4)

    print(f"\nSaved asymmetries to: {out_path}")

    # Print summary table
    mass_edges = [300 + 100 * i for i in range(n_bins + 1)]
    bin_labels = [f"[{mass_edges[i]},{mass_edges[i+1]}]" for i in range(n_bins)]
    for tag in ("Data", "MC"):
        print(f"\n{'='*10} {tag} {'='*10}")
        print(f"{'Mass bin':>18}{'A_out':>10}{'A_in':>10}{'A_FB':>10}{'A_c':>10}")
        for i, label in enumerate(bin_labels):
            vals = {a: output[tag][a][i] for a in ASYMMETRY_MAP}
            print(f"{label:>18}{vals['A_out']:>10.4f}{vals['A_in']:>10.4f}"
                  f"{vals['A_FB']:>10.4f}{vals['A_c']:>10.4f}")


if __name__ == "__main__":
    main()
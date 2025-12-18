#!/usr/bin/env python3
import json
import os
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--era", required=True)
    args = parser.parse_args()

    era = args.era

    dataset_file = f"dataset_{era}.json"
    lumi_dir = f"lumi/{era}"
    out_file = f"processed_dataset_{era}.json"

    if not os.path.exists(dataset_file):
        raise FileNotFoundError(dataset_file)

    with open(dataset_file) as f:
        datasets = json.load(f)

    if "Data" not in datasets:
        raise RuntimeError("No Data section in dataset JSON")

    processed = {}

    for name, query in datasets["Data"].items():

        summary_path = os.path.join(
            lumi_dir, f"{name}_summary.json"
        )
        lumi_value_path = os.path.join(
            lumi_dir, f"{name}_lumis_golden_value.json"
        )

        if not os.path.exists(summary_path):
            print(f"⚠️  Missing summary for {name}, skipping")
            continue

        if not os.path.exists(lumi_value_path):
            print(f"⚠️  Missing lumi value for {name}, skipping")
            continue

        with open(summary_path) as f:
            summary = json.load(f)

        with open(lumi_value_path) as f:
            lumi_val = json.load(f)

        processed[name] = {
            "query": query,
            "run_min": summary["run_min"],
            "run_max": summary["run_max"],
            "n_lumisections": summary["n_lumisections"],
            "recorded_lumi_pb": lumi_val["recorded_lumi_pb"]
        }

    with open(out_file, "w") as f:
        json.dump(processed, f, indent=2, sort_keys=True)

    print(f"✅ Written: {out_file}")
    print(f"   Datasets included: {len(processed)}")


if __name__ == "__main__":
    main()

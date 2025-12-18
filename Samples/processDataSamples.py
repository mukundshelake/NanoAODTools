#!/usr/bin/env python3
import os
import json
import uproot
import subprocess
import argparse
import numpy as np
from FWCore.PythonUtilities.LumiList import LumiList


# -----------------------------------------------------------
# Helper: load era dataset definitions
# -----------------------------------------------------------
def load_era_json(era):
    json_path = f"dataset_{era}.json"
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"❌ dataset JSON not found: {json_path}")

    with open(json_path) as f:
        return json.load(f)


# -----------------------------------------------------------
# Lumi helpers
# -----------------------------------------------------------
def build_dataset_lumilist(era, dataset_name):
    """
    Merge per-file LumiList JSONs into a dataset-level JSON
    WITHOUT using LumiList arithmetic (CMSSW13-safe).
    """
    lumi_dir = f"lumi/{era}/{dataset_name}"
    if not os.path.isdir(lumi_dir):
        print(f"❌ Lumi directory not found: {lumi_dir}")
        return None

    files = [
        os.path.join(lumi_dir, f)
        for f in os.listdir(lumi_dir)
        if f.endswith(".json")
    ]

    if not files:
        print(f"❌ No LumiList JSONs found in {lumi_dir}")
        return None

    merged = {}

    for f in files:
        with open(f) as jf:
            data = json.load(jf)

        for run, ranges in data.items():
            run = str(run)
            merged.setdefault(run, [])

            for lo, hi in ranges:
                merged[run].append((lo, hi))

    # Compact ranges per run
    compact = {}
    for run, ranges in merged.items():
        ls = []
        for lo, hi in ranges:
            ls.extend(range(lo, hi + 1))

        ls = sorted(set(ls))
        out = []

        start = prev = ls[0]
        for x in ls[1:]:
            if x == prev + 1:
                prev = x
            else:
                out.append([start, prev])
                start = prev = x
        out.append([start, prev])

        compact[run] = out

    outdir = f"lumi/{era}"
    os.makedirs(outdir, exist_ok=True)

    outpath = os.path.join(outdir, f"{dataset_name}_lumis.json")
    with open(outpath, "w") as f:
        json.dump(compact, f, indent=2, sort_keys=True)

    print(f"📦 Dataset LumiList written: {outpath}")
    return outpath



def intersect_with_golden_json(dataset_lumi_json, golden_json):
    """
    Intersect dataset lumis with Golden JSON using compareJSON.py
    """
    outpath = dataset_lumi_json.replace(".json", "_golden.json")

    cmd = [
        "compareJSON.py",
        "--and",
        dataset_lumi_json,
        golden_json,
        outpath
    ]

    subprocess.check_call(cmd)
    print(f"✨ Golden LumiList written: {outpath}")
    return outpath


# -----------------------------------------------------------
# DAS helpers
# -----------------------------------------------------------
def fetch_das_file_list(dataset):
    print(f"📡 DAS query:\n  {dataset}")
    query = f"file dataset={dataset}"

    try:
        output = subprocess.check_output(
            ["dasgoclient", "-query", query],
            text=True
        ).strip().split("\n")

        files = [
            "root://cms-xrd-global.cern.ch/" + f.strip()
            for f in output if f.strip()
        ]

        print(f"  → Found {len(files)} files\n")
        return files

    except subprocess.CalledProcessError as e:
        print(f"❌ DAS error: {e}")
        return []


def save_file_list(era, dataset_name, file_list):
    outdir = f"filelists/{era}"
    os.makedirs(outdir, exist_ok=True)

    if not file_list:
        return None

    path = os.path.join(outdir, f"{dataset_name}.json")
    with open(path, "w") as f:
        json.dump({"files": file_list}, f, indent=2)

    print(f" saved: {path}")
    return path


def load_file_list(era, dataset_name):
    path = f"filelists/{era}/{dataset_name}.json"
    if not os.path.exists(path):
        return None

    with open(path) as f:
        return json.load(f)["files"]


# -----------------------------------------------------------
# Incremental processing helpers
# -----------------------------------------------------------
def load_partial_results(era, dataset_name):
    path = f"results/{era}/{dataset_name}.json"

    if not os.path.exists(path):
        return {
            "dataset": dataset_name,
            "run_min": None,
            "run_max": None,
            "files_processed": []
        }

    with open(path) as f:
        return json.load(f)


def save_partial_results(era, dataset_name, data):
    outdir = f"results/{era}"
    os.makedirs(outdir, exist_ok=True)

    path = os.path.join(outdir, f"{dataset_name}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved: {path}\n")


# -----------------------------------------------------------
# File-level processing
# -----------------------------------------------------------
def process_file(data_file, timeout=60):
    """
    Extract run range and lumisections from a DATA NanoAOD file.
    """
    try:
        with uproot.open(data_file, timeout=timeout) as f:
            runs_tree = f["Runs"]
            lumi_tree = f["LuminosityBlocks"]

            runs = runs_tree["run"].array(library="np")
            run_min = int(runs.min())
            run_max = int(runs.max())

            lumi_runs = lumi_tree["run"].array(library="np")
            lumi_secs = lumi_tree["luminosityBlock"].array(library="np")

            lumis = list({
                (int(r), int(ls))
                for r, ls in zip(lumi_runs, lumi_secs)
            })
            print(f"Processed file: {data_file}")
            return {
                "file_run_min": run_min,
                "file_run_max": run_max,
                "lumis": lumis
            }

    except Exception as e:
        print(f"  ⚠️ Failed to read: {data_file}")
        print(f"     Reason: {e}")
        return None


# -----------------------------------------------------------
# Dataset-level processing
# -----------------------------------------------------------
def process_dataset(era, dataset_name, dataset_query):
    print(f"\n🚀 Dataset: {dataset_name}")

    file_list = load_file_list(era, dataset_name)
    if file_list is None:
        file_list = fetch_das_file_list(dataset_query)
        save_file_list(era, dataset_name, file_list)

    data = load_partial_results(era, dataset_name)
    processed = {f["file"] for f in data["files_processed"]}
    remaining = [f for f in file_list if f not in processed]
    num_remaining_files = len(remaining)

    for fpath in remaining:
        info = process_file(fpath)
        if info is None:
            continue
        else:
            num_remaining_files -= 1

        lumi_list = LumiList(lumis=info["lumis"])

        lumi_outdir = f"lumi/{era}/{dataset_name}"
        os.makedirs(lumi_outdir, exist_ok=True)

        fname = os.path.basename(fpath).replace(".root", ".json")
        lumi_list.writeJSON(os.path.join(lumi_outdir, fname))

        data["run_min"] = (
            info["file_run_min"]
            if data["run_min"] is None
            else min(data["run_min"], info["file_run_min"])
        )
        data["run_max"] = (
            info["file_run_max"]
            if data["run_max"] is None
            else max(data["run_max"], info["file_run_max"])
        )

        data["files_processed"].append({
            "file": fpath,
            "file_run_min": info["file_run_min"],
            "file_run_max": info["file_run_max"]
        })

        save_partial_results(era, dataset_name, data)

    # --------------------------------------------------
    # Guard: only finalize lumi if ALL files processed
    # --------------------------------------------------
    if num_remaining_files > 0:
        print(
            f"⚠️ Dataset incomplete: "
            f"{num_remaining_files}/{len(file_list)} files still to be processed. "
            "Skipping lumi finalization."
        )
        return
    # ---- Final lumi computation (skip if already done) ----
    summary_path = f"lumi/{era}/{dataset_name}_summary.json"
    if os.path.exists(summary_path):
        print(f"ℹ️ Lumi summary already exists: {summary_path}")
        return

    dataset_lumi_json = build_dataset_lumilist(era, dataset_name)
    if dataset_lumi_json is None:
        return

    golden_json = f"goldenJSONs/{era}.json"
    golden_lumi_json = intersect_with_golden_json(
        dataset_lumi_json, golden_json
    )


    dataset_lumi = LumiList(filename=golden_lumi_json)
    n_lumis = sum(
        hi - lo + 1
        for runs in dataset_lumi.getCompactList().values()
        for lo, hi in runs
    )

    summary = {
        "dataset": dataset_name,
        "run_min": data["run_min"],
        "run_max": data["run_max"],
        "n_lumisections": n_lumis,
    }

    os.makedirs(f"lumi/{era}", exist_ok=True)
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"🧾 Lumi summary written: {summary_path}")


# -----------------------------------------------------------
# Main driver
# -----------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--era", required=True)
    args = parser.parse_args()

    datasets = load_era_json(args.era)

    if "Data" not in datasets:
        print("❌ No Data section in era JSON")
        return

    for name, query in datasets["Data"].items():
        print(f"\n=== Category: {name} ===")
        process_dataset(args.era, name, query)


if __name__ == "__main__":
    main()

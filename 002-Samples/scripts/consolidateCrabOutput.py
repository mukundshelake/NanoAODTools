#!/usr/bin/env python3
"""
Move each file listed in a --generateCrabDatasetJSON output JSON from CRAB's
auto-nested output layout into the clean canonical structure this pipeline
actually wants downstream, then delete the old per-dataset subtree once
every listed file for that dataset has been confirmed at its new location.

CRAB's own output layout unavoidably nests every file under
{primaryDataset}/{outputDatasetTag}/{timestamp}/{counter}/ on top of the
era/DataMC/group/dataset path this pipeline already controls (see
submit_preselection_flexible.py) -- none of that nesting carries meaning
once a single winning submission wave has already been picked out by
generateCrabDatasetJSON's own dedup logic, and leaving it in place is what
was pushing preselection output LFNs toward CRAB's 255-char limit even
after shortening the outputDatasetTag. This script removes it by
physically relocating files into a flat, predictable layout:

    {destinationBase}/{DataMC}/{group}/{dataset}/{filename}

Deleting the old subtree is gated on every one of that dataset's listed
files being confirmed present at the new location first. A file already
present at its destination (e.g. from a prior run of this same script that
got interrupted) is treated as already done rather than re-moved, so this
script is safe to interrupt and re-run.

Usage:
    python3 consolidateCrabOutput.py --datasetJSON <crabOutput_{era}_datasets.json> \\
        --sourceBase <{STORAGE}/{config_hash}/{era}> \\
        --destinationBase <{STORAGE}/preselection/{tag}/{config_hash}/{era}> \\
        [--include ...] [--exclude ...]
"""

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Consolidate CRAB output into a clean flat layout.")
    parser.add_argument("--datasetJSON", required=True, help="Path to crabOutput_{era}_datasets.json (from --generateCrabDatasetJSON).")
    parser.add_argument("--sourceBase", required=True,
                        help="Root of the raw CRAB output tree ({STORAGE}/{config_hash}/{era}) -- its per-dataset "
                             "subtrees are deleted once consolidated.")
    parser.add_argument("--destinationBase", required=True,
                        help="Root of the clean target tree ({STORAGE}/preselection/{tag}/{config_hash}/{era}).")
    parser.add_argument("--include", help='Regex applied to "DataMC/group/dataset"; only matching triples are processed.')
    parser.add_argument("--exclude", help='Regex applied to "DataMC/group/dataset"; matching triples are skipped.')
    args = parser.parse_args()

    with open(args.datasetJSON) as f:
        dataset_data = json.load(f)

    include_pat = re.compile(args.include) if args.include else None
    exclude_pat = re.compile(args.exclude) if args.exclude else None

    total_datasets, consolidated_datasets, failed_datasets = 0, 0, 0

    for DataMC, groups in dataset_data.items():
        for group, datasets in groups.items():
            for dataset, files in datasets.items():
                label = f"{DataMC}/{group}/{dataset}"
                if include_pat and not include_pat.search(label):
                    continue
                if exclude_pat and exclude_pat.search(label):
                    continue
                filepaths = list(files.keys()) if isinstance(files, dict) else list(files)
                total_datasets += 1
                if not filepaths:
                    # 0 healthy files for this dataset (e.g. a tight selection with no
                    # surviving events) -- nothing to move, but the old subtree (holding
                    # only confirmed-unhealthy files) should still be cleaned up rather
                    # than left behind as leftover debris.
                    old_dataset_root = Path(args.sourceBase) / DataMC / group / dataset
                    if old_dataset_root.exists():
                        shutil.rmtree(old_dataset_root)
                        print(f"  [OK] {label}: 0 healthy files, removed old subtree {old_dataset_root}")
                    else:
                        print(f"  [OK] {label}: 0 healthy files, old subtree already gone")
                    consolidated_datasets += 1
                    continue

                dest_dir = Path(args.destinationBase) / DataMC / group / dataset
                dest_dir.mkdir(parents=True, exist_ok=True)

                ok = True
                seen_basenames = {}
                for filepath in filepaths:
                    basename = os.path.basename(filepath)
                    if basename in seen_basenames:
                        print(f"  [ERROR] {label}: two source files map to the same destination basename "
                              f"{basename!r}: {seen_basenames[basename]} and {filepath}")
                        ok = False
                        continue
                    seen_basenames[basename] = filepath
                    dest_path = dest_dir / basename

                    if dest_path.exists():
                        continue  # already consolidated by a prior (possibly interrupted) run

                    if not os.path.exists(filepath):
                        print(f"  [ERROR] {label}: source file missing and not yet at destination: {filepath}")
                        ok = False
                        continue

                    src_size = os.path.getsize(filepath)
                    shutil.move(filepath, str(dest_path))
                    if os.path.getsize(dest_path) != src_size:
                        print(f"  [ERROR] {label}: size mismatch after move for {basename} "
                              f"(src {src_size}, dest {os.path.getsize(dest_path)})")
                        ok = False

                if ok:
                    old_dataset_root = Path(args.sourceBase) / DataMC / group / dataset
                    if old_dataset_root.exists():
                        shutil.rmtree(old_dataset_root)
                        print(f"  [OK] {label}: consolidated {len(filepaths)} files, removed old subtree {old_dataset_root}")
                    else:
                        print(f"  [OK] {label}: consolidated {len(filepaths)} files (old subtree already gone)")
                    consolidated_datasets += 1
                else:
                    failed_datasets += 1
                    print(f"  [SKIP CLEANUP] {label}: not all files verified at destination -- old subtree left in place")

    print(f"\n{'='*60}")
    print(f"Summary: {consolidated_datasets}/{total_datasets} datasets consolidated, {failed_datasets} failed.")

    if failed_datasets:
        sys.exit(1)


if __name__ == "__main__":
    print("Running consolidateCrabOutput.py")
    main()
    print("Finished consolidateCrabOutput.py")

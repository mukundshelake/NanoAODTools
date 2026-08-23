#!/usr/bin/env python3
"""
Move each file listed in a --generateCrabDatasetJSON output JSON out of
CRAB's auto-nested output layout into a clean flat one, then delete the
leftover nested directories once every listed file for that dataset has
been confirmed at its new location.

Unlike 002-Samples' version of this script, 003-ObjectSelectionI's own
outLFNDirBase already points CRAB at the correct final path for each
dataset -- {STORAGE}/selectionI/{tag}/{config_hash}/{era}/{DataMC}/
{group}/{dataset}/ -- since there's no DBS-registered input dataset here
to make the LFN long in the first place (submission uses
Data.userInputFiles, not Data.inputDataset). CRAB still piles its own
{primaryDataset}/{outputDatasetTag}/{timestamp}/{counter}/ nesting
*underneath* that already-correct path, though, so the source root and
the destination root for consolidation are the SAME directory here, not
two separate trees as in 002-Samples. Concretely, this script moves e.g.

    {base}/{DataMC}/{group}/{dataset}/{primaryDataset}/{tag}/{ts}/{n}/{filename}

to

    {base}/{DataMC}/{group}/{dataset}/{filename}

and then removes the now-empty {primaryDataset}/ subtree -- never the
{dataset}/ directory itself, since that IS the destination.

Deleting the leftover nesting is gated on every one of that dataset's
listed files being confirmed present at the new flat location first. A
file already present there (e.g. from a prior run of this same script
that got interrupted) is treated as already done rather than re-moved,
so this script is safe to interrupt and re-run.

Usage:
    python3 consolidateCrabOutput.py --datasetJSON <crabOutput_{era}_datasets.json> \\
        --base <{STORAGE}/selectionI/{tag}/{config_hash}/{era}> \\
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
    parser = argparse.ArgumentParser(description="Consolidate CRAB output into a clean flat layout, in place.")
    parser.add_argument("--datasetJSON", required=True, help="Path to crabOutput_{era}_datasets.json (from --generateCrabDatasetJSON).")
    parser.add_argument("--base", required=True,
                        help="Root of the dataset tree, both source and destination "
                             "({STORAGE}/selectionI/{tag}/{config_hash}/{era}) -- CRAB's own nested "
                             "subdirectories under each dataset are removed once consolidated.")
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

                dest_dir = Path(args.base) / DataMC / group / dataset

                if not filepaths:
                    # 0 healthy files for this dataset (e.g. a tight selection with no
                    # surviving events) -- nothing to move, but any leftover nested
                    # directories (holding only confirmed-unhealthy files) should still
                    # be cleaned up rather than left behind as debris.
                    removed_any = False
                    if dest_dir.exists():
                        for entry in dest_dir.iterdir():
                            if entry.is_dir():
                                shutil.rmtree(entry)
                                removed_any = True
                    print(f"  [OK] {label}: 0 healthy files"
                          f"{', removed leftover nested directories' if removed_any else ''}")
                    consolidated_datasets += 1
                    continue

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
                    # Everything this dataset's JSON entry listed is now confirmed sitting
                    # directly in dest_dir. Any subdirectory still there is leftover CRAB
                    # nesting (primaryDataset/outputDatasetTag/timestamp/counter/) with
                    # nothing of value left inside it -- safe to remove. dest_dir itself
                    # is the destination, not the old location, so it is never removed.
                    removed_any = False
                    for entry in dest_dir.iterdir():
                        if entry.is_dir():
                            shutil.rmtree(entry)
                            removed_any = True
                    if removed_any:
                        print(f"  [OK] {label}: consolidated {len(filepaths)} files, removed leftover nested directories")
                    else:
                        print(f"  [OK] {label}: consolidated {len(filepaths)} files (no leftover nested directories)")
                    consolidated_datasets += 1
                else:
                    failed_datasets += 1
                    print(f"  [SKIP CLEANUP] {label}: not all files verified at destination -- leftover nested directories left in place")

    print(f"\n{'='*60}")
    print(f"Summary: {consolidated_datasets}/{total_datasets} datasets consolidated, {failed_datasets} failed.")

    if failed_datasets:
        sys.exit(1)


if __name__ == "__main__":
    print("Running consolidateCrabOutput.py")
    main()
    print("Finished consolidateCrabOutput.py")

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

A dataset the JSON lists but which exists at neither source nor destination
is reported once as [STALE] rather than as one "source file missing" error
per file: that pattern means the JSON predates the current state (its phase
has already been consolidated and moved off), not that data was lost.

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

    total_datasets, consolidated_datasets, failed_datasets, stale_datasets = 0, 0, 0, 0

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

                # A dataset whose source subtree is gone *and* which has nothing at
                # its destination was neither left half-moved nor is it waiting to be
                # moved -- the JSON is describing something that no longer exists in
                # either place. In practice that means the JSON is stale: it was built
                # during an earlier campaign phase, and that phase's output has since
                # been consolidated, transferred off, and cleaned up. Reporting this
                # per *file* buries the one fact that matters under hundreds of
                # identical lines (confirmed live: 228 "source file missing" errors
                # for five long-since-transferred Data_mu datasets, while the MC_alt
                # datasets actually being asked for went unmentioned). Diagnose it
                # once, per dataset, and say what it usually means.
                dest_dir = Path(args.destinationBase) / DataMC / group / dataset
                old_dataset_root = Path(args.sourceBase) / DataMC / group / dataset
                if not old_dataset_root.exists() and not any(
                    (dest_dir / os.path.basename(fp)).exists() for fp in filepaths
                ):
                    print(f"  [STALE] {label}: listed in the JSON with {len(filepaths)} file(s), but present "
                          f"neither at source ({old_dataset_root}) nor at destination ({dest_dir}) -- "
                          f"this dataset was most likely consolidated and cleaned up already, meaning the "
                          f"JSON predates the current state. Regenerate it with "
                          f"--generateCrabDatasetJSON --force.")
                    stale_datasets += 1
                    failed_datasets += 1
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
    if stale_datasets:
        print(f"{stale_datasets} of those failures were datasets present at neither source nor destination. "
              f"If this run consolidated nothing at all, the JSON almost certainly describes an earlier "
              f"campaign phase whose output has already been moved off -- regenerate it with "
              f"--generateCrabDatasetJSON --force and re-run.")

    if failed_datasets:
        sys.exit(1)


if __name__ == "__main__":
    print("Running consolidateCrabOutput.py")
    main()
    print("Finished consolidateCrabOutput.py")

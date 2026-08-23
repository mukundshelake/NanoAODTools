#!/usr/bin/env python
"""Flatten leftover CRAB output nesting under preselection dataset folders.

Expected final layout: {baseFolder}/{DataMC}/{Group}/{dataset}/*.root

CRAB output is instead sometimes left as:
  {dataset}/{primary_dataset_name}/{crab_task_name}/{timestamp}/0000/*.root
  {dataset}/{primary_dataset_name}/{crab_task_name}/{timestamp}/0000/log/*.log.tar.gz

This script finds every "0000" chunk directory under baseFolder, moves its
ROOT files up into the dataset folder (4 levels above "0000"), deletes the
CRAB job-log tarballs, and removes the now-empty intermediate directories.
"""

import argparse
import os
import shutil


def find_chunk_dirs(baseFolder):
    chunkDirs = []
    for dirpath, dirnames, _ in os.walk(baseFolder):
        for name in dirnames:
            if name == "0000":
                chunkDirs.append(os.path.join(dirpath, name))
    return sorted(chunkDirs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseFolder", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not os.path.isdir(args.baseFolder):
        raise SystemExit(f"Error: Directory does not exist: {args.baseFolder}")

    chunkDirs = find_chunk_dirs(args.baseFolder)
    print(f"Found {len(chunkDirs)} '0000' chunk directories under: {args.baseFolder}")
    print("---")

    movedTotal = 0
    deletedLogsTotal = 0

    for chunkDir in chunkDirs:
        # chunkDir = .../{dataset}/{primary_dataset_name}/{crab_task_name}/{timestamp}/0000
        timestampDir = os.path.dirname(chunkDir)
        taskDir = os.path.dirname(timestampDir)
        primaryDatasetDir = os.path.dirname(taskDir)
        datasetDir = os.path.dirname(primaryDatasetDir)

        print(f"Consolidating: {chunkDir}")
        print(f"  → into dataset dir: {datasetDir}")

        rootFiles = [f for f in os.listdir(chunkDir) if f.endswith(".root")]
        for name in rootFiles:
            src = os.path.join(chunkDir, name)
            dst = os.path.join(datasetDir, name)
            if os.path.exists(dst):
                raise SystemExit(f"Refusing to overwrite existing file: {dst}")
            if not args.dry_run:
                shutil.move(src, dst)
            movedTotal += 1
        print(f"  → moved {len(rootFiles)} ROOT files")

        logDir = os.path.join(chunkDir, "log")
        if os.path.isdir(logDir):
            nLogs = len(os.listdir(logDir))
            print(f"  → deleting log dir ({nLogs} files)")
            deletedLogsTotal += nLogs
            if not args.dry_run:
                shutil.rmtree(logDir)

        if not args.dry_run:
            # remove now-empty intermediate dirs: 0000, timestamp, crab_task, primary_dataset
            for d in (chunkDir, timestampDir, taskDir, primaryDatasetDir):
                os.rmdir(d)

    print("---")
    print(f"Total ROOT files moved: {movedTotal}")
    print(f"Total log files deleted: {deletedLogsTotal}")
    if args.dry_run:
        print("(dry run — nothing was actually changed)")


if __name__ == "__main__":
    main()

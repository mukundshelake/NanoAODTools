#!/usr/bin/env python
"""Scan a directory tree for ROOT files with no events in the 'Events' TTree.

Equivalent to check_empty_root_files.sh but uses uproot instead of shelling
out to `root`. Intended to be run inside the latestcoffea conda env.
"""

import argparse
import os

import uproot


def events_entries(path):
    """Return the number of entries in the 'Events' tree. Raises on unreadable
    files or files missing an 'Events' tree."""
    with uproot.open(path) as f:
        return f["Events"].num_entries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseFolder", required=True)
    parser.add_argument("--delete", action="store_true", dest="deleteEmpty")
    args = parser.parse_args()

    if not os.path.isdir(args.baseFolder):
        raise SystemExit(f"Error: Directory does not exist: {args.baseFolder}")

    print(f"Scanning for ROOT files in: {args.baseFolder}")
    print("---")

    rootFiles = []
    for dirpath, _, filenames in os.walk(args.baseFolder):
        for name in filenames:
            if name.endswith(".root"):
                rootFiles.append(os.path.join(dirpath, name))
    rootFiles.sort()

    emptyCount = 0
    totalCount = 0

    for path in rootFiles:
        totalCount += 1
        print(f"Checking [{totalCount}]: {path}")

        try:
            entries = events_entries(path)
        except uproot.KeyInFileError:
            entries = 0
            print("  → no 'Events' tree found")
        except Exception as exc:
            print(f"  → ERROR reading file ({exc})")
            continue

        if entries == 0:
            emptyCount += 1
            print("  → EMPTY (0 events)")
            if args.deleteEmpty:
                print("  → DELETING...")
                os.remove(path)
        else:
            print(f"  → OK ({entries} events)")

    print("---")
    print(f"Total ROOT files: {totalCount}")
    print(f"Empty ROOT files: {emptyCount}")

    if args.deleteEmpty:
        print(f"Deleted: {emptyCount} empty files")


if __name__ == "__main__":
    main()

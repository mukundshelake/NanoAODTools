#!/usr/bin/env python3
"""
Worker script to extract BDT feature/target branches into parquet files.

Reads the BDTVariables ROOT files for one dataset (as listed in a pre-built
process list JSON), pulls out the configured BDT feature branches plus the
`y` target branch via uproot, and streams them out to numbered parquet part
files -- one dataset can produce several `_part{N}.parquet` files if it has
more events than fit in a single chunk.

Usage:
    python scripts/extractParquet.py --processListJSON <json_file> [--workers N] [--force] [--filter ...]

Options:
    --processListJSON: Path to JSON file with per-dataset task list (required)
    --workers: Number of parallel worker processes, one per dataset (default: 8)
    --filter: Filter by era[/DataMC[/group[/dataset]]], use * as wildcard
    --force: Reprocess a dataset even if parquet output already exists
    --sample: Process only the first file of each dataset (isSample=True)
"""

import os

# ---------------------------------------------------------------------------
# Limit background thread pools BEFORE any library imports, so many worker
# processes reading/writing in parallel don't oversubscribe the machine with
# BLAS threads spawned by numpy (same rationale as runBDTVariables.py).
for _thread_env in [
    "OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS",
]:
    if _thread_env not in os.environ:
        os.environ[_thread_env] = "1"
# ---------------------------------------------------------------------------

import argparse
import glob
import json
import logging
import sys
import traceback

import awkward as ak
import uproot
from tqdm import tqdm


def matches_filter(filters, era, data_mc=None, group=None, dataset=None):
    """Check if era/DataMC/group/dataset matches any of the provided filters."""
    if not filters:
        return True
    for f in filters:
        parts = f.split('/')
        if parts[0] not in ('*', era):
            continue
        if data_mc is not None and len(parts) >= 2 and parts[1] not in ('*', data_mc):
            continue
        if group is not None and len(parts) >= 3 and parts[2] not in ('*', group):
            continue
        if dataset is not None and len(parts) >= 4 and parts[3] not in ('*', dataset):
            continue
        return True
    return False


def process_dataset(data):
    """Extract one dataset's BDT feature/target branches to parquet part files.

    Reads each source ROOT file in full and accumulates chunks across files,
    flushing to a numbered `_part{N}.parquet` once the accumulated row count
    would reach `maxEvents` -- unlike `uproot.iterate`'s `step_size`, this
    carries a file's remainder forward into the next file's chunk instead of
    resetting at every file boundary, so a dataset made of many small files
    doesn't get fragmented into one tiny part per file.

    Args:
        data: Dictionary containing:
            - era, DataMC, group, dataset: identifying labels (for logging only)
            - files: list of source *_BDTVars.root file paths
            - outputDir: where to write {dataset}_part{N}.parquet
            - columns: branch names to read (BDT features + target branch)
            - maxEvents: rows accumulated per output part file

    Returns:
        Number of output part files written (0 if the dataset had no events),
        or None if an error occurred.
    """
    era        = data["era"]
    DataMC     = data["DataMC"]
    group      = data.get("group", None)
    dataset    = data["dataset"]
    files      = data["files"]
    outputDir  = data["outputDir"]
    columns    = data["columns"]
    maxEvents  = data["maxEvents"]

    os.makedirs(outputDir, exist_ok=True)

    n_parts = 0
    pending = []
    pending_rows = 0

    def flush():
        nonlocal n_parts, pending, pending_rows
        if pending_rows == 0:
            return
        chunk = ak.concatenate(pending) if len(pending) > 1 else pending[0]
        out_path = os.path.join(outputDir, f"{dataset}_part{n_parts}.parquet")
        ak.to_parquet(chunk, out_path)
        n_parts += 1
        pending = []
        pending_rows = 0

    try:
        for f in files:
            tree = uproot.open(f)["Events"]
            if tree.num_entries == 0:
                continue
            pending.append(tree.arrays(columns, library="ak"))
            pending_rows += tree.num_entries
            if pending_rows >= maxEvents:
                flush()
        flush()
        logging.info(
            f"Finished {dataset} ({DataMC}/{group}, {era}): "
            f"{n_parts} part file(s) written to {outputDir}"
        )
        return n_parts
    except Exception as e:
        logging.error(f"Error processing dataset {dataset} ({DataMC}, {era}): {e}")
        logging.error(traceback.format_exc())
        return None


if __name__ == "__main__":
    from multiprocessing import Pool, set_start_method

    try:
        set_start_method('spawn')
    except RuntimeError:
        pass

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info("Starting BDT parquet extraction script.")

    parser = argparse.ArgumentParser(description="Extract BDT feature/target branches to parquet.")
    parser.add_argument('--processListJSON', '-i', required=True,
                        help='Path to a JSON file containing a list of per-dataset task dicts.')
    parser.add_argument('--workers', '-w', type=int, default=8, help='Number of parallel worker processes')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard.')
    parser.add_argument('--force', action='store_true',
                       help='Reprocess a dataset even if parquet output already exists.')
    parser.add_argument('--sample', action='store_true',
                       help='Process only the first file of each dataset (isSample=True).')
    args = parser.parse_args()

    try:
        with open(args.processListJSON, 'r') as f:
            process_list = json.load(f)
    except FileNotFoundError:
        logging.error(f"Process list JSON not found: {args.processListJSON}")
        sys.exit(1)

    logging.info(f"Loaded {len(process_list)} dataset tasks from {args.processListJSON}")
    if len(process_list) == 0:
        logging.info("No datasets to process. Exiting.")
        sys.exit(0)

    tasks_to_run = []
    pre_skipped  = 0
    for data in process_list:
        if not matches_filter(args.filter,
                              data["era"],
                              data.get("DataMC"),
                              data.get("group"),
                              data.get("dataset")):
            pre_skipped += 1
            continue
        if args.sample and not data.get("isSample", False):
            pre_skipped += 1
            continue
        if args.sample:
            data = dict(data, files=data["files"][:1])
        if not args.force:
            existing = glob.glob(os.path.join(data["outputDir"], f"{data['dataset']}_part*.parquet"))
            if existing:
                pre_skipped += 1
                continue
        tasks_to_run.append(data)

    logging.info(f"Pre-filtering: {len(tasks_to_run)} datasets to run, {pre_skipped} already done / filtered out.")
    if len(tasks_to_run) == 0:
        logging.info("Nothing to do. Exiting.")
        sys.exit(0)
    logging.info("Starting parallel processing of datasets...")

    num_cores = args.workers
    with Pool(num_cores, maxtasksperchild=1) as pool:
        results = list(tqdm(pool.imap(process_dataset, tasks_to_run),
                            total=len(tasks_to_run),
                            desc="Processing datasets"))

    succeeded = sum(1 for r in results if isinstance(r, int) and r >= 0)
    failed    = sum(1 for r in results if r is None)
    total_parts = sum(r for r in results if isinstance(r, int))
    logging.info(f"Processing complete: {succeeded} datasets succeeded ({total_parts} part files written), "
                 f"{failed} failed out of {len(results)} total ({pre_skipped} pre-skipped).")
    logging.info("Finished all processing.")

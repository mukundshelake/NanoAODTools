#!/usr/bin/env python3
"""
Worker script to write the trained BDT's score onto the 004B ROOT files.

Runs BDTScoreModule over each input file through NanoAODTools' PostProcessor,
one worker process per file, exactly as 004B's runBDTVariables.py does. Output
goes to the `bdtScore` stage and keeps the full NanoAOD structure -- the Runs
tree in particular, which carries genEventCount/genEventSumw that downstream
normalisation depends on. An uproot copy of the Events tree would silently drop
it.

Usage:
    python scripts/applyBDT.py --processListJSON <json> [--workers N] [--force] [--filter ...]

Options:
    --processListJSON: JSON file with the per-file task list (required)
    --workers: parallel worker processes, one per file (default: 8)
    --filter: era[/DataMC[/group[/dataset]]], '*' as wildcard
    --force: reprocess a file even if its output already exists
"""

import os

# ---------------------------------------------------------------------------
# Cap background thread pools BEFORE importing numpy/xgboost. Each worker
# process would otherwise spawn its own BLAS and XGBoost thread pools, and with
# --workers 8 that oversubscribes the machine badly -- the same reason
# runBDTVariables.py and extractParquet.py do this.
for _thread_env in [
    "OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS",
]:
    if _thread_env not in os.environ:
        os.environ[_thread_env] = "1"
# ---------------------------------------------------------------------------

import argparse
import json
import logging
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gROOT.SetBatch(True)

from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor  # noqa: E402
from modules.BDTScoreModule import BDTScoreModule  # noqa: E402


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


def expected_output(output_dir, input_file):
    """Where PostProcessor will write this input file's output.

    Constructed with no postfix, so NanoAODTools falls back to its default of
    '_Skim' -- the same reasoning as 004B's note about output naming.
    """
    stem = Path(input_file).stem
    return Path(output_dir) / f"{stem}_Skim.root"


def process_file(data):
    """Score one ROOT file.

    Args:
        data: dict with era, DataMC, group, dataset (labels), file, outputDir,
              modelPath, and branchName.

    Returns:
        True on success, False if the output already existed and --force was not
        given, None on error.
    """
    era = data["era"]
    DataMC = data["DataMC"]
    group = data.get("group")
    dataset = data["dataset"]
    input_file = data["file"]
    output_dir = data["outputDir"]
    model_path = data["modelPath"]
    branch_name = data.get("branchName", "BDTScore")

    os.makedirs(output_dir, exist_ok=True)

    target = expected_output(output_dir, input_file)
    if target.exists() and not data.get("force"):
        logging.info(f"    Output exists, skipping: {target}")
        return False

    try:
        module = BDTScoreModule(model_path, branch_name)
        post_processor = PostProcessor(
            output_dir,
            [input_file],
            modules=[module],
            noOut=False,
            justcount=False,
            compression="ZLIB:9",
        )
        post_processor.run()
        logging.info(f"Finished {Path(input_file).name} "
                     f"({dataset}, {group}/{DataMC}, {era})")
        return True
    except Exception as exc:  # noqa: BLE001
        logging.error(f"Error scoring {input_file} in {dataset} ({DataMC}, {era}): {exc}")
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
    logging.info("Starting BDT scoring script.")

    parser = argparse.ArgumentParser(description="Write BDTScore onto 004B ROOT files.")
    parser.add_argument('--processListJSON', '-i', required=True,
                        help='JSON file with the per-file task list')
    parser.add_argument('--workers', '-w', type=int, default=8,
                        help='Number of parallel worker processes')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                        help="era[/DataMC[/group[/dataset]]], '*' as wildcard")
    parser.add_argument('--force', action='store_true',
                        help='Reprocess a file even if its output exists')
    args = parser.parse_args()

    with open(args.processListJSON) as handle:
        tasks = json.load(handle)

    selected = [t for t in tasks
                if matches_filter(args.filter, t["era"], t["DataMC"],
                                  t.get("group"), t["dataset"])]
    for task in selected:
        task["force"] = bool(args.force)

    logging.info(f"{len(selected)} of {len(tasks)} task(s) selected")
    if not selected:
        logging.warning("Nothing to do.")
        sys.exit(0)

    with Pool(args.workers, maxtasksperchild=1) as pool:
        results = pool.map(process_file, selected)

    ok = sum(1 for r in results if r is True)
    skipped = sum(1 for r in results if r is False)
    failed = sum(1 for r in results if r is None)
    logging.info(f"Done: {ok} scored, {skipped} skipped, {failed} failed")
    sys.exit(1 if failed else 0)

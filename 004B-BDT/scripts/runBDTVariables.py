#!/usr/bin/env python3
"""
Worker script to run BDT variable computation from a pre-built process list.

This script processes files with the BDTvariableModule, computing event-shape
variables and Fox-Wolfram moments for BDT training and testing.

Usage:
    python scripts/runBDTVariables.py --processListJSON <json_file> [--workers N] [--force] [--filter ...]

Options:
    --processListJSON: Path to JSON file with task list (required)
    --workers: Number of parallel workers (default: 15)
    --filter: Filter by era[/DataMC[/group[/dataset]]], use * as wildcard
    --force: Process files even if output already exists
    --sample: Process only first file of each dataset (isSample=True)
"""

import os, json, argparse, logging, sys, traceback

# ---------------------------------------------------------------------------
# Limit background thread pools BEFORE any library imports.
# Without this, numpy spawns ~20 BLAS threads that compete with ROOT's
# TTreeReader memory allocator in worker processes, causing segfaults.
for _thread_env in [
    "OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS",
]:
    if _thread_env not in os.environ:
        os.environ[_thread_env] = "1"
# ---------------------------------------------------------------------------

# Initialize ROOT in batch mode BEFORE any other ROOT imports
import ROOT
ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True

if ROOT.IsImplicitMTEnabled():
    ROOT.DisableImplicitMT()
ROOT.gErrorIgnoreLevel = ROOT.kWarning

from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from multiprocessing import Pool
from tqdm import tqdm
from modules.BDTvariableModule import BDTvariableModule


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


def _instantiate_module(module_name, era, config):
    """Instantiate a BDT module by name."""
    if module_name == "bdt_variables":
        # BDTvariableModule doesn't take parameters, just returns the module instance
        return BDTvariableModule()
    else:
        logging.error(f"Unknown module: {module_name}")
        return None


def process_file(data):
    """Process a single file through the BDT variable computation pipeline.
    
    Args:
        data: Dictionary containing:
            - era: Physics era (UL2016preVFP, UL2017, etc.)
            - DataMC: Type (Data, MC_mu, MC_e, etc.)
            - group: Process group
            - dataset: Dataset name
            - outputDir: Where to write output files
            - file: Input file path
            - cut_string: Optional cut string to pre-filter events
            - goldenJSON: Optional golden JSON for data selection
            - branchsel: Optional branch selection
            - modules: List of module configs with 'name' and 'config' keys
            - isSample: Whether this is a sample file (first file of dataset)
    
    Returns:
        True if processing succeeded
        False if 0 events pass the cut string
        None if an error occurred
    """
    era            = data["era"]
    DataMC         = data["DataMC"]
    group          = data.get("group", None)
    key            = data["dataset"]
    outputDir      = data["outputDir"]
    file           = data["file"]
    cut_string     = data.get("cut_string", None)
    goldenJSON     = data.get("goldenJSON", None)
    branchsel      = data.get("branchsel", None)
    module_configs = data.get("modules", [])

    os.makedirs(outputDir, exist_ok=True)

    # Guard against 0-event files (only relevant when a cut_string is given).
    if cut_string is not None:
        try:
            _cf = ROOT.TFile.Open(file, "READ")
            if _cf and not _cf.IsZombie():
                _ct = _cf.Get("Events")
                _n  = int(_ct.GetEntries(cut_string)) if (_ct is not None) else 0
                _ct = None
                _cf.Close()
                del _cf
                if _n == 0:
                    logging.info(
                        f"    0 events pass cut string in {file} "
                        f"(dataset={key}, {DataMC}, {era}); skipping."
                    )
                    return False
        except Exception as _e:
            logging.warning(f"    Pre-check failed for {file}: {_e}; proceeding anyway.")

    modules_with_names = []
    try:
        for entry in module_configs:
            mod_name = entry["name"]
            mod_conf = entry.get("config", {})
            loaded = _instantiate_module(mod_name, era, mod_conf)
            modules_with_names.append((mod_name, loaded))
    except Exception as e:
        logging.error(f"Failed to instantiate modules for {key} ({DataMC}): {e}")
        return None

    missing_modules = [name for name, m in modules_with_names if m is None]
    if missing_modules:
        logging.error(f"Failed to load modules {missing_modules} for {key} ({DataMC}). Skipping.")
        return None
    modules = [m for _, m in modules_with_names]

    try:
        post_processor = PostProcessor(
            outputDir,
            [file],
            cut=cut_string,
            jsonInput=goldenJSON,
            branchsel=branchsel,
            modules=modules,
            noOut=False,
            justcount=False,
            compression="ZLIB:9",
        )
        post_processor.run()
        logging.info(f"Finished processing {file} in {key} of {DataMC}")
        return True
    except Exception as e:
        logging.error(f"Error processing {file} in {key} of {DataMC}: {e}")
        logging.error(traceback.format_exc())
        return None


if __name__ == "__main__":
    from multiprocessing import set_start_method

    try:
        set_start_method('spawn')
    except RuntimeError:
        pass

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info("Starting BDT variable computation script.")

    parser = argparse.ArgumentParser(description="Run BDT variable computation from a pre-built process list.")
    parser.add_argument('--processListJSON', '-i', required=True,
                        help='Path to a JSON file containing a list of task dicts.')
    parser.add_argument('--workers', '-w', type=int, default=15, help='Number of parallel workers to use')
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard.')
    parser.add_argument('--force', action='store_true',
                       help='Process all files even if output already exists.')
    parser.add_argument('--sample', action='store_true',
                       help='Process only the first file of each dataset (isSample=True).')
    args = parser.parse_args()

    try:
        with open(args.processListJSON, 'r') as f:
            process_list = json.load(f)
    except FileNotFoundError:
        logging.error(f"Process list JSON not found: {args.processListJSON}")
        sys.exit(1)

    logging.info(f"Loaded {len(process_list)} tasks from {args.processListJSON}")
    if len(process_list) == 0:
        logging.info("No files to process. Exiting.")
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
        if not args.force:
            output_name = os.path.basename(data["file"]).replace(".root", "_BDTVars.root")
            output_path = os.path.join(data["outputDir"], output_name)
            if os.path.exists(output_path):
                pre_skipped += 1
                continue
        tasks_to_run.append(data)

    logging.info(f"Pre-filtering: {len(tasks_to_run)} tasks to run, {pre_skipped} already done / filtered out.")
    if len(tasks_to_run) == 0:
        logging.info("Nothing to do. Exiting.")
        sys.exit(0)
    logging.info("Starting parallel processing of datasets...")

    num_cores = args.workers
    with Pool(num_cores, maxtasksperchild=1) as pool:
        results = list(tqdm(pool.starmap(process_file,
                                         [(data,) for data in tasks_to_run],
                                         chunksize=1),
                            total=len(tasks_to_run),
                            desc="Processing datasets"))

    succeeded = sum(1 for r in results if r is True)
    zero_ev   = sum(1 for r in results if r is False)
    failed    = sum(1 for r in results if r is None)
    logging.info(f"Processing complete: {succeeded} succeeded, {failed} failed, {zero_ev} skipped (0 events) "
                 f"out of {len(results)} total ({pre_skipped} pre-skipped).")
    logging.info("Finished all processing.")

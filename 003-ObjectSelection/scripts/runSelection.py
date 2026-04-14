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
ROOT.gROOT.SetBatch(True)  # Disable graphics
ROOT.PyConfig.IgnoreCommandLineOptions = True

# Disable ROOT's multithreading to avoid conflicts with Python multiprocessing
ROOT.EnableImplicitMT(0)
ROOT.gErrorIgnoreLevel = ROOT.kWarning  # Suppress info messages

from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from multiprocessing import Pool
from tqdm import tqdm

from modules.LHEWeightSign import LHEWeightSignProducer
from modules.MuonIDWeight import MuonIDWeightProducer
from modules.MuonHLTWeight import MuonHLTWeightProducer
from modules.JetPUIDWeight import jetPUIdWeightProducer
from modules.bTaggingWeight import bTaggingWeightProducer
from modules.SelectedObjects import SelectedObjectsProducer


def _instantiate_module(module_name, era, key, config):
    if module_name == "lheWeightSign":
        return LHEWeightSignProducer(config)
    elif module_name == "muonID":
        return MuonIDWeightProducer(config)
    elif module_name == "muonHLT":
        return MuonHLTWeightProducer(config)
    elif module_name == "bTagging":
        return bTaggingWeightProducer(config, key)
    elif module_name == "jetPUID":
        return jetPUIdWeightProducer(era, key, config)
    elif module_name == "selectedObjects":
        return SelectedObjectsProducer(config)
    else:
        logging.error(f"Unknown module: {module_name}")
        return None


def process_file(data):
    era       = data["era"]
    DataMC    = data["DataMC"]
    key       = data["key"]
    outputDir = data["outputDir"]
    file      = data["file"]
    cut_string  = data.get("cut_string", None)
    goldenJSON  = data.get("goldenJSON", None)
    branchsel   = data.get("branchsel", None)
    module_configs = data.get("modules", [])
    # module_configs: list of {"name": <str>, "config": <dict>}

    os.makedirs(outputDir, exist_ok=True)

    modules_with_names = []
    try:
        for entry in module_configs:
            mod_name = entry["name"]
            mod_conf = entry.get("config", {})
            loaded = _instantiate_module(mod_name, era, key, mod_conf)
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

    # Set multiprocessing start method to 'spawn' for better ROOT compatibility
    try:
        set_start_method('spawn')
    except RuntimeError:
        pass

    # --- Configure Logging ---
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info("Starting selection processing script.")

    # --- Argument Parser ---
    parser = argparse.ArgumentParser(description="Run NanoAOD postprocessing from a pre-built process list.")
    parser.add_argument('--processListJSON', '-i', required=True,
                        help='Path to a JSON file containing a list of task dicts, each with keys: '
                             'stage, era, DataMC, key, outputDir, file, configDir')
    parser.add_argument('--workers', '-w', type=int, default=15, help='Number of parallel workers to use')

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
    logging.info("Starting parallel processing of datasets...")

    # --- Run the pool ---
    # maxtasksperchild=1: each worker handles exactly one file then exits.
    # This gives every file a fresh Python+ROOT process with zero stale
    # TTreeReader proxy state, eliminating the segfaults seen after ~6-7 files.
    num_cores = args.workers
    with Pool(num_cores, maxtasksperchild=1) as pool:
        results = list(tqdm(pool.imap_unordered(process_file, process_list),
                            total=len(process_list),
                            desc="Processing datasets"))

    succeeded = sum(1 for r in results if r is True)
    failed    = sum(1 for r in results if r is None)
    logging.info(f"Processing complete: {succeeded} succeeded, {failed} failed out of {len(results)} total.")
    logging.info("Finished all processing.")

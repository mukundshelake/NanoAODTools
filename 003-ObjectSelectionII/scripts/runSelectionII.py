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

# Disable ROOT's multithreading to avoid conflicts with Python multiprocessing.
# NOTE: EnableImplicitMT(0) means "enable with all hardware threads" — do NOT use it.
# Guard with IsImplicitMTEnabled() to avoid a noisy RuntimeWarning that fires
# every time a spawned worker calls DisableImplicitMT() when MT is already off.
if ROOT.IsImplicitMTEnabled():
    ROOT.DisableImplicitMT()
ROOT.gErrorIgnoreLevel = ROOT.kWarning  # Suppress info messages

from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from multiprocessing import Pool
from tqdm import tqdm
from modules.bTaggingWeight import bTaggingWeightProducer
from modules.JetPUIDWeight import jetPUIdWeightProducer
from modules.LHEWeightSign import LHEWeightSignProducer
from modules.MuonHLTWeight import MuonHLTWeightProducer
from modules.MuonIDWeight import MuonIDWeightProducer


def matches_filter(filters, era, data_mc=None, group=None, dataset=None):
    """Check if era/DataMC/group/dataset matches any of the provided filters.

    Each filter is a slash-separated string, e.g. 'UL2017/MC_mu/SingleTop/Tchannel'.
    Use '*' as a wildcard for any level.
    A shorter filter path matches all entries at deeper levels.
    """
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

def _instantiate_module(module_name, era, DataMC, key, config):
    # channel is the dataset name, used as the efficiency file stem (e.g. "Tchannel.root")
    channel = key
    if module_name == "bTagging":
        return bTaggingWeightProducer(config, channel)
    elif module_name == "jetPUID":
        return jetPUIdWeightProducer(era, channel, config)
    elif module_name == "lheWeightSign":
        return LHEWeightSignProducer(config)
    elif module_name == "muonHLT":
        return MuonHLTWeightProducer(config)
    elif module_name == "muonID":
        return MuonIDWeightProducer(config)
    else:
        logging.error(f"Unknown module: {module_name}")
        return None


def process_file(data):
    era       = data["era"]
    DataMC    = data["DataMC"]
    group     = data.get("group", None)
    key       = data["dataset"]
    outputDir = data["outputDir"]
    file      = data["file"]
    cut_string  = data.get("cut_string", None) or None  # normalise "" -> None
    goldenJSON  = data.get("goldenJSON", None)
    branchsel   = data.get("branchsel", None)
    module_configs = data.get("modules", [])
    # module_configs: list of {"name": <str>, "config": <dict>}
    os.makedirs(outputDir, exist_ok=True)

    # Guard against 0-event files after the cut string.
    # When the TEntryList has GetN()==0, PostProcessor skips eventLoop() entirely,
    # meaning beginFile() is never called on modules, leaving the output TTree
    # without custom branches.  ROOT then segfaults in FullOutput.write()
    # because CopyTree() walks branch buffers that were never properly
    # initialised.  Detect this cheaply before spawning PostProcessor.
    if cut_string is not None:
        try:
            _cf = ROOT.TFile.Open(file, "READ")
            if _cf and not _cf.IsZombie():
                _ct = _cf.Get("Events")
                _n  = int(_ct.GetEntries(cut_string)) if (_ct is not None) else 0
                # Release _ct BEFORE Close(): TFile::Close() calls DeleteAll()
                # which frees the TTree C++ object. del _ct after Close()
                # would have PyROOT touch a dangling pointer → SIGSEGV.
                _ct = None
                _cf.Close()
                del _cf
                if _n == 0:
                    logging.info(
                        f"    0 events pass cut string in {file} "
                        f"(dataset={key}, {DataMC}, {era}); skipping to avoid ROOT segfault."
                    )
                    return None
        except Exception as _e:
            logging.warning(f"    Pre-check failed for {file}: {_e}; proceeding anyway.")

    modules_with_names = []
    try:
        for entry in module_configs:
            mod_name = entry["name"]
            mod_conf = entry.get("config", {})
            loaded = _instantiate_module(mod_name, era, DataMC, key, mod_conf)
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
    parser.add_argument('--filter', nargs='+', default=None, metavar='FILTER',
                       help='Filter by era[/DataMC[/group[/dataset]]]. Use * as wildcard at any level. '
                            'Multiple filters are OR-ed. E.g.: --filter UL2017 --filter UL2018/MC_mu/SingleTop')
    parser.add_argument('--force', action='store_true',
                       help='Process all files even if output files already exists.')
    parser.add_argument('--sample', action='store_true',
                       help='Process only the first file of each dataset (isSample=True), '
                            'useful for quick validation runs.')
    args = parser.parse_args()

    try:
        with open(args.processListJSON, 'r') as f:
            process_list = json.load(f)
    except FileNotFoundError:
        logging.error(f"Process list JSON not found: {args.processListJSON}")
        sys.exit(1)

    # Change to the run folder (grandparent of the processListJSON) so that
    # relative SF/config paths stored in the JSON resolve correctly.
    # Layout: outputs/<tag>/<hash>/<era>/<tag>_<era>_processListJSON.json
    #                        ^run_dir^
    run_dir = os.path.dirname(os.path.dirname(os.path.abspath(args.processListJSON)))
    os.chdir(run_dir)
    logging.info(f"Working directory set to run folder: {run_dir}")

    logging.info(f"Loaded {len(process_list)} tasks from {args.processListJSON}")
    if len(process_list) == 0:
        logging.info("No files to process. Exiting.")
        sys.exit(0)

    # --- Pre-filter tasks in the main process ---
    # Spawning a worker process (ROOT import + Python startup) costs ~5 s each.
    # Dispatching all N tasks to the pool and filtering inside process_file()
    # means paying that cost for every task, even ones that will be skipped
    # immediately.  Pre-filtering here reduces the pool workload to only the
    # files that actually need processing.
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
            skim_name = os.path.basename(data["file"]).replace(".root", "_Skim.root")
            skim_path = os.path.join(data["outputDir"], skim_name)
            if os.path.exists(skim_path):
                pre_skipped += 1
                continue
        tasks_to_run.append(data)

    logging.info(f"Pre-filtering: {len(tasks_to_run)} tasks to run, {pre_skipped} already done / filtered out.")
    if len(tasks_to_run) == 0:
        logging.info("Nothing to do. Exiting.")
        sys.exit(0)
    logging.info("Starting parallel processing of datasets...")

    # --- Run the pool ---
    # chunksize=1 + maxtasksperchild=1: each worker handles exactly one file
    # then exits, giving every file a completely fresh Python+ROOT process so
    # ROOT's global TFile/TTreeReader state never accumulates across files.
    num_cores = args.workers
    with Pool(num_cores, maxtasksperchild=1) as pool:
        results = list(tqdm(pool.starmap(process_file,
                                         [(data,) for data in tasks_to_run],
                                         chunksize=1),
                            total=len(tasks_to_run),
                            desc="Processing datasets"))

    succeeded = sum(1 for r in results if r is True)
    failed    = sum(1 for r in results if r is None)
    logging.info(f"Processing complete: {succeeded} succeeded, {failed} failed out of {len(results)} total "
                 f"({pre_skipped} pre-skipped).")
    logging.info("Finished all processing.")

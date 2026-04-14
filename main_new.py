from pathlib import Path
import os, json, argparse, logging, yaml, re
import sys

# ---------------------------------------------------------------------------
# .env loader  —  must run before any path-dependent code
# ---------------------------------------------------------------------------
def _load_env(env_file=None):
    """
    Parse a .env file and inject its KEY=VALUE pairs into os.environ.
    Keys already present in the real environment take priority (so CI/condor
    jobs can override via actual env vars without touching .env).
    Returns the dict of values that were loaded.
    """
    if env_file is None:
        # .env lives next to this script (repo root)
        env_file = Path(__file__).resolve().parent / ".env"
    else:
        env_file = Path(env_file)

    loaded = {}
    if not env_file.exists():
        # Not fatal — the user may supply all vars via real env
        return loaded

    with open(env_file) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("'\"")
            loaded[key] = value
            # Real env vars win; .env only fills gaps
            if key not in os.environ:
                os.environ[key] = value
    return loaded

_ENV = _load_env()

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

# Helper: resolve a path that may be stored relative to a storage root
def _resolve_path(path: str, storage_root: str) -> str:
    """Return an absolute path.  Relative paths are joined onto storage_root."""
    if os.path.isabs(path):
        return path
    return os.path.join(storage_root, path)

def _relative_path(abs_path: str, storage_root: str) -> str:
    """Strip storage_root prefix so the path is portable across machines."""
    try:
        return os.path.relpath(abs_path, storage_root)
    except ValueError:   # on Windows if drives differ — defensive
        return abs_path

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
import importlib
from tqdm import tqdm

# 🧠 --- Global correctionlib cache ---
import correctionlib
_GLOBAL_CORRECTION_CACHE = {}

def preload_correctionlib(file_path):
    """Load a correctionlib file only once in the parent process."""
    if file_path not in _GLOBAL_CORRECTION_CACHE:
        logging.info(f"[preload] Loading correctionlib file: {file_path}")
        _GLOBAL_CORRECTION_CACHE[file_path] = correctionlib.CorrectionSet.from_file(file_path)
    return _GLOBAL_CORRECTION_CACHE[file_path]

def load_yaml_config(filePath):
    with open(filePath, "r") as f:
        config = yaml.safe_load(f)
    return config


def processFolder(outputDirBase, tag, stage, era, datasetsFolder):
    """
    Scan the output directory tree for a (tag, stage, era) and write a
    dataFiles JSON.  Paths are stored **relative to OUTPUT_STORAGE** so the
    same JSON works on any machine once OUTPUT_STORAGE is set in .env.
    """
    output_storage = os.environ.get("OUTPUT_STORAGE", "")  # may be empty
    baseD = os.path.join(outputDirBase, stage, tag, era)
    if not os.path.exists(baseD):
        logging.error(f"Fileset directory does not exist: {baseD}")
        return
    jsonDic = {}
    for DataMC in os.listdir(baseD):
        jsonDic[DataMC] = {}
        for key in os.listdir(os.path.join(baseD, DataMC)):
            jsonDic[DataMC][key] = {}
            for file in os.listdir(os.path.join(baseD, DataMC, key)):
                if file.endswith(".root"):
                    fileTotalPath = os.path.join(baseD, DataMC, key, file)
                    if is_root_file_healthy(fileTotalPath):
                        # Drop files whose Events tree is empty.
                        # NOTE: hold the TFile in a variable — if it goes out
                        # of scope as a temporary, ROOT closes the file and the
                        # tree pointer becomes dangling, causing a segfault.
                        f_check = ROOT.TFile.Open(fileTotalPath)
                        tree    = f_check.Get("Events") if f_check else None
                        n_entries = tree.GetEntries() if tree else 0
                        f_check.Close()
                        if n_entries == 0:
                            logging.warning(f"Skipping zero-event file while making JSON: {fileTotalPath}")
                            continue
                        # Store as relative path when OUTPUT_STORAGE is known
                        stored_path = (
                            _relative_path(fileTotalPath, output_storage)
                            if output_storage else fileTotalPath
                        )
                        jsonDic[DataMC][key][stored_path] = "Events"
                    else:
                        logging.warning(f"Skipping unhealthy file while making JSON: {fileTotalPath}")
    jsonFile = os.path.join(datasetsFolder, f"{tag}_{stage}_{era}_dataFiles.json")
    with open(jsonFile, 'w') as jf:
        json.dump(jsonDic, jf, indent=4)
    logging.info(f"Wrote JSON file: {jsonFile}")

def load_module(module_name, era, key=None, config=None):
    # --- Preload all correctionlib files once before forking ---
    # This prevents "Duplicate Correction name" error in multiprocessing
    loaded = None
    if module_name == "lheWeightSign":
        from modules.workflow.LHEWeightSign import lheWeightSignModule
        loaded = lheWeightSignModule(config)
    elif module_name == "muonID":
        ID_json = config["IDSFFile"]
        preload_correctionlib(ID_json)
        from modules.workflow.MuonIDWeight import muonIDWeightModule
        loaded = muonIDWeightModule(config)
    elif module_name == "muonHLT":
        HLT_json = config["HLTSFFile"]
        preload_correctionlib(HLT_json)
        from modules.workflow.MuonHLTWeight import muonHLTWeightModule
        loaded = muonHLTWeightModule(config)
    elif module_name == "bTagging":
        from modules.workflow.bTaggingWeight import bTaggingWeightModule
        loaded = bTaggingWeightModule(config, key)
    elif module_name == "jetPUID":
        # FIX: pass config so the module reads paths from the config YAML
        # instead of using hard-coded absolute paths.
        from modules.workflow.JetPUIdWeightModule_new import jetPUIdWeightModule
        loaded = jetPUIdWeightModule(era, key, config)
    elif module_name == "Reco":
        # FIX: use the corrected RecoModule that applies jet quality cuts
        from modules.workflow.RecoModule_new import RecoModule
        min_pgof = config.get("minPgof") if isinstance(config, dict) else None
        loaded = RecoModule(era, minPgof=min_pgof)
    elif module_name == "BDTVariable":
        from modules.workflow.BDTvariableModule import BDTvariableModule
        loaded = BDTvariableModule()
    elif module_name == "Observables":
        from modules.workflow.observables import ObservablesProducer
        loaded = ObservablesProducer()
    elif module_name == "applyBDT":
        from modules.workflow.applyBDTModule import applyBDTModule
        loaded = applyBDTModule(config["moduleConfigs"]["applyBDT"])
    elif module_name == "yCalculator":
        from modules.workflow.yCalculator import yCalculator
        loaded = yCalculator()
    return loaded

def is_root_file_healthy(filepath: str) -> bool:
    """Check if a ROOT file is healthy using PyROOT, with logging info."""
    if not os.path.exists(filepath):
        logging.error(f"File does not exist: {filepath}")
        return False
    if not os.path.isfile(filepath):
        logging.error(f"Path is not a file: {filepath}")
        return False
    if not os.access(filepath, os.R_OK):
        logging.error(f"File not readable: {filepath}")
        return False
    if os.path.getsize(filepath) == 0:
        logging.error(f"File is empty: {filepath}")
        return False
    try:
        f = ROOT.TFile.Open(filepath)
    except OSError as e:
        logging.error(f"Failed to open ROOT file {filepath}: {e}")
        return False

    if not f or f.IsZombie():
        logging.error(f"ROOT file is not openable or is zombie: {filepath}")
        if f: f.Close()
        return False

    if f.TestBit(ROOT.TFile.kRecovered):
        logging.error(f"ROOT file was recovered, may be corrupted: {filepath}")
        f.Close()
        return False

    if not f.GetListOfKeys() or f.GetNkeys() == 0:
        logging.error(f"ROOT file has no keys: {filepath}")
        f.Close()
        return False

    f.Close()
    return True

def process_file(data):
    stage, era, DataMC, key, outputDir, file, configDir = data
    stage_era_configPath = os.path.join(configDir, f"{stage}_{era}_config.yaml")
    
    try:
        conf = load_yaml_config(stage_era_configPath)
    except Exception as e:
        logging.error(f"Failed to load config {stage_era_configPath}: {e}")
        return None

    files = [file]
    cuts = conf.get("cuts", None)
    if cuts is not None:
        # Drop Flag_* cuts for DataMC groups whose input files lack those branches
        # (e.g. MC_alt samples skimmed with older crab jobs that stripped Flag_ branches).
        no_flag_groups = conf.get("noFlagCutsFor", [])
        if DataMC in no_flag_groups:
            cuts = [c for c in cuts if "Flag_" not in c]
        cut_string = " && ".join(cuts) if cuts else None
    else:
        cut_string = None
    branchselector = conf.get("branchsel", None)
    goldenJSON = None

    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    # Track modules with their names so we can detect failures to load
    modules_with_names = []
    try:
        if "Data" in DataMC:
            goldenJSON = conf.get("goldenJSON", None)
            if conf["modules"]["Data"] is not None:
                logging.info(f"Loading Data modules for {key} in {stage}:{era}")
                for mod in conf["modules"]["Data"]:
                    module_config_path = os.path.join(configDir, f"{mod}_{era}_config.yaml")
                    module_conf = load_yaml_config(module_config_path)
                    loaded = load_module(mod, era, key, module_conf)
                    modules_with_names.append((mod, loaded))
        elif "MC" in DataMC:
            if conf["modules"]["MC"] is not None:
                logging.info(f"Loading MC modules for {key} in {stage}:{era}")
                for mod in conf["modules"]["MC"]:
                    module_config_path = os.path.join(configDir, f"{mod}_{era}_config.yaml")
                    module_conf = load_yaml_config(module_config_path)
                    loaded = load_module(mod, era, key, module_conf)
                    modules_with_names.append((mod, loaded))
    except Exception as e:
        logging.error(f"Failed to load modules for {key} in {stage}:{era}: {e}")
        return None

    # Filter out any modules that failed to load (None) and warn
    missing_modules = [name for name, m in modules_with_names if m is None]
    if missing_modules:
        logging.error(f"Failed to load modules {missing_modules} for {key} in {stage}:{era}. Skipping this file.")
        return None
    modules = [m for _, m in modules_with_names]

    try:
        post_processor = PostProcessor(
            outputDir,
            files,
            cut=cut_string,
            jsonInput=goldenJSON,
            branchsel=branchselector,
            modules=modules, 
            noOut=False,
            justcount=False,
            compression="ZLIB:9",  # Use ZLIB instead of LZMA for compatibility
        )   
        post_processor.run()
        logging.info(f"Finished processing {file} in {key} of {DataMC} for {stage}:{era}")
        return True
    except Exception as e:
        logging.error(f"Error processing {file} in {key} of {DataMC} for {stage}:{era}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return None

if __name__ == "__main__":
    import sys
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
    parser = argparse.ArgumentParser(description="Process NanoAOD files with specified era and output tag.")
    parser.add_argument('--outputTag','-t', required=True, help='Tag for the output directory (e.g., April152025)')
    parser.add_argument('--era', '-e', help='Analysis era (e.g., UL2016preVFP, UL2016postVFP)')
    parser.add_argument('--stage', '-s', help='Stage of the analysis')
    parser.add_argument('--workers', '-w', type=int, default=15, help='Number of parallel workers to use')
    parser.add_argument('--includeKeys', help='Regex pattern: only include keys that match this pattern')
    parser.add_argument('--excludeKeys', help='Regex pattern: exclude keys that match this pattern')
    parser.add_argument('--includeTrees', help='Regex pattern to include file paths')
    parser.add_argument('--excludeTrees', help='Regex pattern to exclude file paths')
    parser.add_argument('--sample', action='store_true', help='If set, process only a sample of the datasets for testing')
    parser.add_argument('--harvest-only', action='store_true',
                        help='Skip file processing; only (re)generate dataFiles JSONs from existing output. '
                             'Useful after HTCondor jobs finish to regenerate the JSON index.')

    args = parser.parse_args()
    include_key_pattern = re.compile(args.includeKeys) if args.includeKeys else None
    exclude_key_pattern = re.compile(args.excludeKeys) if args.excludeKeys else None
    include_tree_pattern = re.compile(args.includeTrees) if args.includeTrees else None
    exclude_tree_pattern = re.compile(args.excludeTrees) if args.excludeTrees else None
    outputTag = args.outputTag
    runSample = args.sample
    logging.info(f"Using era: {args.era}")
    logging.info(f"Using output tag: {outputTag}")

    if runSample:
        logging.info("Running in sample mode: will process only one file from each dataset")
    if include_key_pattern:
        logging.info(f"Including keys matching: {args.includeKeys}")
    if exclude_key_pattern:
        logging.info(f"Excluding keys matching: {args.excludeKeys}")
    if include_tree_pattern:
        logging.info(f"Including files matching: {args.includeTrees}")
    if exclude_tree_pattern:
        logging.info(f"Excluding files matching: {args.excludeTrees}")

    configDir = os.path.join("configs", f"{outputTag}")
    processFlowPath = os.path.join(configDir, "processFlow_config.yaml")
    config = load_yaml_config(processFlowPath)

    datasetsFolder = config["DatasetJSONFolder"]
    tag = config["tag"]

    # OUTPUT_STORAGE in .env overrides outputDirBase from the YAML config.
    # This lets the same config file work on TIFR and lxplus with only .env changes.
    output_storage = os.environ.get("OUTPUT_STORAGE", "").rstrip("/")
    input_storage  = os.environ.get("INPUT_STORAGE",  "").rstrip("/")
    if output_storage:
        outputDirBase = output_storage
        logging.info(f"OUTPUT_STORAGE from .env overrides outputDirBase: {outputDirBase}")
    else:
        outputDirBase = config["outputDirBase"]
        logging.info(f"Using outputDirBase from config: {outputDirBase}")
    if input_storage:
        logging.info(f"INPUT_STORAGE from .env: {input_storage}")
    else:
        logging.warning("INPUT_STORAGE not set in .env — relative paths in nfs_skimmed JSONs will fail")

    # --- FIX #1: Separate the planning loop from processFolder calls ---
    #
    # In the original main.py, processFolder() was called inside the planning
    # loop (before the Pool runs). This caused the downstream stage JSON to be
    # scanned BEFORE the files existed on disk, resulting in empty file lists
    # when running multiple stages in a single invocation.
    #
    # New approach:
    #   Phase 1 — build process_list (all stages x eras)
    #   Phase 2 — run the pool
    #
    # A set tracks which (stage, era) pairs were actually scheduled so we only
    # regenerate JSONs for those.

    process_list = []
    scheduled_stage_eras = []   # list of (stage, era) that had work queued

    # --harvest-only: skip file scanning and pool; just decide which (stage,era)
    # to regenerate JSONs for, then jump straight to Phase 3.
    if args.harvest_only:
        logging.info("--harvest-only mode: skipping file processing, regenerating JSONs only.")
        for stage in config["processFlow"]:
            if args.stage and stage != args.stage:
                continue
            for era in config["processFlow"][stage]:
                if args.era and era != args.era:
                    continue
                scheduled_stage_eras.append((stage, era))
        logging.info(f"Will regenerate JSONs for: {scheduled_stage_eras}")
        for stage, era in scheduled_stage_eras:
            processFolder(outputDirBase, tag, stage, era, datasetsFolder)
        logging.info("Harvest complete.")
        sys.exit(0)

    for stage in config["processFlow"]:
        if args.stage and stage != args.stage:
            logging.info(f"Skipping stage {stage} as it does not match specified stage {args.stage}")
            continue
        logging.info(f"Processing stage: {stage}")
        for era in config["processFlow"][stage]:
            if args.era and era != args.era:
                logging.info(f"Skipping era {era} as it does not match specified era {args.era}")
                continue
            inputTag = config["processFlow"][stage][era]["inputTag"]
            inputStage = config["processFlow"][stage][era]["inputStage"]
            if inputTag is None:
                logging.warning(f"Skipping Processing {stage}: {era} due to None inputTag")
                continue
            logging.info(f"Processing {stage}: {era} with inputTag: {inputTag} and inputStage: {inputStage}")

            inputJSON = os.path.join(datasetsFolder, f"{inputTag}_{inputStage}_{era}_dataFiles.json")
            logging.info(f"Loading dataset JSON: {inputJSON}")
            try:
                with open(inputJSON, 'r') as json_file:
                    dicti = json.load(json_file)
            except FileNotFoundError:
                logging.error(f"JSON file not found at {inputJSON}")
                continue

            # Choose the right storage root for resolving JSON paths.
            # nfs_skimmed JSONs are relative to INPUT_STORAGE;
            # all processed-stage JSONs are relative to OUTPUT_STORAGE.
            is_nfs_input = (inputTag == "nfs" and inputStage == "skimmed")
            json_base = input_storage if is_nfs_input else output_storage

            stage_had_work = False
            for DataMC in dicti:
                for key in dicti[DataMC]:
                    if include_key_pattern and not include_key_pattern.search(key):
                        continue
                    if exclude_key_pattern and exclude_key_pattern.search(key):
                        continue
                    outputDir = os.path.join(outputDirBase, stage, tag, era, DataMC, key)
                    if not os.path.exists(outputDir):
                        os.makedirs(outputDir)
                    files = dicti[DataMC][key]
                    for file in files:
                        # Resolve relative paths using the appropriate storage root
                        file = _resolve_path(file, json_base) if json_base else file
                        if include_tree_pattern and not include_tree_pattern.search(file):
                            continue
                        if exclude_tree_pattern and exclude_tree_pattern.search(file):
                            continue
                        if is_root_file_healthy(file):
                            destFile = os.path.join(outputDir, os.path.basename(file).replace('.root', '_Skim.root'))
                            if os.path.exists(destFile):
                                if is_root_file_healthy(destFile):
                                    logging.info(f"Output file already exists and is healthy: {destFile}. Skipping.")
                                    continue
                            process_list.append((stage, era, DataMC, key, outputDir, file, configDir))
                            stage_had_work = True
                            if runSample:
                                logging.info("Sample mode active: processed one file, moving to next dataset.")
                                break
                        else:
                            logging.warning(f"Skipping unhealthy input file: {file}")

            if stage_had_work:
                scheduled_stage_eras.append((stage, era))

    logging.info(f"Total files to process: {len(process_list)}")
    if len(process_list) == 0:
        logging.info("No files to process. Exiting.")
        sys.exit(0)
    logging.info("Starting parallel processing of datasets...")

    # --- Phase 2: Run the pool ---
    num_cores = args.workers
    # maxtasksperchild=1: each worker handles exactly one file then exits.
    # This gives every file a fresh Python+ROOT process with zero stale
    # TTreeReader proxy state, eliminating the segfaults seen after ~6-7 files.
    with Pool(num_cores, maxtasksperchild=1) as pool:
        results = list(tqdm(pool.imap_unordered(process_file, process_list),
                            total=len(process_list),
                            desc="Processing datasets"))

    # --- FIX #7: Report success/failure counts ---
    succeeded = sum(1 for r in results if r is True)
    failed    = sum(1 for r in results if r is None)
    logging.info(f"Processing complete: {succeeded} succeeded, {failed} failed out of {len(results)} total.")

    logging.info("Finished all processing.")

from pathlib import Path
import os, json, argparse, logging, yaml, re
import sys, ROOT
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


def processFolder(tag, stage, era):
    baseD = os.path.join("/mnt/disk1/skimmed_Run2", stage, tag, era)
    if not os.path.exists(baseD):
        logging.error(f"Fileset directory does not exist: {baseD}")
    jsonDic = {}
    for DataMC in os.listdir(baseD):
        jsonDic[DataMC] = {}
        for key in os.listdir(os.path.join(baseD, DataMC)):
            jsonDic[DataMC][key] = {}
            for file in os.listdir(os.path.join(baseD, DataMC, key)):
                if file.endswith(".root"):
                    fileTotalPath = os.path.join(baseD, DataMC, key, file)
                    if is_root_file_healthy(fileTotalPath):
                        jsonDic[DataMC][key][fileTotalPath] = "Events"
                    else:
                        logging.warning(f"Skipping unhealthy file while making JSON: {os.path.join(baseD, DataMC, key, file)}")
    jsonFile = os.path.join("/home/mukund/Projects/SkimandSlim/NanoAODTools/Datasets", f"{tag}_{stage}_{era}_dataFiles.json")
    with open(jsonFile, 'w') as jf:
        json.dump(jsonDic, jf, indent=4)
    logging.info(f"Wrote JSON file: {jsonFile}")

def load_module(module_name, era, key=None, config=None):
    # --- Preload all correctionlib files once before forking ---
    # This prevents "Duplicate Correction name" error in multiprocessing
    loaded = None
    if module_name == "lheWeightSign":
        # logging.info(f"Loading {module_name} module configs {config}")
        from python.postprocessing.modules.custom.LHEWeightSign import lheWeightSignModule
        loaded = lheWeightSignModule(config)
    elif module_name == "muonID":
        # logging.info(f"Loading {module_name} module configs {config}")
        ID_json = config["IDSFFile"]
        # logging.info(f"Preloading correctionlib file for muonID: {ID_json}")
        preload_correctionlib(ID_json)
        from python.postprocessing.modules.custom.MuonIDWeight import muonIDWeightModule
        loaded = muonIDWeightModule(config)
    elif module_name == "muonHLT":
        # logging.info(f"Loading {module_name} module configs {config}")
        HLT_json = config["HLTSFFile"]
        preload_correctionlib(HLT_json)
        from python.postprocessing.modules.custom.MuonHLTWeight import muonHLTWeightModule
        loaded = muonHLTWeightModule(config)
    elif module_name == "bTagging":
        # logging.info(f"Loading {module_name} module configs {config}")
        from python.postprocessing.examples.bTaggingWeights import bTaggingWeightModule
        loaded = bTaggingWeightModule(era, key)
    elif module_name == "jetPUID":
        # logging.info(f"Loading {module_name} module configs {config}")
        from python.postprocessing.examples.JetPUIdWeightModule import jetPUIdWeightModule
        loaded = jetPUIdWeightModule(era, key)
    elif module_name == "Reco":
        from python.postprocessing.examples.RecoModule import RecoModule
        loaded = RecoModule(era)
    elif module_name == "BDTvariables":
        from python.postprocessing.examples.BDTvariableModule import BDTvariableModule
        loaded = BDTvariableModule()
    elif module_name == "Observables":
        from python.postprocessing.modules.custom.observables import ObservablesProducer
        loaded = ObservablesProducer()
    elif module_name == "applyBDT":
        from python.postprocessing.modules.custom.BDTScore import BDTScoreProducer
        loaded = BDTScoreProducer(config["moduleConfigs"]["BDTScore"])
    elif module_name == "yCalculator":
        from python.postprocessing.modules.custom.yCalculator import yCalculator
        loaded = yCalculator()
    return loaded

def is_root_file_healthy(filepath: str) -> bool:
    """Check if a ROOT file is healthy using PyROOT, with logging info."""
    # General checks
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
    # logging.info(f"File is healthy: {filepath}")
    return True

def process_file(data):
    stage, era, DataMC, key, outputDir, file, configDir = data
    stage_era_configPath = os.path.join(configDir, f"{stage}_{era}_config.yaml")
    conf = load_yaml_config(stage_era_configPath)

    files = [file]
    cuts = conf.get("cuts", None)
    if cuts is not None:
        cut_string = " && ".join(cuts)
    else:
        cut_string = None
    branchselector = conf.get("branchsel", None)
    goldenJSON = None

    noOut = False
    justcount = False

    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    modules = []
    if "Data" in DataMC:
        goldenJSON = conf.get("goldenJSON", None)
        if conf["modules"]["Data"] is not None:
            logging.info(f"Loading Data modules for {key} in {stage}:{era}")
            for mod in conf["modules"]["Data"]:
                module_config_path = os.path.join(configDir, f"{mod}_{era}_config.yaml")
                module_conf = load_yaml_config(module_config_path)
                modules.append(load_module(mod, era, key, module_conf))
    elif "MC" in DataMC:
        if conf["modules"]["MC"] is not None:
            logging.info(f"Loading MC modules for {key} in {stage}:{era}")
            for mod in conf["modules"]["MC"]:
                module_config_path = os.path.join(configDir, f"{mod}_{era}_config.yaml")
                module_conf = load_yaml_config(module_config_path)
                # logging.info(f"Module config for {mod}: {module_conf}")
                modules.append(load_module(mod, era, key, module_conf))

    # modules = [factory() for factory in modules]
    # logging.info(f"Processing modules for {file} in {key} of {DataMC} for {stage}:{era}: {[type(m).__name__ for m in modules]}")
    # Set up the PostProcessor
    post_processor = PostProcessor(
        outputDir,
        files,
        cut=cut_string,
        jsonInput=goldenJSON,
        branchsel=branchselector,
        modules=modules, 
        noOut=False,
        justcount=False,
    )   
    # Run the PostProcessor
    post_processor.run()
    logging.info(f"Finished processing {file} in {key} of {DataMC} for {stage}:{era}")

if __name__ == "__main__":
    import sys

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

    configDir = os.path.join("/home/mukund/Projects/SkimandSlim/NanoAODTools/configs/", f"{outputTag}")

    processFlowPath = os.path.join(configDir, "processFlow_config.yaml")

    config = load_yaml_config(processFlowPath)

    datasetsFolder = config["DatasetJSONFolder"]
    outputDirBase = config["outputDirBase"]
    tag = config["tag"]
    process_list = []
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
            if inputTag == None:
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
            for DataMC in dicti:
                logging.info(f"Processing Data/MC category: {DataMC}")
                logging.info(f"Processing keys in {DataMC} for {stage}:{era}")
                for key in dicti[DataMC]:
                    if include_key_pattern and not include_key_pattern.search(key):
                        continue
                    if exclude_key_pattern and exclude_key_pattern.search(key):
                        continue
                    logging.info(f"Processing dataset key: {key}")
                    logging.info(f"Processing files in {key} of {DataMC} for {stage}:{era}")
                    outputDir = os.path.join(outputDirBase, stage, tag, era, DataMC, key) 
                    if not os.path.exists(outputDir):
                        os.makedirs(outputDir)
                    files = dicti[DataMC][key]
                    for file in files:
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
                            if runSample:
                                logging.info("Sample mode active: processed one file, moving to next dataset.")
                                break
                        else:
                            logging.warning(f"Skipping unhealthy input file: {file}")
            processFolder(tag, stage, era)
    logging.info(f"Total files to process: {len(process_list)}")
    if len(process_list) == 0:
        logging.info("No files to process. Exiting.")
        sys.exit(0)
    logging.info("Starting parallel processing of datasets...")


    # Use multiprocessing to process datasets in parallel
    num_cores = args.workers

    with Pool(num_cores) as pool:
        results = list(tqdm(pool.imap_unordered(process_file, process_list),
                            total=len(process_list),
                            desc="Processing datasets"))

    logging.info("Finished all processing.")
    # print(dataset_list)               

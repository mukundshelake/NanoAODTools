#!/usr/bin/env python3
import os, json, argparse, logging
try:
    import ROOT
    HAS_ROOT = True
except ImportError:
    HAS_ROOT = False
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from python.postprocessing.examples.RecoModule_withConfig import RecoModule
from multiprocessing import Pool
import yaml

def process_dataset(data):
    """Function to process a single dataset."""
    DataMC, key, files, outDir, era, recoconfigPath = data

    logging.info(f"Processing {key} in {DataMC}")

    if not os.path.exists(outDir):
        os.makedirs(outDir)
    else:
        # Check for existing output files
        existing_files = [f for f in os.listdir(outDir) if f.endswith('.root')]
        if existing_files:
            # Get input file names without paths
            input_basenames = [os.path.basename(f) for f in files]
            # Find which input files don't have good output
            files_to_process = []
            for f in files:
                output_name = os.path.basename(f).replace('.root', '_Skim.root')
                output_path = os.path.join(outDir, output_name)
                
                # Check if output exists and is healthy
                if os.path.exists(output_path):
                    continue
                    # try:
                    #     # Basic health checks
                    #     if os.path.getsize(output_path) == 0:
                    #         logging.warning(f"Empty output file found: {output_path}")
                    #         files_to_process.append(f)
                    #         continue
                            
                    #     # Try to open the file to verify it's valid
                    #     import ROOT
                    #     root_file = ROOT.TFile(output_path)
                    #     if not root_file or root_file.IsZombie():
                    #         logging.warning(f"Corrupted ROOT file found: {output_path}")
                    #         files_to_process.append(f)
                    #         root_file.Close()
                    #         continue
                    #     root_file.Close()
                    # except Exception as e:
                    #     logging.warning(f"Error checking {output_path}: {str(e)}")
                    #     files_to_process.append(f)
                    #     continue
                else:
                    files_to_process.append(f)
            
            files = files_to_process
            if not files:
                logging.info(f"All files already processed for {key} in {DataMC}")
                return
            logging.info(f"Processing {len(files)} remaining files out of {len(input_basenames)} for {key} in {DataMC}")
            logging.info(f"Files to process: {files}")

    # Determine if the dataset is Data or MC
    if "Data" in DataMC:
        modules = [
            RecoModule(era, recoconfigPath),  # Add BDT variable module
        ] 
    else:
        modules = [
            RecoModule(era, recoconfigPath),  # Add BDT variable module
        ]  # Add MC-specific modules

    # Set up the PostProcessor
    post_processor = PostProcessor(
        outDir,
        files,
        modules=modules,
        noOut=False,
        justcount=False,
    )

    # Run the PostProcessor
    post_processor.run()
    logging.info(f"Finished processing {key} in {DataMC}")

if __name__ == "__main__":
    import sys


    # --- Configure Logging ---
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info("Starting selection processing script.")

    # --- Argument Parser ---
    parser = argparse.ArgumentParser(description="Process NanoAOD files with specified era and output tag.")
    parser.add_argument('--config', required=True, default='config.yaml', help='Path to the configuration file (default: config.yaml)')
    args = parser.parse_args()
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    era = config.get('era', 'UL2016preVFP')
    outputTag = config.get('outputTag', 'defaultTag')
    logging.info(f"Using era: {era}")
    logging.info(f"Using output tag: {outputTag}")
    configPaths = config.get('configPaths', {})
    logging.info(f"Loaded configuration:\n {config}")
    # --- Load the dataset configuration ---
    json_file_path = f'/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/Datasets/selected_dataFiles_{era}.json'
    logging.info(f"Loading dataset configuration from: {json_file_path}")
    try:
        with open(json_file_path, 'r') as json_file:
            dicti = json.load(json_file)
    except FileNotFoundError:
        logging.error(f"JSON file not found at {json_file_path}")
        sys.exit(1)
    # exit(0)
    # Prepare datasets for parallel processing
    dataset_list = []
    for DataMC in dicti:
        for key in dicti[DataMC]:
            if "Fully" in key:
                continue
            if "Semi" in key:
                continue
            outDir = f'/mnt/disk1/skimmed_Run2/Reco/{outputTag}/{era}/{DataMC}/{key}' # Use outputTag and era
            input_files = dicti[DataMC][key]
            dataset_list.append((DataMC, key, input_files, outDir, era, configPaths.get('reco', 'reco.yaml')))
    with open("dataset_list.json", "w") as f:
        json.dump(dataset_list, f, indent=2)

    # # Use multiprocessing to process datasets in parallel
    num_cores = 4
    with Pool(num_cores) as pool:
        pool.map(process_dataset, dataset_list)
    
    logging.info("Finished all processing.")
    # print(dataset_list)
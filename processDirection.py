#!/usr/bin/env python3
import os, json, argparse, logging
try:
    import ROOT
    HAS_ROOT = True
except ImportError:
    HAS_ROOT = False
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from python.postprocessing.examples.assignDirectionModule import assignDirectionModule
from multiprocessing import Pool

def process_dataset(data):
    """Function to process a single dataset."""
    DataMC, key, files, outDir, era = data

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
                    try:
                        # Basic health checks
                        if os.path.getsize(output_path) == 0:
                            logging.warning(f"Empty output file found: {output_path}")
                            files_to_process.append(f)
                            continue
                            
                        # Try to open the file to verify it's valid
                        import ROOT
                        root_file = ROOT.TFile(output_path)
                        if not root_file or root_file.IsZombie():
                            logging.warning(f"Corrupted ROOT file found: {output_path}")
                            files_to_process.append(f)
                            root_file.Close()
                            continue
                        root_file.Close()
                    except Exception as e:
                        logging.warning(f"Error checking {output_path}: {str(e)}")
                        files_to_process.append(f)
                        continue
                else:
                    files_to_process.append(f)
            
            files = files_to_process
            if not files:
                logging.info(f"All files already processed for {key} in {DataMC}")
                return
            logging.info(f"Processing {len(files)} remaining files out of {len(input_basenames)} for {key} in {DataMC}")

    # Determine if the dataset is Data or MC
    if "Data" in DataMC:
        modules = [
            assignDirectionModule(),  # Add BDT variable module
        ] 
    else:
        modules = [
            assignDirectionModule(),  # Add BDT variable module
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
    # --- Argument Parser ---
    parser = argparse.ArgumentParser(description="Process NanoAOD files with specified era and output tag.")
    parser.add_argument('--era', required=True, help='Analysis era (e.g., UL2016preVFP, UL2016postVFP)')
    parser.add_argument('--outputTag', required=True, help='Tag for the output directory (e.g., April152025)')
    parser.add_argument('--includeKeys', help='Regex pattern: only include keys that match this pattern')
    parser.add_argument('--excludeKeys', help='Regex pattern: exclude keys that match this pattern')
    parser.add_argument('--includeTrees', help='Regex pattern to include file paths')
    parser.add_argument('--excludeTrees', help='Regex pattern to exclude file paths')
    args = parser.parse_args()
    include_key_pattern = re.compile(args.includeKeys) if args.includeKeys else None
    exclude_key_pattern = re.compile(args.excludeKeys) if args.excludeKeys else None
    include_tree_pattern = re.compile(args.includeTrees) if args.includeTrees else None
    exclude_tree_pattern = re.compile(args.excludeTrees) if args.excludeTrees else None
    era = args.era
    outputTag = args.outputTag
    logging.info(f"Using era: {era}")
    logging.info(f"Using output tag: {outputTag}")
    if include_key_pattern:
        logging.info(f"Including keys matching: {args.includeKeys}")
    if exclude_key_pattern:
        logging.info(f"Excluding keys matching: {args.excludeKeys}")
    if include_tree_pattern:
        logging.info(f"Including files matching: {args.includeTrees}")
    if exclude_tree_pattern:
        logging.info(f"Excluding files matching: {args.excludeTrees}")
    # --- Load the dataset configuration ---
    json_file_path = f'/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/Datasets/.json'
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
            if include_key_pattern and not include_key_pattern.search(key):
                continue
            if exclude_key_pattern and exclude_key_pattern.search(key):
                continue
            raw_input_files = dicti[DataMC][key]
            filtered_input_files = []
            for file in raw_input_files:
                if include_tree_pattern and not include_tree_pattern.search(file):
                    continue
                if exclude_tree_pattern and exclude_tree_pattern.search(file):
                    continue
                filtered_input_files.append(file)

            if not filtered_input_files:
                continue  # Skip if all files were filtered out

            outDir = f'/mnt/disk1/skimmed_Run2/Directions/{outputTag}/{era}/{DataMC}/{key}'
            dataset_list.append((DataMC, key, filtered_input_files, outDir, era))

    # Use multiprocessing to process datasets in parallel
    num_cores = 6
    with Pool(num_cores) as pool:
        pool.map(process_dataset, dataset_list)
    
    logging.info("Finished all processing.")
    # print(dataset_list)
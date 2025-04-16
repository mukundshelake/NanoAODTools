import os, json, argparse, logging
import sys, ROOT
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
from multiprocessing import Pool

def process_dataset(data):
    """Function to process a single dataset."""
    DataMC, key, files, outDir, cut_string = data

    logging.info(f"Processing {key} in {DataMC}")

    if not os.path.exists(outDir):
        os.makedirs(outDir)

    # Set up the PostProcessor
    post_processor = PostProcessor(
        outDir,
        files,
        cut=cut_string,
        branchsel="python/postprocessing/examples/keep_and_drop.txt",  # Adjust this if needed
        modules=[],  # Add your custom modules if needed
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
    logging.info("Starting pre-selection processing script.")

    # --- Argument Parser ---
    parser = argparse.ArgumentParser(description="Process NanoAOD files with specified era.")
    parser.add_argument('--era', required=True, help='Analysis era (e.g., UL2016preVFP, UL2016postVFP)')
    parser.add_argument('--outputTag', required=True, help='Tag for the output directory (e.g., April142025)')
    args = parser.parse_args()
    era = args.era
    outputTag = args.outputTag
    logging.info(f"Using era: {era}")
    logging.info(f"Using output tag: {outputTag}")
    # --- Load the dataset configuration ---
    json_file_path = f'/home/mukund/Projects/updatedCoffea/Coffea_Analysis/src/Datasets/dataFiles_{era}.json'
    logging.info(f"Loading dataset configuration from: {json_file_path}")
    try:
        with open(json_file_path, 'r') as json_file:
            dicti = json.load(json_file)
    except FileNotFoundError:
        logging.error(f"JSON file not found at {json_file_path}")
        sys.exit(1)
    # --- Define cut strings based on era ---
    cut_strings = {
        "UL2016preVFP": (
            "Sum$(Muon_pt > 20 && abs(Muon_eta) < 3.0 && Muon_tightId) > 0 && "
            "Sum$(Jet_pt > 20 && abs(Jet_eta) < 3.0 && Jet_btagDeepFlavB > 0.2598) >= 2 && "
            "Sum$(Jet_pt > 20 && abs(Jet_eta) < 3.0) > 3 && "
            "(HLT_IsoMu24 || HLT_IsoTkMu24)"
        ),
        "UL2016postVFP": (
            "Sum$(Muon_pt > 20 && abs(Muon_eta) < 3.0 && Muon_tightId) > 0 && "
            "Sum$(Jet_pt > 20 && abs(Jet_eta) < 3.0 && Jet_btagDeepFlavB > 0.2489) >= 2 && "
            "Sum$(Jet_pt > 20 && abs(Jet_eta) < 3.0) > 3 && "
            "(HLT_IsoMu24 || HLT_IsoTkMu24)"
        ),
        # Add other eras and their cuts here as needed
        # "UL2017": ( ... ),
        # "UL2018": ( ... ),
    }

    if era not in cut_strings:
        logging.error(f"Cut string not defined for era '{era}'. Please add it to the script.")
        sys.exit(1)

    cut_string = cut_strings[era]
    logging.info(f"Cut string being applied for {era}: {cut_string}")
    # exit(0)
    # Prepare datasets for parallel processing
    dataset_list = []
    for DataMC in dicti:
        if 'mu' in DataMC:
            for key in dicti[DataMC]:
                outDir = f'/mnt/disk1/skimmed_Run2/preselection/{outputTag}/{era}/{DataMC}/{key}'
                # outDir = 'outputs'
                input_files = dicti[DataMC][key]
                dataset_list.append((DataMC, key, input_files, outDir, cut_string))

    # Use multiprocessing to process datasets in parallel
    num_cores = 20
    with Pool(num_cores) as pool:
        pool.map(process_dataset, dataset_list)
    
    logging.info("Finished all processing.")

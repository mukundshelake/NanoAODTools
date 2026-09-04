# This scripts generates dataset JSON file given a base directory and saves it in the given output directory with given name.

import logging
import os
import json
import argparse
import ROOT

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
    
    # checkif events are > 0
    tree = f.Get("Events")
    if not tree or tree.GetEntries() == 0:
        logging.error(f"ROOT file has no events: {filepath}")
        f.Close()
        return False


    if not f.GetListOfKeys() or f.GetNkeys() == 0:
        logging.error(f"ROOT file has no keys: {filepath}")
        f.Close()
        return False

    f.Close()
    return True

def generate_dataset_json(base_dir, output_dir, output_name):
    dataset_dict = {}
    totalEraFiles = 0
    rejected_totalEraFiles = 0
    for DataMC in os.listdir(base_dir):
        DataMCDir = os.path.join(base_dir, DataMC)
        if not os.path.isdir(DataMCDir):
            logging.warning(f"Skipping non-directory: {DataMCDir}")
            continue
        dataset_dict[DataMC] = {}
        totalDataMCFiles = 0
        rejected_totalDataMCFiles = 0
        for group in os.listdir(DataMCDir):
            sampleDir = os.path.join(DataMCDir, group)
            if not os.path.isdir(sampleDir):
                logging.warning(f"Skipping non-directory: {sampleDir}")
                continue
            dataset_dict[DataMC][group] = {}
            totalGroupFiles = 0
            rejected_totalGroupFiles = 0
            for dataset in os.listdir(sampleDir):
                datasetDir = os.path.join(sampleDir, dataset)
                if not os.path.isdir(datasetDir):
                    logging.warning(f"Skipping non-directory: {datasetDir}")
                    continue
                dataset_dict[DataMC][group][dataset] = {}
                # loop over all root files in datasetDir and it's subdirectories
                totalDatasetFiles = 0
                rejected_totalDatasetFiles = 0
                for dirpath, _, filenames in os.walk(datasetDir):
                    for file in filenames:
                        if file.endswith('.root'):
                            filePath = os.path.join(dirpath, file)
                            if is_root_file_healthy(filePath):
                                # Append {filePath: "Events"} to dataset_dict[DataMC][group][dataset]
                                dataset_dict[DataMC][group][dataset][filePath] = "Events"
                                totalDatasetFiles += 1
                                totalGroupFiles += 1
                                totalDataMCFiles += 1
                                totalEraFiles += 1
                            else:
                                logging.warning(f"Skipping unhealthy ROOT file: {filePath}")
                                rejected_totalDatasetFiles += 1
                                rejected_totalGroupFiles += 1
                                rejected_totalDataMCFiles += 1
                                rejected_totalEraFiles += 1
                logging.info(f"Total healthy (unhealthy) ROOT files in dataset {dataset}: {totalDatasetFiles} ({rejected_totalDatasetFiles})")
            logging.info(f"Total healthy (unhealthy) ROOT files in group {group}: {totalGroupFiles} ({rejected_totalGroupFiles})")
        logging.info(f"Total healthy (unhealthy) ROOT files in Data/MC {DataMC}: {totalDataMCFiles} ({rejected_totalDataMCFiles})")
    logging.info(f"Total healthy (unhealthy) ROOT files in all eras: {totalEraFiles} ({rejected_totalEraFiles})")

    
    output_path = os.path.join(output_dir, output_name)
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, 'w') as json_file:
        json.dump(dataset_dict, json_file, indent=4)
    print(f"Dataset JSON file generated at: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate dataset JSON file from a base directory.")
    parser.add_argument("--outputDirectory", required=True, help="Output directory for JSON files")
    parser.add_argument("--outputFileName", required=True, help="Output file name for the JSON file")
    parser.add_argument("--baseDirectory", required=True, help="Base directory for the datasets")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    generate_dataset_json(args.baseDirectory, args.outputDirectory, args.outputFileName)
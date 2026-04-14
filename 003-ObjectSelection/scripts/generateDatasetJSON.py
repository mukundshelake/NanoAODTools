# Generate JSON files for the datasets provided the era, stage and tag

import argparse
import json
import logging
import os
import sys
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


def generate_json(baseDirectory, outputFilePath):
    # Get the list of directories in the base directory
    datasetJSON = {}
    for group in os.listdir(baseDirectory):
        print(f"Processing group: {group}")
        groupPath = os.path.join(baseDirectory, group)
        if os.path.isdir(groupPath):
            datasetJSON[group] = {}
            for dataset in os.listdir(groupPath):
                print(f"  Processing dataset: {dataset}")
                datasetPath = os.path.join(groupPath, dataset)
                if os.path.isdir(datasetPath):
                    datasetJSON[group][dataset] = {}
                    for dirpath, _, filenames in os.walk(datasetPath):
                        for file in filenames:
                            if file.endswith('.root'):
                                filePath = os.path.join(dirpath, file)
                                print(f"    Processing file: {filePath}")
                                if is_root_file_healthy(filePath):
                                    datasetJSON[group][dataset][filePath] = "Events"
                                else:
                                    logging.warning(f"Skipping unhealthy file: {filePath}")
    # Write the JSON file
    with open(outputFilePath, 'w') as jsonFile:
        json.dump(datasetJSON, jsonFile, indent=4)
    print(f"JSON file generated at: {outputFilePath}")
                        
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate JSON files for datasets")
    parser.add_argument("--outputDirectory", required=True, help="Output directory for JSON files")
    parser.add_argument("--outputFileName", required=True, help="Output file name for the JSON file")
    parser.add_argument("--baseDirectory", required=True, help="Base directory for the datasets")
    
    args = parser.parse_args()
    baseDirectory = args.baseDirectory
    # Create output directory if it doesn't exist
    os.makedirs(args.outputDirectory, exist_ok=True)
    if baseDirectory is None:
        print("Error: Base directory is not set.")
        sys.exit(1)

    print(f"Generating JSON file for base directory: {baseDirectory}")
    outputFilePath = os.path.join(args.outputDirectory, args.outputFileName)
    generate_json(baseDirectory, outputFilePath)
